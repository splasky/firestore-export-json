import argparse
import json
import os
import sys
from functools import partial
from multiprocessing import Pool, cpu_count, Value
from pathlib import Path
import base64

from google.appengine.api import datastore
from google.appengine.api.datastore_types import EmbeddedEntity, GeoPt, Blob, ByteString
from google.appengine.datastore import entity_bytes_pb2 as entity_pb2

from converter import records
from converter.exceptions import BaseError, ValidationError
from converter.utils import embedded_entity_to_dict, serialize_json

import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.dialects.postgresql import JSONB, BYTEA

num_files: Value = Value("i", 0)
num_files_processed: Value = Value("i", 0)
except_table = ["spatial_ref_sys"]

def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog="fs_to_json", description="Firestore DB export to JSON"
    )

    parser.add_argument(
        "source_dir",
        help="Destination directory to store generated JSON",
        type=str,
        action="store",
        default=None,
    )

    parser.add_argument(
        "dest_dir",
        help="Destination directory to store generated JSON",
        type=str,
        action="store",
        default=None,
    )

    parser.add_argument(
        "-P",
        "--processes",
        help=f"Number of processes to use to process the files. Defaults to {cpu_count() - 1}",
        default=cpu_count() - 1,
        type=int,
    )

    parser.add_argument(
        "-C",
        "--clean-dest",
        help="Remove all json files from output dir",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-c",
        "--no-check-crc",
        help="Turn off the check/computation of CRC values for the records."
        "This will increase performance at the cost of potentially having corrupt data,"
        "mostly on systems without accelerated crc32c.",
        default=False,
        action="store_true",
    )

    parser.add_argument(
        "-pg",
        "--write-to-postgres",
        help="Write to postgres",
        default=False,
        action="store_true",
    ) 

    parser.add_argument(
        "-cstr",
        "--connection-string",
        help="Postgres connection string",
        default="postgresql://postgres:mysecretpassword@localhost:5432/Datastore",
        type=str,
    )

    args = parser.parse_args(args)
    try:
        source_dir = os.path.abspath(args.source_dir)
        if not os.path.isdir(source_dir):
            raise ValidationError("Source directory does not exist.")
        if not args.dest_dir:
            dest_dir = os.path.join(source_dir, "json")
        else:
            dest_dir = os.path.abspath(args.dest_dir)

        Path(dest_dir).mkdir(parents=True, exist_ok=True)

        if os.listdir(dest_dir) and args.clean_dest:
            print("Destination directory is not empty. Deleting json files...")
            for f in Path(dest_dir).glob("*.json"):
                try:
                    print(f"Deleting file {f.name}")
                    f.unlink()
                except OSError as e:
                    print("Error: %s : %s" % (f, e.strerror))

        process_files(
            source_dir=source_dir,
            dest_dir=dest_dir,
            num_processes=args.processes,
            no_check_crc=args.no_check_crc,
            write_to_pg=args.write_to_postgres,
            conn_str=args.connection_string,
        )
    except BaseError as e:
        print(str(e))
        sys.exit(1)


def process_files(source_dir: str, dest_dir: str, num_processes: int, no_check_crc: bool, write_to_pg: bool, conn_str: str):
    p = Pool(num_processes)
    files = []
    if os.path.isdir(source_dir):
        files = traverse_dir(source_dir)
    else:
        files = sorted(os.listdir(source_dir))
    num_files.value = len(files)
    print(f"processing {num_files.value} file(s)")

    if write_to_pg:
        drop_tables(create_engine(conn_str))

    f = partial(process_file, source_dir, dest_dir, no_check_crc, write_to_pg, conn_str)
    p.map(f, files)
    p.close()
    p.join()
    print(
        f"processed: {num_files_processed.value}/{num_files.value} {num_files_processed.value/num_files.value*100}%"
    )

def traverse_dir(source_dir: str):
    outputs = []
    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.find("output-") == -1:
                continue
            outputs.append(os.path.abspath(os.path.join(root, file)))
    return outputs

def process_file(source_dir: str, dest_dir: str, no_check_crc: bool, write_to_pg: bool, conn_str: str, filename: str):
    if filename.find("output-") == -1:
        return

    in_file = filename 
    table = ""
    pg_dataframe = []
    convert_dtype = {}

    try:
        with open(in_file, "rb") as raw:
            reader = records.RecordsReader(raw, no_check_crc=no_check_crc)
            for record in reader:
                entity_proto = entity_pb2.EntityProto()
                entity_proto.ParseFromString(record)

                ds_entity = datastore.Entity.FromPb(entity_proto)
                data = {}
                table = ds_entity.key().kind()
                for name, value in list(ds_entity.items()):
                    if isinstance(value, EmbeddedEntity):
                        dt = {}
                        data[name] = json.dumps(embedded_entity_to_dict(value, dt))
                        convert_dtype[name] = JSONB 
                    elif isinstance(value, datastore.Key):
                        data[name] = {"kind": value.kind(), "id": value.id_or_name()}
                        convert_dtype[name] = JSONB 
                    elif isinstance(value, list): 
                        # is list of datastore key
                        if len(value) > 0 and isinstance(value[0], datastore.Key):
                            data[name] = [{"kind": v.kind(), "id": v.id_or_name()} for v in value]
                        # is list of datastore entity
                        elif len(value) > 0 and isinstance(value[0], EmbeddedEntity):
                            dt = {}
                            data[name] = [embedded_entity_to_dict(e, dt) for e in value]
                        # others
                        else:
                            data[name] = value
                        convert_dtype[name] = JSONB
                    elif isinstance(value, GeoPt):
                        data[name] = (value.lat, value.lon)
                    elif isinstance(value, Blob):
                        data[name] = value
                        convert_dtype[name] = BYTEA
                    elif isinstance(value, bytes):
                        data[name] = value 
                        convert_dtype[name] = BYTEA
                    else:
                        data[name] = value
                
                if ds_entity.parent() is not None:
                    data["parent"] = {"kind": ds_entity.parent().kind(), "id": ds_entity.parent().id_or_name()}
                    convert_dtype["parent"] = JSONB

                data["id"] = ds_entity.key().id_or_name()
                pg_dataframe.append(data)
    except Exception as e:
        print(f"Error processing file {in_file}: {str(e)}")
        return

    if len(pg_dataframe) == 0:
        print(f"File {in_file} is empty")
        return

    if write_to_pg:
        write_to_postgres(conn_str, table, pg_dataframe, convert_dtype)
    else:
        out_file_path = os.path.join(dest_dir, filename + ".json")
        with open(out_file_path, "w", encoding="utf8") as out:
            out.write(
                json.dumps(pg_dataframe, default=serialize_json, ensure_ascii=False, indent=2)
            )
    
    num_files_processed.value += 1
    if num_files.value > 0:
        print(
            f"progress: {num_files_processed.value}/{num_files.value} {num_files_processed.value/num_files.value*100}%"
        )

def drop_tables(engine):
    metadata = MetaData()
    # Reflect existing tables
    metadata.reflect(bind=engine)
    # Drop all tables
    for table_name, table in metadata.tables.items():
        if table_name in except_table:
            print(f'Skipping table {table_name}')
            continue
        table.drop(engine, checkfirst=False)
        print(f"Table {table_name} dropped")
    print("Finished dropping tables")

def concat_curr(engine, table, concat_df, convert_dtype):
    old = pd.read_sql_table(table, engine)
    old = old.convert_dtypes()
    new_df = pd.concat([old, concat_df], ignore_index=True, verify_integrity=True)
    new_df = new_df.convert_dtypes()
    try:
        new_df.to_sql(table, engine, if_exists='replace', index=False, dtype=convert_dtype)
    except Exception as e:
        raise e

def write_to_postgres(conn_str, table, json_data, convert_dtype):
    # Create an engine to connect to the database
    engine = create_engine(conn_str)

    # Create a Pandas DataFrame
    df = pd.DataFrame(json_data).convert_dtypes()
    try:
        # Insert DataFrame into PostgreSQL table
        df.to_sql(table, engine, if_exists='append', index=False, dtype=convert_dtype)
    except Exception as err:
        try:
            df = df.convert_dtypes()
            concat_curr(engine, table, df, convert_dtype)
        except Exception as e:
            print(f"Error writing to postgres: {str(e)}, table:{table}, convert_dtypes: {convert_dtype}")

if __name__ == "__main__":
    main()
