# Firestore Export JSON
Convert google firestore/datastore LevelDB exports to JSON data.

## Dependencies

* Python  3.9
*  [Python Google Appengine](https://github.com/GoogleCloudPlatform/appengine-python-standard)

## Setup

### Method 1 - Direct Install

1. Clone the repository.

   ```shell
   $ git clone git@github.com:labbots/firestore-export-json.git
   $ cd firestore-export-json
   ```

2. Create a new virtual environment within the project directory.

   ```shell
   $ python -m venv venv
   ```

3. Activate the virtual environment in the project.

   ```shell
   $ source ./venv/bin/activate
   ```

4. Install framework dependencies.

   ```shell
   $ pip install -e .
   ```

### Method 2 - Make

1. Clone the repository.

   ```shell
   $ git clone git@github.com:labbots/firestore-export-json.git
   $ cd firestore-export-json
   ```

2. Run the make command

   ```
   $ make install
   ```



## Usage

### Method 1

`fs_to_json.py` can be used to convert the firestore/datastore export files to JSON data.

```shell
$ python fs_to_json.py [path_to_source_dir] -d [path_to_destination]
```

1. Provide the path to the directory where files such as `output-0` exists as source directory usually this would be `kinds_*` directory.
2. Destination directory can be specified where the json files will be created. If not provided then the json files will be created using `json` folder in source directory.

### Method 2

The project exposes console script using setuptools to provide `fs_to_json` CLI command which can be used to convert exports to JSON.

```shell
$ fs_to_json [path_to_source_dir] -d [path_to_destination]
```



## Inspired By

* [Venryx/firestore-leveldb-tools](https://github.com/Venryx/firestore-leveldb-tools) - LevelDB files to JSON converter written in python 2.7.