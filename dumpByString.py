import subprocess
import os
import json
import argparse
from datetime import datetime

MONGOTOOLS_BIN_PATH = "mongo_tools/bin"

def dump_database(db_name, collections, mongo_uri, output_dir, date, mongodump_path):
    """Dumps a database and its collections."""

    if not collections:
        print(f"Dumping ALL collections from database: {db_name}")
        command = [
            mongodump_path,  # Use local mongodump
            "--uri", mongo_uri,
            "--db", db_name,
            "--out", os.path.join(output_dir, f"{db_name}-{date}"),
        ]
    else:
        print(f"Dumping specific collections from database: {db_name}")
        for collection in collections:
            command = [
                mongodump_path,
                "--uri", mongo_uri,
                "--db", db_name,
                "--collection", collection,
                "--out", os.path.join(output_dir, f"{db_name}-{date}"),
            ]
            try:
                subprocess.run(command, check=True)
                print(f"Successfully dumped {db_name}.{collection}")
            except subprocess.CalledProcessError as e:
                print(f"Error dumping {db_name}.{collection}: {e}")
                exit(1)  # Exit immediately if any collection fails

        return #important to stop here after dumping individual collections

    try:
        subprocess.run(command, check=True)
        print(f"Successfully dumped {db_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error dumping {db_name}: {e}")
        exit(1)


def main():
    parser = argparse.ArgumentParser(description="MongoDB dump script.")
    parser.add_argument("-c", "--config", required=True, help="Path to the JSON configuration file.")
    parser.add_argument("-d", "--dbs", required=True, help="Path to the text file containing databases and collections.")

    args = parser.parse_args()

    config_path = args.config
    dbs_path = args.dbs

    try:
        with open(config_path, "r") as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in configuration file '{config_path}'.")
        exit(1)

    try:
        with open(dbs_path, "r") as f:
            dbs_and_collections_string = f.read()
    except FileNotFoundError:
        print(f"Error: Databases and collections file '{dbs_path}' not found.")
        exit(1)

    MONGO_HOST = config.get("mongo_host")
    MONGO_USER = config.get("mongo_user")
    MONGO_PASSWORD = config.get("mongo_password")
    OUTPUT_DIR = config.get("output_dir")
    if not all([MONGO_HOST, MONGO_USER, MONGO_PASSWORD, OUTPUT_DIR]):
       print("Error: All MongoDB config parameters must be defined in the json file")
       exit(1)

    DATE = datetime.now().strftime("%Y%m%d_%H%M%S")
    mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}"

    print("Starting MongoDB dump...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    collections_dict = {}
    for line in dbs_and_collections_string.strip().splitlines():
        if line:
            db_name, collection = line.split("\t")
            if db_name not in collections_dict:
                collections_dict[db_name] = []
            collections_dict[db_name].append(collection)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    mongodump_path = os.path.join(script_dir, MONGOTOOLS_BIN_PATH, "mongodump")

    for db_name, collections in collections_dict.items():
        dump_database(db_name, collections, mongo_uri, OUTPUT_DIR, DATE, mongodump_path)

    print("MongoDB dump completed.")

if __name__ == "__main__":
    main()