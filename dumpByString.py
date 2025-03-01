import shutil
import subprocess
import os
import json
import argparse
from datetime import datetime
import urllib.parse
import bson

MONGOTOOLS_BIN_PATH = "mongo_tools/bin"

def dump_database(db_name, collections, mongo_uri, output_dir, mongodump_path, mirror_dir):
    db_output_dir = os.path.join(output_dir)
    os.makedirs(db_output_dir, exist_ok=True)
    db_mirror_dir = os.path.join(mirror_dir)
    os.makedirs(db_mirror_dir, exist_ok=True)

    if not collections:
        print(f"Dumping ALL collections from database: {db_name}")
        command = [
            mongodump_path,
            "--uri", mongo_uri,
            "--db", db_name,
            "--out", db_output_dir,
        ]
    else:
        print(f"Dumping specific collections from database: {db_name}")
        for collection in collections:
            command = [
                mongodump_path,
                "--uri", mongo_uri,
                "--db", db_name,
                "--collection", collection,
                "--out", db_output_dir,
            ]

    try:
        subprocess.run(command, check=True)  # Run the initial mongodump
        print(f"Successfully dumped {db_name}")

        # Now, copy the metadata files to the mirror directory
        for root, _, files in os.walk(db_output_dir):
            for file in files:
                if file.endswith(".metadata.json"):  # Only copy metadata files and create a empty bson
                    src_path = os.path.join(root, file)
                    dst_path = os.path.join(db_mirror_dir, os.path.relpath(src_path, db_output_dir)) #maintain the folder structure in the mirror folder
                    os.makedirs(os.path.dirname(dst_path), exist_ok=True) #create the folder structure in the mirror folder if not exist
                    shutil.copy2(src_path, dst_path) #copy the metadata with all metadata
                    bson_path = os.path.splitext(dst_path)[0] + ".bson"  # Same name, .bson extension
                    with open(bson_path, "wb") as f:  # Open in binary write mode
                        f.write(bson.dumps({})) #write a empty dictionary to the bson file

    except subprocess.CalledProcessError as e:
        print(f"Error dumping {db_name}: {e}")
        exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        exit(1)
    return


def main():
    parser = argparse.ArgumentParser(description="MongoDB dump script.")
    config_group = parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument("-c", "--config", help="Path to the JSON configuration file.")
    config_group.add_argument("-u", "--uri", help="MongoDB connection URI (alternative to config file).")
    parser.add_argument("-d", "--dbs", required=True, help="Path to the text file containing databases and collections.")

    args = parser.parse_args()

    if args.config:
        config_path = args.config
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Configuration file '{config_path}' not found.")
            exit(1)
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format in configuration file '{config_path}'.")
            exit(1)

        MONGO_HOST = config.get("mongo_host")
        MONGO_USER = config.get("mongo_user")
        MONGO_PASSWORD = config.get("mongo_password")
        OUTPUT_DIR = config.get("output_dir")

        if not all([MONGO_HOST, MONGO_USER, MONGO_PASSWORD, OUTPUT_DIR]):
            print("Error: All MongoDB config parameters must be defined in the json file")
            exit(1)
        mongo_uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}"

    elif args.uri:
        mongo_uri = args.uri
        try:
            parsed_uri = urllib.parse.urlparse(mongo_uri)
            MONGO_USER = parsed_uri.username
            MONGO_PASSWORD = parsed_uri.password
            MONGO_HOST = parsed_uri.hostname

            OUTPUT_DIR = input("Enter the output directory: ")
        except ValueError:
            print("Error: Invalid MongoDB connection URI.")
            exit(1)
        if not all([MONGO_USER, MONGO_PASSWORD, MONGO_HOST, OUTPUT_DIR]):
            print("Error: All MongoDB config parameters must be defined in the connection string")
            exit(1)

    dbs_path = args.dbs

    try:
        with open(dbs_path, "r") as f:
            dbs_and_collections_string = f.read()
    except FileNotFoundError:
        print(f"Error: Databases and collections file '{dbs_path}' not found.")
        exit(1)

    DATE = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_root_dir = os.path.join(OUTPUT_DIR, f"mongodump_{DATE}")
    mirror_root_dir = os.path.join(OUTPUT_DIR, f"mongodump_{DATE}_mirror")
    os.makedirs(output_root_dir, exist_ok=True)
    os.makedirs(mirror_root_dir, exist_ok=True)

    print("Starting MongoDB dump...")

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
        dump_database(db_name, collections, mongo_uri, output_root_dir, mongodump_path, mirror_root_dir)

    print("MongoDB dump completed.")


if __name__ == "__main__":
    main()