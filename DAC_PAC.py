import subprocess
import json
import os
import pandas as pd
from pymongo import MongoClient
import urllib
import time
import pyodbc
import re

# Create output directory for DACPAC files
current_dir = os.getcwd()
dacpac_output_folder = os.path.join(current_dir, "DACPAC_Output")
os.makedirs(dacpac_output_folder, exist_ok=True)

def convert_bson_to_sql_friendly(data):
    """Convert BSON types (e.g., ObjectId) into SQL-friendly formats."""
    for key, value in data.items():
        if isinstance(value, dict) and "$oid" in value: 
            data[key] = str(value["$oid"])
        elif isinstance(value, list): 
            data[key] = [str(item["$oid"]) if isinstance(item, dict) and "$oid" in item else item for item in value]
    return data

def sanitize_table_name(collection_name):
    """Sanitize the MongoDB collection name to make it SQL-friendly."""
    sanitized_name = re.sub(r'[^A-Za-z0-9_]', '_', collection_name)
    return sanitized_name

def process_json_file(json_file, collection, cursor, conn):
    """Process the JSON file and insert data into SQL Server."""
    try:
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON in {json_file}. Error: {e}. Skipping...")
        return False

    table_name = sanitize_table_name(collection)

    if isinstance(data, list):
        create_table_if_not_exists(data[0], table_name, cursor)
        
        for entry in data:
            entry = convert_bson_to_sql_friendly(entry)

            columns = ', '.join(entry.keys())
            values = ', '.join([r"'%s'" % (str(value).replace('\\', '\\\\')) if value is not None else "NULL" for value in entry.values()])

            query = f"INSERT INTO [{table_name}] ({columns}) VALUES ({values})"
            try:
                cursor.execute(query)
            except Exception as e:
                print(f"Error inserting data into {table_name}. Error: {e}")
                conn.rollback()
                return False

        conn.commit()
        print(f"Inserted data from MongoDB collection '{collection}' into SQL Server.")
        
    elif isinstance(data, dict):
        create_table_if_not_exists(data, table_name, cursor)

        data = convert_bson_to_sql_friendly(data)

        columns = ', '.join(data.keys())
        values = ', '.join([r"'%s'" % (str(value).replace('\\', '\\\\')) if value is not None else "NULL" for value in data.values()])

        query = f"INSERT INTO [{table_name}] ({columns}) VALUES ({values})"
        try:
            cursor.execute(query)
        except Exception as e:
            print(f"Error inserting data into {table_name}. Error: {e}")
            conn.rollback()
            return False

        conn.commit()
        print(f"Inserted single document from MongoDB collection '{collection}' into SQL Server.")
        
    else:
        # Handle unexpected data format
        print(f"Data in {json_file} is not in the expected format (list or dict). Skipping...")

    return True


def create_table_if_not_exists(data, table_name, cursor):
    """Create the SQL table if it does not exist, based on the provided data structure."""
    # Determine column data types based on data types
    columns_with_types = []
    for key, value in data.items():
        data_type = "VARCHAR(MAX)"  # Default type
        if isinstance(value, int):
            data_type = "INT"
        elif isinstance(value, float):
            data_type = "FLOAT"
        elif isinstance(value, bool):
            data_type = "BIT"
        elif isinstance(value, dict):
            data_type = "VARCHAR(MAX)"  # Assuming nested dictionaries are treated as text
        # Add the column definition
        columns_with_types.append(f"[{key}] {data_type}")
    
    # Create table query
    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
    BEGIN
        CREATE TABLE [{table_name}] ({', '.join(columns_with_types)})
    END
    """
    cursor.execute(create_table_query)


def migrate_mongo_to_sql():
    """Migrate data from MongoDB to SQL Server."""
    password = "INDUS@123"
    encoded_password = urllib.parse.quote_plus(password)
    username = "anjali33100"
    cluster_url = "cluster0.ebjrk.mongodb.net"
    dbname = "Test_DB"
    
    uri = f"mongodb+srv://{username}:{encoded_password}@{cluster_url}/{dbname}"
    client = MongoClient(uri)
    db = client[dbname]
    collections = db.list_collection_names()

    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          'SERVER=192.168.0.195;'
                          'DATABASE=ANJ_TEST;'
                          'UID=sa;'
                          'PWD=sa@12345678')
    cursor = conn.cursor()

    for collection in collections:
        export_command = [
            r"C:\Users\anjsingh\Desktop\mongodb-database-tools-windows-x86_64-100.10.0\bin\mongoexport.exe", 
            f"--uri={uri}",
            f"--collection={collection}", 
            "--out", f"{collection}.json"
        ]
        
        try:
            subprocess.run(export_command, check=True)
            print(f"MongoDB collection '{collection}' exported to {collection}.json")
        except subprocess.CalledProcessError as e:
            print(f"Error while exporting MongoDB collection '{collection}': {e}")
            continue

        json_file = f"{collection}.json"
        if os.path.exists(json_file) and os.path.getsize(json_file) > 0:
            success = process_json_file(json_file, collection, cursor, conn)
            if success:
                os.remove(json_file)
                print(f"Deleted the JSON file '{json_file}' after import.")
        else:
            print(f"Skipping {json_file} as it is empty or does not exist.")

    cursor.close()
    conn.close()

def create_dacpac_using_sql_driver(output_path, server, database, username, password, max_retries=3):
    """Create a DACPAC file using SQL Server."""
    sqlpackage_path = r"C:\Users\anjsingh\Desktop\SQL-Driver\SqlPackage.exe"

    if not os.path.exists(sqlpackage_path):
        print(f"Error: SqlPackage.exe not found at {sqlpackage_path}")
        return

    connection_string = f"Data Source={server};Initial Catalog={database};User ID={username};Password={password};TrustServerCertificate=True"

    command = [
        "powershell.exe",
        "-Command",
        f"& '{sqlpackage_path}' /Action:Export /SourceConnectionString:\"{connection_string}\" /TargetFile:{output_path}"
    ]
    
    attempt = 0
    while attempt < max_retries:
        try:
            subprocess.run(command, check=True, shell=True)
            print(f"DACPAC file created successfully at {output_path}")
            break
        except subprocess.CalledProcessError as e:
            attempt += 1
            print(f"Error occurred while creating DACPAC: {e}")
            if attempt < max_retries:
                print(f"Retrying... Attempt {attempt}/{max_retries}")
                time.sleep(5)
            else:
                print("Max retry attempts reached. DACPAC creation failed.")

migrate_mongo_to_sql()

server = "192.168.0.195"
database = "ANJ_TEST"
username = "sa"
password = "sa@12345678"
output_path = os.path.join(dacpac_output_folder, f"{database}.bacpac") 

create_dacpac_using_sql_driver(output_path, server, database, username, password)


