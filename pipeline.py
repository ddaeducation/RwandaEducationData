import pandas as pd
import numpy as np
import requests
import psycopg2
from dotenv import load_dotenv
import io
import os
from requests.auth import HTTPBasicAuth

# Load environment variables from a .env file
load_dotenv()

# KoboToolbox credentials and CSV export URL
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = ""

# PostgreSQL Credentials 
PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Targeting the schema and table from PostgreSQL
schema_name = "edtech"
table_name = "rwanda"

# importing the data into python
# ===========   END  ===================
# Fetching the csv data from PostgreSQL
print("I am happy, I have fetched the Data From Kobotoolbox ...")
response = requests.get(KOBO_CSV_URL,auth=HTTPBasicAuth(KOBO_USERNAME,KOBO_PASSWORD))

# Checking if the data was fetched successfull
if response.status_code == 200:
    print("Woow, data was fetched successful")

    # Importing the dataframe 
    csv_data = io.StringIO(response.text)
    df =pd.read_csv(csv_data, sep=";", on_bad_lines="skip")
    
    # Importing the data frame
    print(f"I have successfull import data {df.head}")
 


    # ================ END =====================

    # Uploading the Data into possigress
    conn=psycopg2.connect(
        host = PG_HOST,
        database = PG_DATABASE,
        user = PG_HOST,
        port = PG_PORT,
        password = PG_PASSWORD
    )
    cur = conn.cursor()

    # Creating the schema if it doesn't exit 
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {schema_name};")

    # Drop the table if exists 
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

    # Create a new table with predefined schema
    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS{schema_name}.{table_name}(
            Id SERIAL PRIMARY KEY,
            "start" TIMESTAMP,
            "end" TIMESTAMP,
            "date" "D,

        
        )
        """
    )
