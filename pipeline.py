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
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aDXq36HCefxrUmhTbgx6pg/export-settings/esBd8FoDyMXDVBumMixQn4X/data.csv"

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
    df.columns = df.columns.str.replace(" ","_").str.replace("(","").str.replace(")","").str.replace("-","_").str.replace(".","_") 
    
    # Importing the data frame
    print(f"I have successfull import data {df.head}")
    
    # ================ END =====================
    # https://simplemaps.com/svg/country/rw#admin1 & https://mapshaper.org/
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
            Name TEXT,
            Gender TEXT,
            Age INT,
            Program_Of_Study TEXT,
            Year_Of_Enrollment TEXT,
            Grade FLOAT,
            Province TEXT,
            District TEXT,
            Status TEXT,
            Scholarship_Status TEXT,
            Enrollment_Status TEXT,
            Graduation_Year TEXT,
            Address TEXT
        )
    """)
    # Inserting the data into the table
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
            "start",
            "end",
            Name,
            Gender,
            age,
            Program_Of_Study,
            Year_Of_Enrollment,
            Grade,
            Province,
            District,
            Status,
            Scholarship_Status,
            Enrollment_Status,
            Graduation_Year,
            Address
        ) VALUES %s;
    """

    # 