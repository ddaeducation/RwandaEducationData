import pandas as pd
import numpy as np
import requests
import psycopg2
from dotenv import load_dotenv
import io
import os
from requests.auth import HTTPBasicAuth
from psycopg2.extras import execute_values  # for efficient bulk insert

# Load environment variables
load_dotenv()

# KoboToolbox credentials and CSV export URL
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aDXq36HCefxrUmhTbgx6pg/export-settings/esBd8FoDyMXDVBumMixQn4X/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_USER = os.getenv("PG_USER")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

# Schema and table
schema_name = "warrecords"
table_name = "rwanda"

# Fetching the CSV data from KoboToolbox
print("Fetching the data from KoboToolbox...")
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("Data was fetched successfully!")

    # Import CSV into pandas DataFrame
    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=";", on_bad_lines="skip")
    
    # Clean column names
    df.columns = df.columns.str.replace(" ", "_").str.replace("(", "").str.replace(")", "").str.replace("-", "_").str.replace(".", "_")

    print(f"Imported data with shape {df.shape}")
    print(df.head())

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    # Create schema if it does not exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")

    # Drop the table if it exists
    cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")

    # Create the table
    cur.execute(f"""
        CREATE TABLE {schema_name}.{table_name} (
            id SERIAL PRIMARY KEY,
            start TIMESTAMP,
            end TIMESTAMP,
            name TEXT,
            gender TEXT,
            age INT,
            program_of_study TEXT,
            year_of_enrollment TEXT,
            grade FLOAT,
            province TEXT,
            district TEXT,
            status TEXT,
            scholarship_status TEXT,
            enrollment_status TEXT,
            graduation_year TEXT,
            address TEXT
        )
    """)

    # Insert data efficiently
    insert_query = f"""
        INSERT INTO {schema_name}.{table_name} (
            start,
            end,
            name,
            gender,
            age,
            program_of_study,
            year_of_enrollment,
            grade,
            province,
            district,
            status,
            scholarship_status,
            enrollment_status,
            graduation_year,
            address
        ) VALUES %s
    """

    # Prepare the data for insertion
    data_to_insert = [
        (
            row.get('start'),
            row.get('end'),
            row.get('Name'),
            row.get('Gender'),
            row.get('Age'),
            row.get('Program_Of_Study'),
            row.get('Year_Of_Enrollment'),
            row.get('Grade'),
            row.get('Province'),
            row.get('District'),
            row.get('Status'),
            row.get('Scholarship_Status'),
            row.get('Enrollment_Status'),
            row.get('Graduation_Year'),
            row.get('Address')
        )
        for idx, row in df.iterrows()
    ]

    execute_values(cur, insert_query, data_to_insert)

    conn.commit()
    cur.close()
    conn.close()

    print("Successfully uploaded the data into PostgreSQL.")
else:
    print(f"Failed to fetch data from KoboToolbox. Status code: {response.status_code}")
    print("Response content:", response.text)
