import pandas as pd
import numpy as np
import requests
import psycopg2
from dotenv import load_dotenv
import io
import os
from requests.auth import HTTPBasicAuth

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

# Connecting the SHEMA & DATABASE
schema_name : "edtech"
table_name : "rwanda"


