import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
import io
import os
from requests.auth import HTTPBasicAuth

# connecting to KOBO_TOOLS_BOX
KOBO_USERNAME = os.getenv("KOBO_USERNAME")


