import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
SQL_TEMPLATE = os.getenv("SQL_TEMPLATE")

FILES_TO_INGEST = [
    "people.csv",
    "regions.csv"
]