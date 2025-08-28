import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
SQL_TEMPLATE = os.getenv("SQL_TEMPLATE")

FILES_TO_INGEST = [
    "faction_distribution.csv",
    "households.csv",
    "language_building_blocks.csv",
    "language_roots.csv",
    "moons.csv",
    "people.csv",
    "planets.csv",
    "region_biome.csv",
    "regions.csv"
]