import requests
import os
import io
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, func, desc, Index
from sqlalchemy.orm import declarative_base, sessionmaker
import polars as pl
from config import BASE_URL
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

dbname = os.getenv("POSTGRES_DB")
user = os.getenv("POSTGRES_USER")  
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")

Base = declarative_base()

class People(Base):
    __tablename__ = 'people'
    person_id = Column(String, primary_key=True)
    first_name = Column(String)
    age = Column(Integer)
    language = Column(String)
    current_region_id = Column(Integer, ForeignKey('region.region_id'))
    household_id = Column(String)
    family_name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

Index('ix_people_person_id', People.person_id, unique=True, postgresql_using='btree')

class Region(Base):
    __tablename__ = 'region'
    region_id = Column(Integer, primary_key=True)
    ancient_name = Column(String)
    current_faction = Column(String)
    era_tag = Column(String)
    full_name = Column(String)
    colloquial_name = Column(String)
    founding_era = Column(String)
    density_tier = Column(String)
    capital = Column(String)
    primary_industry = Column(String)
    founding_story = Column(String)
    vote_history_last3 = Column(String)
    key_pressure_points = Column(String)
    unbound_presence = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

def create_database_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=user,
            password=password,
            host=host,
            port=port
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {dbname}")
            print(f"Database '{dbname}' created.")
        else:
            print(f"Database '{dbname}' already exists.")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"psycopg2 connection failed: {e}")
        return False
    
    
def create_tables(engine):
    Base.metadata.create_all(engine)
    print("All tables created (if not already present).")

def fetch_and_prepare_data():
    people_url = f"{BASE_URL}/people.csv"
    regions_url = f"{BASE_URL}/regions.csv"

    BYTE_LIMIT = 1_048_576

    people_response = requests.get(people_url, stream=True)
    people_response.raise_for_status()
    people_bytes = b""
    for chunk in people_response.iter_content(chunk_size=8192):
        people_bytes += chunk
        if len(people_bytes) >= BYTE_LIMIT:
            people_bytes = people_bytes[:BYTE_LIMIT]
            break

    region_response = requests.get(regions_url, stream=True)
    region_response.raise_for_status()
    region_bytes = b""
    for chunk in region_response.iter_content(chunk_size=8192):
        region_bytes += chunk
        if len(region_bytes) >= BYTE_LIMIT:
            region_bytes = region_bytes[:BYTE_LIMIT]
            break

    people_df = pl.read_csv(io.BytesIO(people_bytes))
    region_df = pl.read_csv(io.BytesIO(region_bytes))

    print("People columns:", people_df.columns)
    print("Region columns:", region_df.columns)

    people_df = people_df.head(5000)
    region_df = region_df.head(200)

    return people_df, region_df

def load_sample_data(engine, people_df, region_df):
    region_column_mapping = {
        "Region_ID": "region_id",
        "Ancient_Name": "ancient_name",
        "Current_Faction": "current_faction",
        "Era_Tag": "era_tag",
        "Full_Name": "full_name",
        "Colloquial_Name": "colloquial_name",
        "Founding_Era": "founding_era",
        "Density_Tier": "density_tier",
        "Capital": "capital",
        "Primary_Industry": "primary_industry",
        "Founding_Story": "founding_story",
        "Vote_History_Last3": "vote_history_last3",
        "Key_Pressure_Points": "key_pressure_points",
        "Unbound_Presence": "unbound_presence"
    }

    region_df = region_df.rename(region_column_mapping)
    region_objs = [Region(**row) for row in region_df.to_dicts()]
    people_objs = [People(**row) for row in people_df.to_dicts()]

    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        session.bulk_save_objects(region_objs)
        session.bulk_save_objects(people_objs)
        session.commit()
        print("Sample data loaded successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error loading sample data: {e}")
    finally:
        session.close()

def run_analysis_queries(engine):    
    Session = sessionmaker(bind=engine)
    session = Session()

    region_population_subq = (
        session.query(
            People.current_region_id.label("region_id"),
            func.count(People.person_id).label("region_population")
        )
        .group_by(People.current_region_id)
        .subquery()
    )

    results = (
        session.query(
            Region.full_name,
            func.avg(People.age).label("average_age"),
            region_population_subq.c.region_population
        )
        .join(People, Region.region_id == People.current_region_id)
        .join(region_population_subq, Region.region_id == region_population_subq.c.region_id)
        .group_by(Region.full_name, region_population_subq.c.region_population)
        .order_by(desc(region_population_subq.c.region_population))
        .all()
    )

    for row in results:
        print(row.full_name, row.average_age, row.region_population)

    results = (
        session.query(
            Region.full_name,
            func.count(func.distinct(People.person_id)).label("region_population")
        )
        .join(People, Region.region_id == People.current_region_id)
        .group_by(Region.full_name)
        .order_by(desc("region_population"))
        .all()
    )

    for row in results:
        print(row.full_name, row.region_population)

    session.close()

def main():
    if not create_database_connection():
        return

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
    try:
        with engine.connect() as connection:
            print("SQLAlchemy connection successful!")
    except Exception as e:
        print(f"SQLAlchemy connection failed: {e}")

    create_tables(engine)

    people_df, region_df = fetch_and_prepare_data()

    load_sample_data(engine, people_df, region_df)

    run_analysis_queries(engine)

if __name__ == "__main__":
    main()


