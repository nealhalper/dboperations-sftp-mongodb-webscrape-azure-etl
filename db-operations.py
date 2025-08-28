import requests
import os
import psycopg2
from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
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

engine = create_engine(
    f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )   
try:
    with engine.connect() as connection:
        print("SQLAlchemy connection successful!")
except Exception as e:
    print(f"SQLAlchemy connection failed: {e}")

Base = declarative_base()

class People(Base):
    __tablename__ = 'people'
    id = Column(Integer, primary_key=True)
    name = Column(String)

class Region(Base):
    __tablename__ = 'region'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('people.id'))
    name = Column(String)
    created_at = Column(DateTime)
    parent = relationship("People")

Base.metadata.create_all(connection)

def create_database_connection():
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        print("psycopg2 connection successful!")
        conn.close()
        return True
    except Exception as e:
        print(f"psycopg2 connection failed: {e}")
        return False
    
    
def create_tables(connection):

def load_sample_data(connection, df):

def run_analysis_queries(connection):    



if __name__ == "__main__":
    if create_database_connection():
        print("Test: psycopg2 connection test passed.")
    else:
        print("Test: psycopg2 connection test failed.")


