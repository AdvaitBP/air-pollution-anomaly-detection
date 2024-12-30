import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="./environment.env")

# PostgreSQL credentials from environment variables
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DB = os.getenv('PG_DB')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Set the display options to show all columns
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Prevent wrapping of columns
pd.set_option('display.max_rows', None)  # Optionally, show all rows if needed

def fetch_data():
    query = """
    SELECT
    *
    FROM air_quality;  -- Replace with your actual table name
    """
    
    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    ) as conn:
        df = pd.read_sql(query, conn)
    return df

df = fetch_data()
print(df.head())
print(df.shape)

print(df.info())

df['date'] = pd.to_datetime(df['date'], errors='coerce')

print(df.describe(include='all'))

df.isnull().sum()

missing_counts = df.isnull().sum().sort_values(ascending=False)
print(missing_counts)
