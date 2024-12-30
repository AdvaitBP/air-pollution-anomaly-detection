import requests
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient
import os
import datetime
from dotenv import load_dotenv


# Load environment variables
load_dotenv(dotenv_path=".\environment.env")

# Constants
API_KEY = os.getenv('AIRNOW_API_KEY')

PG_HOST=os.getenv('PG_HOST')
PG_PORT=os.getenv('PG_PORT')
PG_DB=os.getenv('PG_DB')
PG_USER=os.getenv('PG_USER')
PG_PASSWORD=os.getenv('PG_PASSWORD')


# Check Postgres credentials
print(f"[INFO] Postgres config -> Host:{PG_HOST}, Port:{PG_PORT}, DB:{PG_DB}, User:{PG_USER}")


# Debugging print
if API_KEY:
    print(f"API Key loaded successfully: {API_KEY}")
else:
    print("Failed to load API Key! Check your .env file.")

# AIRNOW API Parameters
BASE_URL = "https://www.airnowapi.org/aq/observation/zipCode/current/"
ZIP_CODE = "27705"  # Example zip code (Duke University area)
DISTANCE = "25"  # Search within 25 miles
FORMAT = "application/json"

# Fetch data from AirNow API
def fetch_air_quality_data():
    """
    Attempts to fetch air quality data from AirNOW API
    Returns list of dictionaries
    """
    
    params = {
    "format": FORMAT,
    "zipCode": ZIP_CODE,
    "distance": DISTANCE,
    "api_key": API_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params)
        print(f"Requesting URL: {response.url}")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []


# initialize postgresql table 

def init_postgresql_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS air_quality (
        id SERIAL PRIMARY KEY,
        date_observed VARCHAR(50),
        hour_observed INT,
        local_time_zone VARCHAR(10),
        reporting_area VARCHAR(100),
        state_code VARCHAR(10),
        latitude REAL,
        longitude REAL,
        parameter_name VARCHAR(50),
        aqi INT,
        category_number INT,
        category_name VARCHAR(50),
        data_retrieved_at TIMESTAMP
        );
    """

    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    ) as connection:
        with connection.cursor() as curs:
            curs.execute(create_table_query)
            connection.commit()


# Insert data into postgres table
def store_data_in_postgres_table(data):
    """
    input: data is expected to be list of dictionary items that represent each row in air_quality table
    function inserts data into table
    """
    fetched_time = datetime.datetime.now()

    insert_query = """
    INSERT INTO air_quality(date_observed, hour_observed, local_time_zone, reporting_area, state_code, latitude, longitude, parameter_name, aqi, category_number, category_name, data_retrieved_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    with psycopg2.connect(host=PG_HOST, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB, port=PG_PORT) as connection:
        with connection.cursor() as curs:
            for record in data:
                date_observed = record.get("DateObserved")
                hour_observed = record.get("HourObserved")
                local_time_zone = record.get("LocalTimeZone")
                reporting_area = record.get("ReportingArea")
                state_code = record.get("StateCode")
                latitude = record.get("Latitude")
                longitude = record.get("Longitude")
                parameter_name = record.get("ParameterName")
                aqi = record.get("AQI")
                category_number = record.get("AQICategory", {}).get("Number")
                category_name = record.get("AQICategory", {}).get("Name")
                curs.execute(insert_query, (
                        date_observed, hour_observed, local_time_zone,
                        reporting_area, state_code, latitude, longitude,
                        parameter_name, aqi, category_number, category_name, fetched_time))
                
            connection.commit()


def main():
    init_postgresql_table()

    print("Fetching air quality data...")
    data = fetch_air_quality_data()

    store_data_in_postgres_table(data)

if __name__ == '__main__':
    main()