import psycopg2
import os
import pandas as pd
from psycopg2 import sql
from meteostat import Point, Daily
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path="./environment.env")

# PostgreSQL credentials from environment variables
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DB = os.getenv('PG_DB')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# Initialize PostgreSQL database and table
def init_postgresql_database():
    """
    Creates the database (if it doesn't already exist).
    """
    connection = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname='postgres'  # Connect to the default 'postgres' database initially
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"CREATE DATABASE {PG_DB};")
        except psycopg2.errors.DuplicateDatabase:
            print(f"Database {PG_DB} already exists.")
    connection.close()

def init_postgresql_table():
    """
    Creates the air_quality table in the database.
    """
    create_table_query = """
    CREATE TABLE IF NOT EXISTS air_quality (
        id SERIAL PRIMARY KEY,
        date DATE,
        overall_aqi_value INT,
        main_pollutant VARCHAR(50),
        site_name VARCHAR(100),
        site_id VARCHAR(20),
        source VARCHAR(20),
        co REAL,
        ozone REAL,
        pm10 REAL,
        pm25 REAL,
        no2 REAL,
        temperature REAL,
        precipitation REAL,
        wind_speed REAL,
        UNIQUE (date, site_name, main_pollutant)
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

# Load CSV files and insert data into PostgreSQL
def load_csv_and_insert_into_postgres(file_paths):
    """
    Loads data from multiple CSV files and inserts it into the air_quality table.
    """
    column_mapping = {
        'Date': 'date',
        'Overall AQI Value': 'overall_aqi_value',
        'Main Pollutant': 'main_pollutant',
        'Site Name (of Overall AQI)': 'site_name',
        'Site ID (of Overall AQI)': 'site_id',
        'Source (of Overall AQI)': 'source',
        'CO': 'co',
        'Ozone': 'ozone',
        'PM10': 'pm10',
        'PM25': 'pm25',
        'NO2': 'no2'
    }

    insert_query = """
    INSERT INTO air_quality (
        date, overall_aqi_value, main_pollutant, site_name, site_id, source, co, ozone, pm10, pm25, no2
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (date, site_name, main_pollutant) DO NOTHING;
    """
    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    ) as connection:
        with connection.cursor() as curs:
            for file_path in file_paths:
                df = pd.read_csv(file_path)
                df = df.rename(columns=lambda col: col.strip())
                df = df.rename(columns=column_mapping)

                numeric_columns = ['co', 'ozone', 'pm10', 'pm25', 'no2']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                required_columns = list(column_mapping.values())
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    print(f"Missing columns in {file_path}: {missing_columns}")
                    continue

                for _, row in df.iterrows():
                    try:
                        curs.execute(insert_query, (
                            row['date'],
                            row['overall_aqi_value'],
                            row['main_pollutant'],
                            row['site_name'],
                            row['site_id'],
                            row['source'],
                            row['co'],
                            row['ozone'],
                            row['pm10'],
                            row['pm25'],
                            row['no2']
                        ))
                    except Exception as e:
                        print(f"Error inserting row: {e}")
                        continue
            connection.commit()

# Add weather data into PostgreSQL
def add_weather_data(start_date, end_date, latitude, longitude):
    """
    Adds weather data for a specific location and date range to the PostgreSQL database.
    """
    # Convert date strings to datetime objects
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Define location using Meteostat
    location = Point(latitude, longitude)
    weather_data = Daily(location, start=start_date, end=end_date).fetch()

    # Reset index to include the date as a column
    weather_data.reset_index(inplace=True)

    # Add a unique constraint to 'date' for updating weather data only
    add_constraint_query = """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conname = 'unique_date_constraint'
        ) THEN
            ALTER TABLE air_quality
            ADD CONSTRAINT unique_date_constraint UNIQUE (date);
        END IF;
    END $$;
    """

    insert_query = """
    INSERT INTO air_quality (
        date, temperature, precipitation, wind_speed
    ) VALUES (%s, %s, %s, %s)
    ON CONFLICT (date) DO UPDATE
    SET temperature = EXCLUDED.temperature,
        precipitation = EXCLUDED.precipitation,
        wind_speed = EXCLUDED.wind_speed;
    """
    with psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    ) as connection:
        with connection.cursor() as curs:
            # Add unique constraint if it doesn't exist
            curs.execute(add_constraint_query)
            connection.commit()

            # Insert weather data
            for _, row in weather_data.iterrows():
                try:
                    curs.execute(insert_query, (
                        row['time'],  # Date
                        row['tavg'],  # Average temperature
                        row['prcp'],  # Precipitation
                        row['wspd']   # Wind speed
                    ))
                except Exception as e:
                    print(f"Error inserting weather data: {e}")
            connection.commit()
    print("Weather data added successfully.")

# Main function
def main():
    try:
        init_postgresql_database()
    except Exception as e:
        print(f"Database initialization skipped: {e}")
    init_postgresql_table()

    file_paths = [
        "./phoenix_mesa_scottsdale_data/aqidaily2017.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2018.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2019.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2020.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2021.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2022.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2023.csv",
        "./phoenix_mesa_scottsdale_data/aqidaily2024.csv"
    ]
    load_csv_and_insert_into_postgres(file_paths)

    start_date = '2017-01-01'
    end_date = '2023-12-31'
    latitude = 33.4484
    longitude = -112.0740
    add_weather_data(start_date, end_date, latitude, longitude)

if __name__ == '__main__':
    main()
