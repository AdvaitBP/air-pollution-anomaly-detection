"""Database utilities for persisting air quality data."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Iterator, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from .config import DatabaseConfig
from .logging_utils import get_logger

logger = get_logger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS air_quality (
    id SERIAL PRIMARY KEY,
    date_observed DATE,
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
    data_retrieved_at TIMESTAMP,
    temperature REAL,
    precipitation REAL,
    wind_speed REAL,
    overall_aqi_value INT,
    main_pollutant VARCHAR(50),
    site_name VARCHAR(100),
    site_id VARCHAR(20),
    source VARCHAR(20)
);
"""


@contextmanager
def get_connection(config: DatabaseConfig, *, autocommit: bool = False) -> Iterator[PGConnection]:
    """Yield a psycopg connection using the provided configuration."""

    connection = psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
    )
    try:
        if autocommit:
            connection.autocommit = True
        yield connection
    finally:
        connection.close()


class PostgresAirQualityRepository:
    """High-level operations for storing air quality data."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config

    def ensure_database_exists(self, maintenance_db: str = "postgres") -> None:
        """Create the target database if it does not already exist."""

        logger.debug("Ensuring database %s exists", self.config.database)
        connection = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            dbname=maintenance_db,
            user=self.config.user,
            password=self.config.password,
        )
        try:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (self.config.database,),
                )
                exists = cursor.fetchone() is not None
                if not exists:
                    cursor.execute(
                        sql.SQL("CREATE DATABASE {}"
                        ).format(sql.Identifier(self.config.database))
                    )
                    logger.info("Created database %s", self.config.database)
        finally:
            connection.close()

    def ensure_schema(self) -> None:
        """Ensure the air_quality table is present."""

        logger.debug("Ensuring air_quality table exists")
        with get_connection(self.config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(CREATE_TABLE_SQL)
                connection.commit()

    def insert_airnow_observations(self, records: Sequence[tuple]) -> int:
        """Insert AirNow observations into the database."""

        if not records:
            logger.info("No AirNow observations to insert.")
            return 0
        insert_sql = """
        INSERT INTO air_quality (
            date_observed, hour_observed, local_time_zone, reporting_area, state_code,
            latitude, longitude, parameter_name, aqi, category_number, category_name,
            data_retrieved_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        with get_connection(self.config) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(insert_sql, records)
            connection.commit()
        logger.info("Inserted %d AirNow rows", len(records))
        return len(records)

    def insert_aqi_csv_records(self, rows: Iterable[tuple]) -> int:
        """Insert historical AQI CSV data."""

        insert_sql = """
        INSERT INTO air_quality (
            date_observed, overall_aqi_value, main_pollutant, site_name, site_id, source,
            co, ozone, pm10, pm25, no2
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_observed, site_name, main_pollutant) DO NOTHING;
        """
        count = 0
        with get_connection(self.config) as connection:
            with connection.cursor() as cursor:
                for row in rows:
                    cursor.execute(insert_sql, row)
                    count += 1
            connection.commit()
        logger.info("Inserted %d rows from CSV files", count)
        return count

    def upsert_weather_metrics(self, rows: Iterable[tuple]) -> int:
        """Upsert weather metrics keyed by date."""

        add_constraint_sql = """
        DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint WHERE conname = 'unique_date_constraint'
        ) THEN
            ALTER TABLE air_quality
            ADD CONSTRAINT unique_date_constraint UNIQUE (date_observed);
        END IF;
        END $$;
        """

        upsert_sql = """
        INSERT INTO air_quality (date_observed, temperature, precipitation, wind_speed)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (date_observed) DO UPDATE
        SET temperature = EXCLUDED.temperature,
            precipitation = EXCLUDED.precipitation,
            wind_speed = EXCLUDED.wind_speed;
        """
        count = 0
        with get_connection(self.config) as connection:
            with connection.cursor() as cursor:
                cursor.execute(add_constraint_sql)
                for row in rows:
                    cursor.execute(upsert_sql, row)
                    count += 1
            connection.commit()
        logger.info("Upserted weather metrics for %d dates", count)
        return count


__all__ = ["PostgresAirQualityRepository", "get_connection"]
