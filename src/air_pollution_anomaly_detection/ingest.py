"""Orchestration helpers for the ingestion workflows."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Iterable

from .airnow import fetch_airnow_observations
from .config import AppConfig
from .csv_loader import load_aqi_csvs
from .database import PostgresAirQualityRepository
from .logging_utils import get_logger
from .weather import WeatherRecord, fetch_weather_records

logger = get_logger(__name__)


def ingest_airnow(config: AppConfig) -> int:
    """Fetch AirNow observations and persist them to the database."""

    repository = PostgresAirQualityRepository(config.database)
    repository.ensure_database_exists()
    repository.ensure_schema()
    fetched_at = dt.datetime.utcnow()
    observations = fetch_airnow_observations(config.airnow)
    tuples = [observation.as_db_tuple(fetched_at) for observation in observations]
    return repository.insert_airnow_observations(tuples)


def ingest_aqi_csvs(config: AppConfig, csv_paths: Iterable[Path]) -> int:
    """Load historical AQI CSV files and persist them."""

    repository = PostgresAirQualityRepository(config.database)
    repository.ensure_database_exists()
    repository.ensure_schema()
    records = load_aqi_csvs(csv_paths)
    tuples = (record.as_db_tuple() for record in records)
    return repository.insert_aqi_csv_records(tuples)


def ingest_weather(
    config: AppConfig,
    *,
    start_date: dt.datetime,
    end_date: dt.datetime,
    latitude: float,
    longitude: float,
) -> int:
    """Fetch weather data and upsert it into the database."""

    repository = PostgresAirQualityRepository(config.database)
    repository.ensure_database_exists()
    repository.ensure_schema()
    records = fetch_weather_records(start_date, end_date, latitude, longitude)
    tuples = (record.as_db_tuple() for record in records)
    return repository.upsert_weather_metrics(tuples)


__all__ = ["ingest_airnow", "ingest_aqi_csvs", "ingest_weather", "WeatherRecord"]
