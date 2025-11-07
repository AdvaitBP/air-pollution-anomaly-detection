"""Helpers for loading AQI CSV files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

import pandas as pd

from .logging_utils import get_logger

logger = get_logger(__name__)

COLUMN_MAPPING = {
    "Date": "date_observed",
    "Overall AQI Value": "overall_aqi_value",
    "Main Pollutant": "main_pollutant",
    "Site Name (of Overall AQI)": "site_name",
    "Site ID (of Overall AQI)": "site_id",
    "Source (of Overall AQI)": "source",
    "CO": "co",
    "Ozone": "ozone",
    "PM10": "pm10",
    "PM25": "pm25",
    "NO2": "no2",
}


@dataclass(slots=True)
class AqiDailyRecord:
    """Normalized representation of a historical AQI row."""

    date_observed: str
    overall_aqi_value: int | None
    main_pollutant: str | None
    site_name: str | None
    site_id: str | None
    source: str | None
    co: float | None
    ozone: float | None
    pm10: float | None
    pm25: float | None
    no2: float | None

    def as_db_tuple(self) -> tuple:
        return (
            self.date_observed,
            self.overall_aqi_value,
            self.main_pollutant,
            self.site_name,
            self.site_id,
            self.source,
            self.co,
            self.ozone,
            self.pm10,
            self.pm25,
            self.no2,
        )


def load_aqi_csv(path: Path) -> Sequence[AqiDailyRecord]:
    """Load and normalize AQI data from ``path``."""

    logger.debug("Loading AQI CSV %s", path)
    df = pd.read_csv(path).rename(columns=lambda col: col.strip())
    df = df.rename(columns=COLUMN_MAPPING)
    required_columns = list(COLUMN_MAPPING.values())
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"Missing expected columns {missing_columns} in {path.name}."
        )
    numeric_columns = ["co", "ozone", "pm10", "pm25", "no2", "overall_aqi_value"]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    records = [AqiDailyRecord(**row) for row in df[required_columns].to_dict("records")]
    logger.debug("Parsed %d rows from %s", len(records), path)
    return records


def load_aqi_csvs(paths: Iterable[Path]) -> Iterator[AqiDailyRecord]:
    """Yield normalized records from multiple CSV paths."""

    for path in paths:
        yield from load_aqi_csv(path)
