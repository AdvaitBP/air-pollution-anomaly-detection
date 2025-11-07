"""Weather data utilities leveraging Meteostat."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from meteostat import Daily, Point

from .logging_utils import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class WeatherRecord:
    """Daily weather metrics used for enrichment."""

    date_observed: datetime
    temperature: float | None
    precipitation: float | None
    wind_speed: float | None

    def as_db_tuple(self) -> tuple:
        return (
            self.date_observed,
            self.temperature,
            self.precipitation,
            self.wind_speed,
        )


def fetch_weather_records(
    start_date: datetime,
    end_date: datetime,
    latitude: float,
    longitude: float,
) -> Iterator[WeatherRecord]:
    """Fetch weather metrics for the provided location and date range."""

    location = Point(latitude, longitude)
    dataset = Daily(location, start=start_date, end=end_date).fetch()
    dataset = dataset.reset_index()
    for row in dataset.to_dict("records"):
        yield WeatherRecord(
            date_observed=row["time"],
            temperature=row.get("tavg"),
            precipitation=row.get("prcp"),
            wind_speed=row.get("wspd"),
        )
