"""Interactions with the AirNow API."""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable

import requests

from .config import AirNowConfig
from .logging_utils import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class AirNowObservation:
    """Normalized AirNow observation payload."""

    date_observed: dt.date
    hour_observed: int
    local_time_zone: str
    reporting_area: str
    state_code: str
    latitude: float
    longitude: float
    parameter_name: str
    aqi: int
    category_number: int | None
    category_name: str | None

    @classmethod
    def from_payload(cls, payload: dict) -> "AirNowObservation":
        category = payload.get("AQICategory") or {}
        date_str = payload.get("DateObserved")
        date_observed = (
            dt.datetime.strptime(date_str, "%Y-%m-%d").date()
            if isinstance(date_str, str)
            else dt.date.today()
        )
        return cls(
            date_observed=date_observed,
            hour_observed=int(payload.get("HourObserved") or 0),
            local_time_zone=str(payload.get("LocalTimeZone") or ""),
            reporting_area=str(payload.get("ReportingArea") or ""),
            state_code=str(payload.get("StateCode") or ""),
            latitude=float(payload.get("Latitude") or 0.0),
            longitude=float(payload.get("Longitude") or 0.0),
            parameter_name=str(payload.get("ParameterName") or ""),
            aqi=int(payload.get("AQI") or 0),
            category_number=category.get("Number"),
            category_name=category.get("Name"),
        )

    def as_db_tuple(self, fetched_at: dt.datetime) -> tuple:
        """Return a tuple shaped for database insertion."""

        return (
            self.date_observed,
            self.hour_observed,
            self.local_time_zone,
            self.reporting_area,
            self.state_code,
            self.latitude,
            self.longitude,
            self.parameter_name,
            self.aqi,
            self.category_number,
            self.category_name,
            fetched_at,
        )


def build_request_params(config: AirNowConfig) -> dict[str, str]:
    """Build query parameters for the AirNow API."""

    return {
        "format": config.response_format,
        "zipCode": config.zip_code,
        "distance": str(config.distance),
        "api_key": config.api_key,
    }


def fetch_airnow_observations(config: AirNowConfig) -> list[AirNowObservation]:
    """Fetch observations from AirNow and normalize the payloads."""

    params = build_request_params(config)
    logger.info("Requesting data from AirNow for zip %s", config.zip_code)
    response = requests.get(
        "https://www.airnowapi.org/aq/observation/zipCode/current/",
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    payload: Iterable[dict] = response.json()
    observations = [AirNowObservation.from_payload(record) for record in payload]
    logger.info("Fetched %d AirNow observations", len(observations))
    return observations
