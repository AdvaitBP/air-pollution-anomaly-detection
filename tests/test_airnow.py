import datetime as dt

import pytest

from air_pollution_anomaly_detection.airnow import AirNowObservation


@pytest.fixture
def sample_payload():
    return {
        "DateObserved": "2024-04-19",
        "HourObserved": 11,
        "LocalTimeZone": "EST",
        "ReportingArea": "Durham",
        "StateCode": "NC",
        "Latitude": 35.994,
        "Longitude": -78.8986,
        "ParameterName": "PM2.5",
        "AQI": 42,
        "AQICategory": {"Number": 2, "Name": "Good"},
    }


def test_airnow_observation_from_payload(sample_payload):
    observation = AirNowObservation.from_payload(sample_payload)
    assert observation.date_observed == dt.date(2024, 4, 19)
    assert observation.hour_observed == 11
    assert observation.parameter_name == "PM2.5"
    assert observation.category_name == "Good"


def test_airnow_observation_as_db_tuple(sample_payload):
    observation = AirNowObservation.from_payload(sample_payload)
    fetched_at = dt.datetime(2024, 4, 20, 12, 0, 0)
    db_tuple = observation.as_db_tuple(fetched_at)
    assert db_tuple[-1] == fetched_at
    assert db_tuple[0] == dt.date(2024, 4, 19)
