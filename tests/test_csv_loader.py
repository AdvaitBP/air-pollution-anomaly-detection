from pathlib import Path

import pandas as pd
import pytest

from air_pollution_anomaly_detection.csv_loader import AqiDailyRecord, load_aqi_csv


def test_load_aqi_csv(tmp_path: Path):
    data = {
        "Date": ["2024-01-01"],
        "Overall AQI Value": [50],
        "Main Pollutant": ["PM2.5"],
        "Site Name (of Overall AQI)": ["Downtown"],
        "Site ID (of Overall AQI)": ["123"],
        "Source (of Overall AQI)": ["EPA"],
        "CO": [0.1],
        "Ozone": [0.02],
        "PM10": [12.0],
        "PM25": [10.0],
        "NO2": [15.0],
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "aqi.csv"
    df.to_csv(csv_path, index=False)

    records = load_aqi_csv(csv_path)
    assert len(records) == 1
    record = records[0]
    assert isinstance(record, AqiDailyRecord)
    assert record.date_observed == "2024-01-01"
    assert record.overall_aqi_value == 50
    assert record.co == pytest.approx(0.1)


def test_missing_columns_raise(tmp_path: Path):
    df = pd.DataFrame({"Date": ["2024-01-01"]})
    csv_path = tmp_path / "aqi.csv"
    df.to_csv(csv_path, index=False)

    with pytest.raises(ValueError):
        load_aqi_csv(csv_path)
