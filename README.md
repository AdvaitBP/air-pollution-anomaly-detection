# Air Pollution Anomaly Detection

> Production-ready data ingestion pipelines for air quality anomaly detection research.

This repository packages several standalone scripts into a cohesive Python project. The
codebase now exposes a reusable command line interface and modular Python package for
fetching, normalising and persisting air quality metrics from multiple sources:

* **AirNow API** – real time measurements by ZIP code.
* **EPA Historical CSVs** – bulk AQI records downloaded as comma separated files.
* **Meteostat** – weather information to enrich air quality observations.

The end goal of the project is to provide clean, dependable datasets for exploratory
analysis and anomaly detection modelling.

## Project layout

```
├── data/                     # Source datasets (not tracked in Git history)
├── notebooks/                # Exploratory notebooks (optional)
├── src/air_pollution_anomaly_detection/
│   ├── airnow.py             # API models and fetch logic
│   ├── cli.py                # Command line entrypoint
│   ├── config.py             # Environment variable helpers
│   ├── csv_loader.py         # Historical CSV parsing utilities
│   ├── database.py           # Persistence layer for PostgreSQL
│   ├── ingest.py             # High-level ingestion orchestration
│   └── weather.py            # Meteostat integrations
├── tests/                    # Automated unit tests
├── get_air_data.py           # Compatibility wrapper for the CLI
├── csv_to_SQL.py             # Compatibility wrapper for the CLI
├── pyproject.toml            # Project metadata and dependencies
└── README.md
```

## Quick start

1. **Clone the repository** and create a virtual environment.

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

2. **Configure credentials** by copying `.env.example` to `.env` and filling in your
   database and AirNow API keys.

   ```bash
   cp .env.example .env
   ```

3. **Run an ingestion command**. The CLI exposes three sub-commands:

   ```bash
   # Fetch the latest AirNow observations for the configured ZIP code
   python -m air_pollution_anomaly_detection.cli ingest-airnow

   # Load historical CSV files
   python -m air_pollution_anomaly_detection.cli ingest-csv data/phoenix_mesa_scottsdale_data/*.csv

   # Add Meteostat weather metrics
   python -m air_pollution_anomaly_detection.cli ingest-weather 2017-01-01 2023-12-31 33.4484 -112.0740
   ```

Each command accepts `--env-file` if you keep credentials outside the project root.
The logging level can be adjusted via `--log-level DEBUG` for verbose output.

## Configuration reference

| Variable           | Description                                  |
|--------------------|----------------------------------------------|
| `AIRNOW_API_KEY`   | API key issued by [AirNow](https://docs.airnowapi.org/). |
| `AIRNOW_ZIP_CODE`  | Optional override for the target ZIP code.   |
| `AIRNOW_DISTANCE`  | Search radius in miles (default 25).         |
| `PG_HOST`          | PostgreSQL host name.                        |
| `PG_PORT`          | PostgreSQL port.                             |
| `PG_DB`            | Database name.                               |
| `PG_USER`          | Username.                                    |
| `PG_PASSWORD`      | Password.                                    |

Additional Meteostat configuration is provided through command line arguments.

## Testing

The project ships with a small test suite focused on the data normalisation layers.
Run the tests with `pytest`:

```bash
pytest
```

## Roadmap

* Build anomaly detection notebooks that leverage the curated datasets.
* Add Docker Compose for local Postgres + administration tooling.
* Publish the package to PyPI for easier reuse across repositories.

---

Maintained by [Your Name]. Contributions and issue reports are welcome!
