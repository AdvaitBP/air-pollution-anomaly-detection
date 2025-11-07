"""Command line interface for data ingestion workflows."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Iterable

from .config import AppConfig, load_environment
from .ingest import ingest_airnow, ingest_aqi_csvs, ingest_weather
from .logging_utils import configure_logging, get_logger

logger = get_logger(__name__)


def _parse_date(value: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a valid date in YYYY-MM-DD format"
        ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional path to a .env file containing credentials.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    airnow_parser = subparsers.add_parser(
        "ingest-airnow", help="Fetch the latest AirNow observations and store them."
    )
    airnow_parser.set_defaults(func=_run_airnow)

    csv_parser = subparsers.add_parser(
        "ingest-csv", help="Load historical AQI CSV files into the database."
    )
    csv_parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Paths to AQI CSV files.",
    )
    csv_parser.set_defaults(func=_run_csv)

    weather_parser = subparsers.add_parser(
        "ingest-weather", help="Fetch Meteostat weather data and upsert it."
    )
    weather_parser.add_argument("start", type=_parse_date, help="Start date YYYY-MM-DD")
    weather_parser.add_argument("end", type=_parse_date, help="End date YYYY-MM-DD")
    weather_parser.add_argument("latitude", type=float, help="Latitude of the location")
    weather_parser.add_argument("longitude", type=float, help="Longitude of the location")
    weather_parser.set_defaults(func=_run_weather)

    return parser


def _configure(parser_args: argparse.Namespace) -> AppConfig:
    configure_logging(getattr(parser_args, "log_level", "INFO"))
    load_environment(parser_args.env_file)
    return AppConfig.from_env()


def _run_airnow(args: argparse.Namespace) -> int:
    config = _configure(args)
    return ingest_airnow(config)


def _run_csv(args: argparse.Namespace) -> int:
    config = _configure(args)
    return ingest_aqi_csvs(config, args.paths)


def _run_weather(args: argparse.Namespace) -> int:
    config = _configure(args)
    return ingest_weather(
        config,
        start_date=args.start,
        end_date=args.end,
        latitude=args.latitude,
        longitude=args.longitude,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = args.func(args)
    logger.info("Command completed with %s records affected", result)
    return result


if __name__ == "__main__":  # pragma: no cover
    main()
