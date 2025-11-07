"""Configuration helpers for the air pollution anomaly detection project."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os

from dotenv import load_dotenv


def load_environment(env_file: Optional[Path] = None) -> None:
    """Load environment variables from a ``.env`` file if present."""

    if env_file:
        load_dotenv(env_file)
    else:
        # Load default .env if present in working directory without failing when absent.
        load_dotenv()


@dataclass(slots=True)
class DatabaseConfig:
    """Connection settings for PostgreSQL."""

    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls, prefix: str = "PG_") -> "DatabaseConfig":
        """Create a configuration instance using environment variables."""

        required_keys = {"HOST", "PORT", "DB", "USER", "PASSWORD"}
        values: dict[str, str] = {}
        missing: list[str] = []
        for key in required_keys:
            env_key = f"{prefix}{key}"
            value = os.getenv(env_key)
            if value is None:
                missing.append(env_key)
            else:
                values[key.lower()] = value
        if missing:
            missing_list = ", ".join(missing)
            raise RuntimeError(
                f"Missing required database environment variables: {missing_list}."
            )
        return cls(
            host=values["host"],
            port=int(values["port"]),
            database=values["db"],
            user=values["user"],
            password=values["password"],
        )


@dataclass(slots=True)
class AirNowConfig:
    """Configuration for querying the AirNow API."""

    api_key: str
    zip_code: str = "27705"
    distance: int = 25
    response_format: str = "application/json"

    @classmethod
    def from_env(cls, api_key_var: str = "AIRNOW_API_KEY") -> "AirNowConfig":
        api_key = os.getenv(api_key_var)
        if not api_key:
            raise RuntimeError(
                f"Environment variable {api_key_var} is required to call the AirNow API."
            )
        zip_code = os.getenv("AIRNOW_ZIP_CODE", "27705")
        distance = int(os.getenv("AIRNOW_DISTANCE", "25"))
        response_format = os.getenv("AIRNOW_FORMAT", "application/json")
        return cls(
            api_key=api_key,
            zip_code=zip_code,
            distance=distance,
            response_format=response_format,
        )


@dataclass(slots=True)
class AppConfig:
    """Aggregate configuration for application components."""

    database: DatabaseConfig
    airnow: AirNowConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            database=DatabaseConfig.from_env(),
            airnow=AirNowConfig.from_env(),
        )
