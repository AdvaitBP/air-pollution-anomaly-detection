"""Logging helpers for the project."""

from __future__ import annotations

import logging
from typing import Optional


def configure_logging(level: int = logging.INFO) -> None:
    """Configure a sensible logging format for CLI usage."""

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Return a module-level logger, configuring logging on first use."""

    if not logging.getLogger().handlers:
        configure_logging()
    return logging.getLogger(name)
