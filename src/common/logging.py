"""Logging helpers shared by every service."""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False
_NOISY_LOGGERS = ("aiokafka", "kafka", "pyspark", "py4j")


def setup_logging(level: str = "INFO") -> None:
    """Configure the root logger once. Safe to call repeatedly."""
    global _CONFIGURED
    root = logging.getLogger()
    root.setLevel(level.upper())

    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s %(levelname)-8s %(name)s :: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        root.handlers.clear()
        root.addHandler(handler)
        _CONFIGURED = True

    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(max(logging.WARNING, root.level))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
