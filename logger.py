"""
Centralised logger factory.
Import this in every module so all output is consistent.
"""

import logging
import os
from config import LOG_LEVEL, LOG_DIR


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger for the given module name."""
    os.makedirs(LOG_DIR, exist_ok=True)

    logger = logging.getLogger(name)

    if logger.handlers:          # Avoid duplicate handlers in Airflow
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File
    fh = logging.FileHandler(os.path.join(LOG_DIR, "etl.log"))
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger
