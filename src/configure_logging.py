"""Logging module."""

import logging
import sys
from logging.handlers import RotatingFileHandler

import coloredlogs

from src.config import ROOT_PATH


def configure_logging() -> None:
    """Configure logging."""
    logFile = ROOT_PATH + "logs/runtime-beatporter.log"
    logging.getLogger().setLevel(logging.NOTSET)
    logging.getLogger().handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(message)s")
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    fileh = RotatingFileHandler(
        logFile,
        mode="w",
        maxBytes=50 * 1024 * 1024,
        backupCount=1,
        encoding=None,
        delay=False,
    )
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s [%(filename)s:%(lineno)d]",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fileh.setFormatter(formatter)
    fileh.setLevel(logging.INFO)
    logging.getLogger().addHandler(fileh)

    fileh = RotatingFileHandler(
        ROOT_PATH + "logs/runtime-beatporter-debug.log",
        "w",
        maxBytes=150 * 1024 * 1024,
        backupCount=1,
        encoding=None,
        delay=False,
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(message)s [%(filename)s:%(lineno)d]", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fileh.setFormatter(formatter)
    fileh.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(fileh)

    # Add a dedicated warnings file (captures WARNING and above)
    warn_fileh = RotatingFileHandler(
        ROOT_PATH + "logs/runtime-beatporter-warnings.log",
        mode="w",
        maxBytes=50 * 1024 * 1024,
        backupCount=1,
        encoding=None,
        delay=False,
    )
    warn_formatter = logging.Formatter(
        "%(asctime)s %(levelname)s %(message)s [%(filename)s:%(lineno)d]",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    warn_fileh.setFormatter(warn_formatter)
    warn_fileh.setLevel(logging.WARNING)
    logging.getLogger().addHandler(warn_fileh)

    coloredlogs.install(
        level="INFO",
        fmt="%(asctime)s %(levelname)s %(message)s",
        # fmt="%(asctime)s %(levelname)s %(message)s [%(filename)s:%(lineno)d]",
        isatty=True,
        stream=sys.stdout,
    )

    # Remove spotify util.py logs
    # Need to modify to LOGGER at .venv/lib/python3.13/site-packages/spotipy/util.py:169
    logging.getLogger("spotipy.util").setLevel(logging.ERROR)
