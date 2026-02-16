"""
Logging configuration module.

This module provides centralized logging configuration for the entire application.
"""
import logging
import logging.config
import sys
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[Path] = None,
    log_to_console: bool = True,
    log_format: Optional[str] = None
) -> None:
    """
    Configure logging for the application.

    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    :type log_level: str
    :param log_file: Path to log file. If None, no file logging is performed
    :type log_file: Optional[Path]
    :param log_to_console: Whether to log to console
    :type log_to_console: bool
    :param log_format: Custom log format string. If None, uses default format
    :type log_format: Optional[str]
    """
    if log_format is None:
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )

    # Create formatters
    formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # File handler
    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance with the specified name.

    :param name: Logger name (typically __name__ of the calling module)
    :type name: str
    :return: Configured logger instance
    :rtype: logging.Logger
    """
    return logging.getLogger(name)
