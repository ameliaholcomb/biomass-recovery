"""Utils for setting up a consistent logging framework"""
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from src.constants import DEFAULT_FORMATTER, DEFAULT_LOG_FILE, DEFAULT_LOG_LEVEL


def get_console_handler(
    stream=sys.stdout,
    formatter: logging.Formatter = DEFAULT_FORMATTER,
) -> logging.StreamHandler:
    """Returns Handler that prints to stdout."""
    console_handler = logging.StreamHandler(stream)
    console_handler.setFormatter(formatter)
    return console_handler


def get_timed_file_handler(
    log_file: os.PathLike = DEFAULT_LOG_FILE,
    formatter: logging.Formatter = DEFAULT_FORMATTER,
    when: str = "midnight",
    backup_count: int = 7,
    **timed_rotating_file_handler_kwargs
) -> TimedRotatingFileHandler:
    """Returns Handler that keeps logs for specified amount of time."""
    file_handler = TimedRotatingFileHandler(
        log_file,
        when=when,
        backupCount=backup_count,
        **timed_rotating_file_handler_kwargs
    )
    file_handler.setFormatter(formatter)
    return file_handler


def get_logger(
    logger_name: str,
    level: int = DEFAULT_LOG_LEVEL,
    propagate: bool = False,
    **handler_kwargs
) -> logging.Logger:
    """Returns logger with console and timed file handler."""

    logger = logging.getLogger(logger_name)

    # if logger already has handlers attached to it, skip the configuration
    if logger.hasHandlers():
        logger.debug("Logger %s already set up.", logger.name)
        return logger

    logger.setLevel(
        level
    )  # better to have too much log than not enough (so default is DEBUG)

    logger.addHandler(get_console_handler(**handler_kwargs))
    logger.addHandler(get_timed_file_handler(**handler_kwargs))

    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = propagate

    return logger
