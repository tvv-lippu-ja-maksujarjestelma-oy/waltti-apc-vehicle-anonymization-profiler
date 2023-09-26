import logging
import os

import google.cloud.logging_v2.handlers


def map_log_level(log_level_string):
    lowered = log_level_string.lower()
    if lowered == "silent":
        return logging.NOTSET
    if lowered == "trace":
        return logging.DEBUG
    if lowered == "debug":
        return logging.DEBUG
    if lowered == "info":
        return logging.INFO
    if lowered == "warn":
        return logging.WARNING
    if lowered == "error":
        return logging.ERROR
    if lowered == "fatal":
        return logging.CRITICAL
    msg = (
        'PINO_LOG_LEVEL must be set to either "silent", "trace", "debug",'
        ' "info", "warn", "error" or "fatal". Instead,'
        f' "{log_level_string}" was given.'
    )
    raise ValueError(msg)


def get_log_level():
    log_level = logging.INFO
    # As config has not been created yet, read LOG_LEVEL directly from the
    # environment.
    log_level_string = os.getenv("PINO_LOG_LEVEL")
    if log_level_string is not None:
        log_level = map_log_level(log_level_string)
    return log_level


def create_logger(name):
    google.cloud.logging_v2.handlers.setup_logging(
        google.cloud.logging_v2.handlers.StructuredLogHandler()
    )
    logger = logging.getLogger(name)
    logger.setLevel(get_log_level())
    return logger
