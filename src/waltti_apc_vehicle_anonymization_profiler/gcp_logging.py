import logging
import os

import google.cloud.logging_v2.handlers


def map_log_level(log_level_string):
    lowered = log_level_string.lower()
    if lowered == "silent":
        return logging.NOTSET
    if lowered == "trace":
        return logging.DEBUG
    elif lowered == "debug":
        return logging.DEBUG
    elif lowered == "info":
        return logging.INFO
    elif lowered == "warn":
        return logging.WARNING
    elif lowered == "error":
        return logging.ERROR
    elif lowered == "fatal":
        return logging.CRITICAL
    else:
        raise ValueError(
            'PINO_LOG_LEVEL must be set to either "silent", "trace", "debug",'
            ' "info", "warn", "error" or "fatal". Instead,'
            f' "{log_level_string}" was given.'
        )


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
