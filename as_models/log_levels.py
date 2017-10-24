
import logging

DEBUG = 'DEBUG'
INFO = 'INFO'
WARNING = 'WARNING'
ERROR = 'ERROR'
CRITICAL = 'CRITICAL'

# NOTE: the following two log "levels" represent output captured from stdout and
# stderr respectively, and don't strictly speaking fall within the standard
# log level hierarchy.
STDOUT = 'STDOUT'
STDERR = 'STDERR'

LEVELS = (DEBUG, INFO, WARNING, ERROR, CRITICAL, STDOUT, STDERR)

def from_stdlib_levelno(level, default=None):
    return {
        logging.DEBUG: DEBUG,
        logging.INFO: INFO,
        logging.WARNING: WARNING,
        logging.ERROR: ERROR,
        logging.CRITICAL: CRITICAL
    }.get(level, default)

def to_stdlib_levelno(level, default=None):
    return {
        DEBUG: logging.DEBUG,
        INFO: logging.INFO,
        WARNING: logging.WARNING,
        ERROR: logging.ERROR,
        CRITICAL: logging.CRITICAL
    }.get(level, default)

def compare(level0, level1):
    return LEVELS.index(level0) - LEVELS.index(level1)
