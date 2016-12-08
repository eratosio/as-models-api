
import logging

DEBUG = 'DEBUG'
INFO = 'INFO'
WARNING = 'WARNING'
ERROR = 'ERROR'
CRITICAL = 'CRITICAL'

LEVELS = (DEBUG, INFO, WARNING, ERROR, CRITICAL)

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
