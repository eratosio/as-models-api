
from .. import log_levels
import os
import select
import subprocess


def execute(command, updater, log_prefix='', **kwargs):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True,
                               **kwargs)

    exit_code = None
    while True:
        for line in process.stdout:
            updater.log(log_prefix + line, level=log_levels.STDOUT)
        for line in process.stderr:
            updater.log(log_prefix, line, level=log_levels.STDERR)

        if exit_code is not None:
            return exit_code

        exit_code = process.poll()
