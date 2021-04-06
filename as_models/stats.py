
import os
import re

SIZE_MULTIPLIERS = {
    'b': 1,
    'kb': 1024,
    'mb': 1024 * 1024,
    'gb': 1024 * 1024 * 1024
}

MEMORY_HWM_STATUS_PATTERN = re.compile(r'VmHWM\s*:\s*([0-9]+)\s*(kb|mb|gb)?', re.I)


def get_peak_memory_usage(pid):
    # Obtain resident set size "high water mark" from /proc/<pid>/status.
    try:
        with open(os.path.join('/proc', str(pid), 'status'), 'r') as f:
            for line in f:
                m = MEMORY_HWM_STATUS_PATTERN.match(line)
                if m:
                    value, multiplier = m.groups('b')
                    return int(value) * SIZE_MULTIPLIERS[multiplier.lower()]
    except IOError:
        pass

    return 0
