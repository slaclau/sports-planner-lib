import numpy as np


def time(seconds, fractional=False):
    hours = int(np.floor(seconds / 3600))
    seconds = seconds - 3600 * hours
    mins = int(np.floor(seconds / 60))
    secs = int(seconds - 60 * mins)
    rtn = ""
    if hours > 0:
        rtn += f"{hours:d}:"
    if mins > 0 or (hours > 0 and mins >= 0):
        rtn += f"{mins:0>2d}:"
    if secs >= 0:
        rtn += f"{secs:0>2d}"
    if fractional:
        pass
    return rtn
