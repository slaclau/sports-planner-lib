import numpy as np


def time(seconds):
    hours = int(np.floor(seconds / 3600))
    seconds = seconds - 3600 * hours
    mins = int(np.floor(seconds / 60))
    secs = int(seconds - 60 * mins)
    rtn = ""
    if hours > 0:
        rtn += f"{hours:d}h"
    if mins > 0:
        rtn += f"{mins:d}m"
    if secs > 0:
        rtn += f"{secs:d}s"

    return rtn
