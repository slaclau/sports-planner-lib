import os


def getint(name):
    return int(os.path.splitext(name)[0])
