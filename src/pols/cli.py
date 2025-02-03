import argh

from . import ls


def cli():
    argh.dispatch_command(ls)
