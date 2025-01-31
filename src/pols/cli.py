import argh

from .pols import pols


def cli():
    argh.dispatch_command(pols)
