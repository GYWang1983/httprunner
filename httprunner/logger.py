# encoding: utf-8

import logging
import sys

from colorama import Fore, init
from colorlog import ColoredFormatter

init(autoreset=True)

log_colors_config = {
    'DEBUG':    'cyan',
    'INFO':     'green',
    'WARNING':  'yellow',
    'ERROR':    'red',
    'CRITICAL': 'red',
}
logger = logging.getLogger("httprunner")
is_file = False

def setup_logger(log_level, log_file=None):
    """setup logger with ColoredFormatter."""
    global is_file

    level = getattr(logging, log_level.upper(), None)
    if not level:
        color_print("Invalid log level: %s" % log_level, "RED")
        sys.exit(1)

    # hide traceback when log level is INFO/WARNING/ERROR/CRITICAL
    if level >= logging.INFO:
        sys.tracebacklimit = 0

    if log_file:
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setFormatter(logging.Formatter(u"%(levelname)-8s %(message)s"))
        is_file = True
    else:
        handler = logging.StreamHandler()
        formatter = ColoredFormatter(
            u"%(log_color)s%(bg_white)s%(levelname)-8s%(reset)s %(message)s",
            datefmt=None,
            reset=True,
            log_colors=log_colors_config
        )
        handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(level)


def coloring(text, color="WHITE"):
    fore_color = getattr(Fore, color.upper())
    return fore_color + text


def color_print(msg, color="WHITE"):
    fore_color = getattr(Fore, color.upper())
    print(fore_color + msg)


def log_with_color(level):
    """ log with color by different level
    """
    global is_file

    def wrapper(text):
        if not is_file:
            color = log_colors_config[level.upper()]
            getattr(logger, level.lower())(coloring(text, color))
        else:
            getattr(logger, level.lower())(text)

    return wrapper


log_debug = log_with_color("debug")
log_info = log_with_color("info")
log_warning = log_with_color("warning")
log_error = log_with_color("error")
log_critical = log_with_color("critical")
