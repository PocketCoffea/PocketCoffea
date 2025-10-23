#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------
#                                                                               -
#  Python dual-logging setup (console and log file),                            -
#  supporting different log levels and colorized output                         -
#                                                                               -
#  Created by Fonic <https://github.com/fonic>                                  -
#  Date: 04/05/20                                                               -
#                                                                               -
#  Based on:                                                                    -
#  https://stackoverflow.com/a/13733863/1976617                                 -
#  https://uran198.github.io/en/python/2016/07/12/colorful-python-logging.html  -
#  https://en.wikipedia.org/wiki/ANSI_escape_code#Colors                        -
#                                                                               -
# -------------------------------------------------------------------------------

# Imports
import os
import sys
import logging
import traceback
from functools import wraps

# Logging formatter supporting colorized output
class LogFormatter(logging.Formatter):

    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m",  # bright/bold magenta
        logging.ERROR: "\033[1;31m",  # bright/bold red
        logging.WARNING: "\033[1;33m",  # bright/bold yellow
        logging.INFO: "\033[0;37m",  # white / light gray
        logging.DEBUG: "\033[1;30m",  # bright/bold black / dark gray
    }

    RESET_CODE = "\033[0m"

    def __init__(self, color, *args, **kwargs):
        super(LogFormatter, self).__init__(*args, **kwargs)
        self.color = color

    def format(self, record, *args, **kwargs):
        if self.color == True and record.levelno in self.COLOR_CODES:
            record.color_on = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on = ""
            record.color_off = ""
        return super(LogFormatter, self).format(record, *args, **kwargs)


# Setup logging
def setup_logging(
    console_log_output,
    console_log_level,
    console_log_color,
    logfile_file,
    logfile_log_level,
    logfile_log_color,
    log_line_template,
):

    # Create logger
    # For simplicity, we use the root logger, i.e. call 'logging.getLogger()'
    # without name argument. This way we can simply use module methods for
    # for logging throughout the script. An alternative would be exporting
    # the logger, i.e. 'global logger; logger = logging.getLogger("<name>")'
    logger = logging.getLogger()

    # Set global log level to 'debug' (required for handler levels to work)
    logger.setLevel(logging.DEBUG)

    # Create console handler
    console_log_output = console_log_output.lower()
    if console_log_output == "stdout":
        console_log_output = sys.stdout
    elif console_log_output == "stderr":
        console_log_output = sys.stderr
    else:
        print("Failed to set console output: invalid output: '%s'" % console_log_output)
        return False
    console_handler = logging.StreamHandler(console_log_output)

    # Set console log level
    try:
        console_handler.setLevel(
            console_log_level.upper()
        )  # only accepts uppercase level names
    except:
        print(
            "Failed to set console log level: invalid level: '%s'" % console_log_level
        )
        return False

    # Create and set formatter, add console handler to logger
    console_formatter = LogFormatter(fmt=log_line_template, color=console_log_color)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Create log file handler
    try:
        logfile_handler = logging.FileHandler(logfile_file)
    except Exception as exception:
        print("Failed to set up log file: %s" % str(exception))
        return False

    # Set log file log level
    try:
        logfile_handler.setLevel(
            logfile_log_level.upper()
        )  # only accepts uppercase level names
    except:
        print(
            "Failed to set log file log level: invalid level: '%s'" % logfile_log_level
        )
        return False

    # Create and set formatter, add log file handler to logger
    logfile_formatter = LogFormatter(fmt=log_line_template, color=logfile_log_color)
    logfile_handler.setFormatter(logfile_formatter)
    logger.addHandler(logfile_handler)

    # Success
    return True

def try_and_log_error(error_file, exit_on_error=False):
    """
    Decorator to catch exceptions and log them to a specified error file.
    
    Parameters
    ----------
    error_file : str
        Path to the error log file. The parent directory will be created if missing.
    exit_on_error : bool, optional
        If True, prints the full traceback and exits the program when an error occurs.
        If False, logs the error and continues execution. Default is False.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_trace = traceback.format_exc()
                # Ensure parent directory exists
                os.makedirs(os.path.dirname(error_file), exist_ok=True)
                # Write traceback to error file
                with open(error_file, "w") as f:
                    f.write(error_trace)
                if hasattr(func, '__name__'):
                    logging.error(f"Error occurred in '{func.__name__}', traceback saved to: {os.path.abspath(error_file)}")
                else:
                    logging.error(f"Error occurred, traceback saved to: {os.path.abspath(error_file)}")
                
                # Exit after logging the error traceback if specified
                if exit_on_error:
                    if hasattr(func, '__name__'):
                        logging.error(f"\nFatal error in '{func.__name__}':")
                    else:
                        logging.error(f"\nFatal error:")
                    print(error_trace)
                    sys.exit(1)
                return None  # prevent the exception from halting the main script if not exiting
        return wrapper
    return decorator
