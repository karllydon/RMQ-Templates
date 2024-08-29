#!/usr/bin/env python3
""" module for consistent logging across publisher/consumers
"""
import logging

LOG_FORMAT = "%(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d:%(levelname)s %(message)s"


class LoggerClass:
    """logger class to supply an info and error logger"""

    def __init__(self):
        self.setup_logger("info_logger", "system.log", logging.DEBUG)
        self.setup_logger("error_logger", "exception.log", logging.ERROR)
        self.info_logger = logging.getLogger("info_logger")
        self.error_logger = logging.getLogger("error_logger")

    def setup_logger(self, logger_name, log_file, level):
        """setup a specific logger
        :param string logger_name: name of logger
        :param string log_file: file to log to
        :param int | string level: level to log
        """
        l = logging.getLogger(logger_name)
        formatter = logging.Formatter(LOG_FORMAT)
        file_handler = logging.FileHandler(log_file, mode="a")
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        l.setLevel(level)
        l.addHandler(file_handler)
        l.addHandler(stream_handler)

    def debug(self, msg):
        """send debug messages to info logger
        :param: string msg: log msg
        """
        self.info_logger.debug(msg)

    def info(self, msg):
        """send info messages to info logger
        :param: string msg: log msg
        """
        self.info_logger.info(msg)

    def warning(self, msg):
        """send warning messages to error logger
        :param: string msg: log msg
        """
        self.error_logger.warning(msg)

    def error(self, msg):
        """send error messages to error logger
        :param: string msg: log msg
        """
        self.error_logger.error(msg)

    def critical(self, msg):
        """send critical messages to critical logger
        :param: string msg: log msg
        """
        self.error_logger.critical(msg)
