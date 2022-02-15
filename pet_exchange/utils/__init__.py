#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import path

ROOT_PATH = path.join(path.dirname(__file__), "..", "..")

LOGGER_CONFIG = {
    "version": 1,
    "formatters": {
        "INFO": {"format": "%(asctime)s :: %(levelname)s :: %(message)s"},
        "DEBUG": {
            "format": "[%(processName)s : %(threadName)s] :: %(asctime)s :: %(levelname)s :: %(filename)s :: %(message)s"
        },
    },
    "handlers": {
        "INFO": {
            "level": "INFO",
            "formatter": "INFO",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "DEBUG": {
            "level": "DEBUG",
            "formatter": "DEBUG",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "__main__": {"handlers": ["INFO"], "level": "INFO", "propagate": False}
    },
}
