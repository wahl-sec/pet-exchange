#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import path
from typing import Dict, Any, Optional
from datetime import datetime
from functools import wraps
import multiprocessing
import logging
import logging.config

import grpc

from utils import ROOT_PATH


logger = logging.getLogger("__main__")


class CustomFormatter(logging.Formatter):
    BLUE = "\x1b[36;20m"
    GRAY = "\x1b[38;20m"
    PURPLE = "\x1b[35;20m"
    YELLOW = "\x1b[33;20m"
    RED = "\x1b[31;20m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    def __init__(self, _format: str):
        self.formats = {
            logging.DEBUG: self.PURPLE + _format + self.RESET,
            logging.INFO: self.BLUE + _format + self.RESET,
            logging.WARNING: self.YELLOW + _format + self.RESET,
            logging.ERROR: self.RED + _format + self.RESET,
            logging.CRITICAL: self.BOLD_RED + _format + self.RESET,
            logging.FATAL: self.BOLD_RED + _format + self.RESET,
        }

    def format(self, record):
        return logging.Formatter(self.formats.get(record.levelno)).format(record)


def proc_logger(logger_config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """Shared logger for all instantiated process components"""
    if logger_config is not None:
        logging.config.dictConfig(logger_config)

    root_logger = logging.getLogger("__main__")

    # file_handler = logging.FileHandler(
    #     path.join(
    #         ROOT_PATH,
    #         "logs",
    #         f"{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}_PET-Exchange.log",
    #     )
    # )
    # file_handler.setFormatter(formatter)
    return root_logger


def route_logger(reply_type):
    def wrapper(func):
        @wraps(func)
        async def _logger(_self, request=None, context=None, **kwargs):
            try:
                logger.info(
                    f"{_self.__name__} ({_self.listen_addr}): <{request.__class__.__name__}> Incoming request from: '{context.peer()}'"
                )
                return await func(_self, request=request, context=context, **kwargs)
            except Exception as exc:
                _exception = str(exc).strip('"')
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(_exception)

                getattr(
                    logger,
                    "exception" if logger.level == logging.DEBUG else "error",
                )(
                    f"{_self.__name__} ({_self.listen_addr}): <{reply_type.__name__}> {_exception}",
                    exc_info=exc if logger.level == logging.DEBUG else None,
                )

                return reply_type()

        return _logger

    return wrapper
