#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Union, List, Dict
import logging

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import OrderType, ExchangeOrderType
from pet_exchange.book.book import OrderBook
from pet_exchange.utils.logging import route_logger

logger = logging.getLogger("__main__")

# TODO: Parameter validation for the incoming requests


class EncryptedOrderBook(OrderBook):
    __name__ = "Encrypted-Order-Book"

    def __init__(self, instrument: str, exchange_order_type: ExchangeOrderType):
        self.__name__ = f"Encrypted-Order-Book-{instrument}-{exchange_order_type}"

        super().__init__(instrument=instrument, exchange_order_type=exchange_order_type)

    def _sort_bid_encrypted(self):
        """Default sort of ciphertext bid orders."""
        pass

    def _sort_ask_encrypted(self):
        """Default sort of ciphertext ask orders."""
        pass
