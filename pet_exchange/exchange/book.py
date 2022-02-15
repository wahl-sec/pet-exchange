#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Union, List, Dict, Any, Callable
import functools
import logging

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import OrderType, ExchangeOrderType
from pet_exchange.common.crypto import (
    encrypt_sub_ciphertext_int,
    encrypt_sub_ciphertext_float,
    encrypt_add_plain_int,
    encrypt_add_plain_float,
)
from pet_exchange.book.book import OrderBook
from pet_exchange.common.utils import generate_identifier
from pet_exchange.utils.logging import route_logger

logger = logging.getLogger("__main__")

# TODO: Parameter validation for the incoming requests


class EncryptedOrderBook(OrderBook):
    __name__ = "Encrypted-Order-Book"

    def __init__(self, instrument: str, exchange_order_type: ExchangeOrderType):
        self._book_bid: Dict[
            str,
            Union[
                grpc_buffer.CiphertextLimitOrder,
                grpc_buffer.CiphertextMarketOrder,
            ],
        ] = {}
        self._book_ask: Dict[
            str,
            Union[
                grpc_buffer.CiphertextLimitOrder,
                grpc_buffer.CiphertextMarketOrder,
            ],
        ] = {}
        self._book_performed: Dict[str, Dict[str, Any]] = {}
        self._exchange_order_type = exchange_order_type

        self.__name__ = f"Encrypted-Order-Book-{instrument}-{exchange_order_type}"
        super().__init__(instrument=instrument, exchange_order_type=exchange_order_type)

    def _sort_bid_encrypted(self, func: Callable):
        """Default sort of ciphertext bid orders."""
        self._book_bid = {
            k: v
            for k, v in sorted(
                self._book_bid.items(),
                key=functools.cmp_to_key(func),
                reverse=True,
            )
        }

    def _sort_ask_encrypted(self, func: Callable):
        """Default sort of ciphertext ask orders."""
        pass

    def sort(self, type: OrderType, func: Callable):
        """Inlines sort the encrypted order book.
        Uses a custom sorting function that communicates with the intermediate.

        Since the orders are encrypted then an Intermediate component holding the secret key must
        be involved to sort.
        """
        getattr(self, f"_sort_{type.name.lower()}_encrypted")(func)

    def add_performed(
        self,
        ask_identifier: str,
        bid_identifier: str,
        performed_price: bytes,
        performed_volume: bytes,
        performed_time: str,
        bid_order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
        ],
        ask_order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
        ],
    ):
        """Mark an order as completed by adding it to the performed orders book."""
        _identifier = generate_identifier()
        while _identifier in self._book_performed:
            _identifier = generate_identifier()

        self._book_performed[_identifier] = {
            "instrument": bid_order.instrument,
            "references": {"ask": ask_identifier, "bid": bid_identifier},
            "bid": {
                "price": bid_order.price.hex()[:100],
                "volume": bid_order.volume.hex()[:100],
            },
            "ask": {
                "price": ask_order.price.hex()[:100],
                "volume": ask_order.volume.hex()[:100],
            },
            "performed": {
                "price": performed_price,
                "volume": performed_volume,
                "at": performed_time,
            },
            "remains": {
                "bid": None,
                "ask": None,
            },  # TODO: Must pass pyfhel if we want this
        }
