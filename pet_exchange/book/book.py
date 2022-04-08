#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Union, List, Dict, Generator, Any, Optional, Callable
import functools
import logging

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import OrderType, ExchangeOrderType
from pet_exchange.common.utils import generate_identifier
from pet_exchange.utils.logging import route_logger

logger = logging.getLogger("__main__")

# TODO: Parameter validation for the incoming requests


class OrderBook:
    __name__ = "Order-Book"

    def __init__(self, instrument: str, exchange_order_type: ExchangeOrderType):
        self._book_bid: Dict[
            str,
            Union[
                grpc_buffer.PlaintextLimitOrder,
                grpc_buffer.PlaintextMarketOrder,
            ],
        ] = {}
        self._book_ask: Dict[
            str,
            Union[
                grpc_buffer.PlaintextLimitOrder,
                grpc_buffer.PlaintextMarketOrder,
            ],
        ] = {}
        self._book_performed: Dict[str, Dict[str, Any]] = {}
        self._book_metrics: Dict[str, Dict[str, Any]] = {}
        self._exchange_order_type = exchange_order_type
        self._book_bid_compared: Dict[Tuple[str, str], bool] = {}
        self._book_ask_compared: Dict[Tuple[str, str], bool] = {}
        self._instrument = instrument
        self.sorted = True
        self._locked_bid = False
        self._locked_ask = False

        self.__name__ = f"Order-Book-{instrument}-{exchange_order_type}"

    @property
    def bid_count(self) -> int:
        """Return the number of bid orders placed in the current order book"""
        return len(self._book_bid)

    @property
    def ask_count(self) -> int:
        """Return the number of ask orders placed in the current order book"""
        return len(self._book_ask)

    def queue(
        self, type: OrderType
    ) -> Generator[
        Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
        None,
        None,
    ]:
        """Return the orders as a tuple of the (identifier, order) for the book of a certain order type"""
        return (
            (identifier, order)
            for identifier, order in getattr(self, f"_book_{type.name.lower()}").items()
        )

    def _add_bid(
        self,
        identifier: str,
        order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
        matcher: Any
    ):
        # bisect https://github.com/python/cpython/blob/3.10/Lib/bisect.py
        while self._locked_bid:
            pass

        self._locked_bid = True
        lo = 0
        hi = len(self._book_bid)
        while lo < hi:
            mid = (lo + hi) // 2
            try:
                _identifier, _order = list(self._book_bid.items())[mid]
            except IndexError as e:
                print("BID", mid, lo, hi, len(list(self._book_bid.items())))
                raise e
            result = matcher.compare_fn(first=(_identifier, _order), second=(identifier, order), instrument=self._instrument, cache=self._book_bid_compared, correct_counter=[], total_counter=[], total_timings=[])

            if result == -1:
                hi = mid
            else:
                lo = mid + 1

        __book = list(self._book_bid.items())
        __book.insert(lo, (identifier, order))
        self._book_bid = dict(__book)
        self._locked_bid = False

        # self._book_bid[identifier] = order

    def _add_ask(
        self,
        identifier: str,
        order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
        matcher: Any
    ):
        while self._locked_ask:
            pass

        self._locked_ask = True
        # bisect https://github.com/python/cpython/blob/3.10/Lib/bisect.py
        lo = 0
        hi = len(self._book_ask)
        while lo < hi:
            try:
                mid = (lo + hi) // 2
            except IndexError as e:
                print("ASK", mid, lo, hi, len(list(self._book_ask.items())))
                raise e
            _identifier, _order = list(self._book_ask.items())[mid]
            result = matcher.compare_fn(first=(identifier, order), second=(_identifier, _order), instrument=self._instrument, cache=self._book_bid_compared, correct_counter=[], total_counter=[], total_timings=[])

            if result == -1:
                hi = mid
            else:
                lo = mid + 1

        __book = list(self._book_ask.items())
        __book.insert(lo, (identifier, order))
        self._book_ask = dict(__book)

        self._locked_ask = False
        # self._book_ask[identifier] = order

    def _sort_bid(self, func: Callable):
        """Default sort of plaintext bid orders."""
        self._book_bid = {
            k: v
            for k, v in sorted(
                self._book_bid.items(), key=functools.cmp_to_key(func), reverse=True
            )
        }

    def _sort_ask(self, func: Callable):
        """Default sort of plaintext ask orders."""
        self._book_ask = {
            k: v
            for k, v in sorted(
                self._book_ask.items(), key=functools.cmp_to_key(func), reverse=False
            )
        }

    def add(
        self,
        order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
        matcher: Any
    ):
        """Add an order to the order book, the order is sorted into the book of the given type."""
        _identifier = generate_identifier()
        _book = getattr(self, f"_book_{OrderType(order.type).name.lower()}")
        while _identifier in _book:
            _identifier = generate_identifier()

        getattr(self, f"_add_{OrderType(order.type).name.lower()}")(
            order=order, identifier=_identifier, matcher=matcher
        )
        # if self._exchange_order_type == ExchangeOrderType.LIMIT:
        #     self.sort(
        #         type=OrderType(order.type),
        #     )

        return _identifier

    def add_metrics(self, category: str, value: Dict[str, Any], section: str = None):
        """Add metrics to the book, this is purely used for evaluation and does not provide any functionality to the trading."""
        if category not in self._book_metrics:
            self._book_metrics[category] = {}

        if section is not None and section not in self._book_metrics[category]:
            self._book_metrics[category][section] = []

        self._book_metrics[category][section].append(value)

    def sort(self, type: OrderType, func: Callable):
        """Inlines sort the order book.
        Uses a custom sorting function passed by parameter.
        """
        getattr(self, f"_sort_{type.name.lower()}")(func)

    def add_performed(
        self,
        ask_identifier: str,
        bid_identifier: str,
        ask_entity: Optional[str],
        bid_entity: Optional[str],
        performed_price: int,
        performed_volume: int,
        performed_time: str,
        bid_order: Union[
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
        ask_order: Union[
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
    ):
        """Mark an order as completed by adding it to the performed orders book."""
        _identifier = generate_identifier()
        while _identifier in self._book_performed:
            _identifier = generate_identifier()

        self._book_performed[_identifier] = {
            "instrument": bid_order.instrument,
            "references": {"ask": ask_identifier, "bid": bid_identifier},
            "bid": {"price": bid_order.price, "volume": bid_order.volume},
            "ask": {"price": ask_order.price, "volume": ask_order.volume},
            "performed": {
                "price": performed_price,
                "volume": performed_volume,
                "at": performed_time,
            },
            "entity": {"bid": bid_entity, "ask": ask_entity},
            "remains": {
                "bid": bid_order.volume - performed_volume,
                "ask": ask_order.volume - performed_volume,
            },
        }

    def merge(
        self,
        other: Union["OrderBook", "EncryptedOrderBook"],
        b_dropped: List[str],
        a_dropped: List[str],
    ):
        """Merges the other order books information with the current one in order to update exisiting orders with new orders.

        This is done to avoid re-execution of already executed orders.
        """
        while self._locked_ask:
            pass

        self._locked_ask = True
        self._book_ask.update({key: value for key, value in other._book_ask.items()})
        self._book_ask = {
            key: value for key, value in self._book_ask.items() if key not in a_dropped
        }

        while self._locked_bid:
            pass

        self._locked_bid = True
        self._book_bid.update({key: value for key, value in other._book_bid.items()})
        self._book_bid = {
            key: value for key, value in self._book_bid.items() if key not in b_dropped
        }
        self._book_performed.update(other._book_performed)
        self._book_bid_compared.update(other._book_bid_compared)
        self._book_ask_compared.update(other._book_ask_compared)
        self.sorted = other.sorted
        self._locked_bid = False
        self._locked_ask = False
