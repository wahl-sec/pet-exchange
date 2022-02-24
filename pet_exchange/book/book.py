#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Union, List, Dict, Generator, Any, Optional
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
        self._exchange_order_type = exchange_order_type

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
    ):
        self._book_bid[identifier] = order

    def _add_ask(
        self,
        identifier: str,
        order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ],
    ):
        self._book_ask[identifier] = order

    def _sort_bid(self):
        """Default sort of plaintext bid orders."""
        self._book_bid = {
            k: v
            for k, v in sorted(
                self._book_bid.items(), key=lambda order: order[1].price, reverse=True
            )
        }

    def _sort_ask(self):
        """Default sort of plaintext ask orders."""
        self._book_ask = {
            k: v
            for k, v in sorted(
                self._book_ask.items(), key=lambda order: order[1].price, reverse=False
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
    ):
        """Add an order to the order book, the order is sorted into the book of the given type."""
        _identifier = generate_identifier()
        _book = getattr(self, f"_book_{OrderType(order.type).name.lower()}")
        while _identifier in _book:
            _identifier = generate_identifier()

        getattr(self, f"_add_{OrderType(order.type).name.lower()}")(
            order=order, identifier=_identifier
        )
        # if self._exchange_order_type == ExchangeOrderType.LIMIT:
        #     self.sort(
        #         type=OrderType(order.type),
        #     )

        return _identifier

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

    def sort(self, type: OrderType):
        """Inlines sort the unencrypted order book directly."""
        getattr(self, f"_sort_{type.name.lower()}")()

    def merge(
        self,
        other: Union["OrderBook", "EncryptedOrderBook"],
        b_dropped: List[str],
        a_dropped: List[str],
    ):
        """Merges the other order books information with the current one in order to update exisiting orders with new orders.

        This is done to avoid re-execution of already executed orders.
        """
        self._book_ask.update({key: value for key, value in other._book_ask.items()})
        self._book_ask = {
            key: value for key, value in self._book_ask.items() if key not in a_dropped
        }
        self._book_bid.update({key: value for key, value in other._book_bid.items()})
        self._book_bid = {
            key: value for key, value in self._book_bid.items() if key not in b_dropped
        }
        self._book_performed.update(other._book_performed)
