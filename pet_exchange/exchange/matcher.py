#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union, Generator, Tuple, List
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from datetime import datetime
from pathlib import Path
from copy import deepcopy
import logging
import json

from Pyfhel import Pyfhel

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.common.utils import generate_random_int
from pet_exchange.common.types import OrderType
from pet_exchange.common.crypto import (
    encrypt_int,
    encrypt_frac,
    encrypt_add_plain_int,
    encrypt_add_plain_float,
    encrypt_add_ciphertext_int,
    encrypt_add_ciphertext_float,
    encrypt_sub_plain_int,
    encrypt_sub_plain_float,
    encrypt_sub_ciphertext_int,
    encrypt_sub_ciphertext_float,
)

logger = logging.getLogger("__main__")


class ExchangeMatcher:
    __name__ = "Exchange-Matcher"

    def __init__(self, output: str):
        self.book: Dict[str, Union[EncryptedOrderBook, OrderBook]] = {}
        self.keys: Dict[str, bytes] = {}
        self.pyfhel: Dict[str, Pyfhel] = {}
        self.output = output

        # Cache storing the instrument to length of book to see if something has updated
        self._cache_bid: Dict[str, int] = {}
        self._cache_ask: Dict[str, int] = {}
        self._orders = {}
        self._intermediate_channel = None

    def _write_output(self, instrument: str) -> NoReturn:
        """Writes the output of the current book for a certain instrument to a JSON file"""
        _path = Path(self.output)
        _book = {}

        if _path.exists():
            with _path.open(mode="r+") as _file:
                _book = json.load(_file)
                if instrument not in _book:
                    _book[instrument] = {}

                _book[instrument].update(self.book[instrument]._book_performed)

        with _path.open(mode="w+") as _file:
            _file.write(json.dumps(_book))

    def _match_encrypted(
        self, instrument: str, book: EncryptedOrderBook
    ) -> Tuple[EncryptedOrderBook, List[str], List[str]]:
        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        b_dropped, a_dropped = [], []

        try:
            b_identifier, b_order = next(bid_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more bid orders ...")
            return book, b_dropped, a_dropped

        try:
            a_identifier, a_order = next(ask_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more ask orders ...")
            return book, b_dropped, a_dropped

        while True:
            if b_identifier in b_dropped:
                try:
                    b_identifier, b_order = next(bid_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more bid orders ..."
                    )
                    return book, b_dropped, a_dropped

            if a_identifier in a_dropped:
                try:
                    a_identifier, a_order = next(ask_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more ask orders ..."
                    )
                    return book, b_dropped, a_dropped

            _price_pad = generate_random_int()
            b_otp_price, a_otp_price = encrypt_add_plain_float(
                b_order.price, _price_pad, pyfhel=self.pyfhel[instrument]
            ), encrypt_add_plain_float(
                a_order.price, _price_pad, pyfhel=self.pyfhel[instrument]
            )

            _minimum_otp_price = self._intermediate_channel.GetMinimumValue(
                first=b_otp_price,
                second=a_otp_price,
                instrument=instrument,
                encoding="float",
            ).minimum

            if b_otp_price == _minimum_otp_price and a_otp_price != _minimum_otp_price:
                logger.info(
                    f"Exchange-Matcher ({instrument}): No more matches possible ..."
                )
                break
            else:
                _volume_pad = generate_random_int()
                b_otp_volume, a_otp_volume = encrypt_add_plain_int(
                    b_order.volume, _volume_pad, pyfhel=self.pyfhel[instrument]
                ), encrypt_add_plain_int(
                    a_order.volume, _volume_pad, pyfhel=self.pyfhel[instrument]
                )

                _minimum_otp_volume = self._intermediate_channel.GetMinimumValue(
                    first=b_otp_volume,
                    second=a_otp_volume,
                    instrument=instrument,
                    encoding="int",
                ).minimum

                _minimum_volume = encrypt_sub_plain_int(
                    _minimum_otp_volume, _volume_pad, pyfhel=self.pyfhel[instrument]
                )

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_order.volume == _minimum_volume:
                    b_order.volume = encrypt_int(0, pyfhel=self.pyfhel[instrument])
                    b_dropped.append(b_identifier)
                else:
                    b_order.volume = encrypt_sub_ciphertext_int(
                        b_order.volume, _minimum_volume, pyfhel=self.pyfhel[instrument]
                    )

                if a_order.volume == _minimum_volume:
                    a_order.volume = encrypt_int(0, pyfhel=self.pyfhel[instrument])
                    a_dropped.append(a_identifier)
                else:
                    a_order.volume = encrypt_sub_ciphertext_int(
                        a_order.volume, _minimum_volume, pyfhel=self.pyfhel[instrument]
                    )

                logger.warn(
                    f"Exchange-Matcher ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume.hex()[:20]}) -> V ({b_order.volume.hex()[:20]}), 'ASK' ({a_identifier}) V ({a_order_c.volume.hex()[:20]}) -> V ({a_order.volume.hex()[:20]}) for P ({a_order.price.hex()[:20]})"
                )

                d_order = self._intermediate_channel.DecryptOrder(
                    grpc_buffer_intermediate.CiphertextOrder(
                        type=a_order.type,
                        instrument=instrument,
                        volume=_minimum_volume,
                        price=a_order.price,
                    )
                )

                book.add_performed(
                    ask_identifier=a_identifier,
                    bid_identifier=b_identifier,
                    performed_price=d_order.order.price,
                    performed_volume=d_order.order.volume,
                    performed_time=datetime.now().strftime("%d/%m/%y %H:%M:%S.%f"),
                    bid_order=b_order_c,
                    ask_order=a_order_c,
                )

        return book, b_dropped, a_dropped

    def _match(
        self, instrument: str, book: OrderBook
    ) -> Tuple[OrderBook, List[str], List[str]]:
        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        b_dropped, a_dropped = [], []

        try:
            b_identifier, b_order = next(bid_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more bid orders ...")
            return book, b_dropped, a_dropped

        try:
            a_identifier, a_order = next(ask_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more ask orders ...")
            return book, b_dropped, a_dropped

        while True:
            if b_identifier in b_dropped:
                try:
                    b_identifier, b_order = next(bid_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more bid orders ..."
                    )
                    return book, b_dropped, a_dropped

            if a_identifier in a_dropped:
                try:
                    a_identifier, a_order = next(ask_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more ask orders ..."
                    )
                    return book, b_dropped, a_dropped

            if b_order.price < a_order.price:
                logger.info(
                    f"Exchange-Matcher ({instrument}): No more matches possible ..."
                )
                break
            else:
                min_volume = min(b_order.volume, a_order.volume)

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_order.volume == min_volume:
                    b_order.volume = 0
                    b_dropped.append(b_identifier)
                else:
                    b_order.volume = b_order.volume - min_volume

                if a_order.volume == min_volume:
                    a_order.volume = 0
                    a_dropped.append(a_identifier)
                else:
                    a_order.volume = a_order.volume - min_volume

                logger.warn(
                    f"Exchange-Matcher ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume}) -> V ({b_order.volume}), 'ASK' ({a_identifier}) V ({a_order_c.volume}) -> V ({a_order.volume}) for P ({a_order.price})"
                )

                # TODO: Publish in a cleartext form since it has been executed
                book.add_performed(
                    ask_identifier=a_identifier,
                    bid_identifier=b_identifier,
                    performed_price=a_order.price,
                    performed_volume=min_volume,
                    performed_time=datetime.now().strftime("%d/%m/%y %H:%M:%S.%f"),
                    bid_order=b_order_c,
                    ask_order=a_order_c,
                )

        return book, b_dropped, a_dropped

    def _match_orders(
        self,
        instrument: str,
        book: Union[EncryptedOrderBook, OrderBook],
        encrypted: bool = True,
    ) -> Tuple[Union[EncryptedOrderBook, OrderBook], List[str], List[str]]:
        """Match one iteration of orders agaist each other"""
        # TODO: We should create some sort of safe state to operate on the book since orders can theoretically be added during matching
        if isinstance(book, EncryptedOrderBook):

            def _remote_sort(
                first: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                second: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                reverse: bool,
            ) -> Union[
                grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
            ]:
                """Compare two encrypted item and return the smallest using the remote intermediate."""
                _, first = first
                _, second = second
                return (
                    1
                    if (
                        self._intermediate_channel.GetMinimumValue(
                            first=first.price,
                            second=second.price,
                            instrument=instrument,
                            encoding="float",
                        ).minimum
                        == first
                    )
                    else -1
                )

            book.sort(
                type=OrderType.BID,
                func=lambda first, second: _remote_sort(first, second, reverse=False),
            )
            book.sort(
                type=OrderType.ASK,
                func=lambda first, second: _remote_sort(first, second, reverse=True),
            )
        else:
            book.sort(type=OrderType.BID)
            book.sort(type=OrderType.ASK)

        return getattr(self, f"_match{'_encrypted' if encrypted else ''}")(
            instrument=instrument, book=book
        )

    def match(self, encrypted: bool) -> NoReturn:
        """Continously match incoming orders against each other
        Runs the sub matchers _match and _match_plaintext depending on if the orders are encrypted
        """
        # TODO: Number of threads should be based on some estimation of instruments being traded at the same time
        with ThreadPoolExecutor(max_workers=10) as pool:
            while True:
                try:
                    future_to_match: Dict[Future, str] = {}
                    for instrument, book in self.book.items():
                        bids = self._cache_bid.setdefault(instrument, 0)
                        asks = self._cache_ask.setdefault(instrument, 0)
                        if bids != book.bid_count or asks != book.ask_count:
                            self._cache_bid[instrument] = book.bid_count
                            self._cache_ask[instrument] = book.ask_count

                            future_to_match[
                                pool.submit(
                                    self._match_orders,
                                    instrument=instrument,
                                    book=deepcopy(book),
                                    encrypted=encrypted,
                                )
                            ] = instrument

                    for future in as_completed(future_to_match):
                        instrument = future_to_match[future]
                        # TODO: Merge books to update with results from matching
                        self.book[instrument].merge(*future.result())
                        if self.output is not None:
                            self._write_output(instrument)

                except Exception as e:
                    logger.error(
                        f"Exchange-Matcher (Global): Encountered error during matching: {e}"
                    )
                    import traceback

                    print(traceback.format_exc())
                    raise e from None
