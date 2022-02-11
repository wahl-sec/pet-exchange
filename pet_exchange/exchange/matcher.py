#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union, Generator, Tuple
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from datetime import datetime
from pathlib import Path
from copy import deepcopy
import logging
import json

import pet_exchange.proto.exchange_pb2 as grpc_buffer

from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.common.types import OrderType

logger = logging.getLogger("__main__")


class ExchangeMatcher:
    __name__ = "Exchange-Matcher"

    def __init__(self, output: str):
        self.book: Dict[str, Union[EncryptedOrderBook, OrderBook]] = {}
        self.keys: Dict[str, bytes] = {}
        self.output = output

        # Cache storing the instrument to length of book to see if something has updated
        self._cache_bid: Dict[str, int] = {}
        self._cache_ask: Dict[str, int] = {}
        self._orders = {}

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

    def _match_encrypted(self, instrument: str, book: EncryptedOrderBook) -> NoReturn:
        pass

    def _match(self, instrument: str, book: OrderBook) -> NoReturn:
        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        # TODO: Se över om matchningen fungerar, lite skumma resultat i test.json typ 29.0 inte genomförs tidigare
        try:
            b_identifier, b_order = next(bid_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more bid orders ...")
            return book

        try:
            a_identifier, a_order = next(ask_queue)
        except StopIteration:
            logger.debug(f"Exchange-Matcher ({instrument}): No more ask orders ...")
            return book

        while True:
            while b_order.volume == 0:
                try:
                    b_identifier, b_order = next(bid_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more bid orders ..."
                    )
                    return book

            while a_order.volume == 0:
                try:
                    a_identifier, a_order = next(ask_queue)
                except StopIteration:
                    logger.debug(
                        f"Exchange-Matcher ({instrument}): No more ask orders ..."
                    )
                    return book

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
                else:
                    b_order.volume = b_order.volume - min_volume

                if a_order.volume == min_volume:
                    a_order.volume = 0
                else:
                    a_order.volume = a_order.volume - min_volume

                logger.warn(
                    f"Exchange-Matcher ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume}) -> V ({b_order.volume}), 'ASK' ({a_identifier}) V ({a_order_c.volume}) -> V ({a_order.volume}) for P ({a_order.price})"
                )

                book.add_performed(
                    ask_identifier=a_identifier,
                    bid_identifier=b_identifier,
                    performed_price=a_order.price,
                    performed_volume=min_volume,
                    performed_time=datetime.now().strftime("%D - %H:%M:%S.%f"),
                    bid_order=b_order_c,
                    ask_order=a_order_c,
                )

        return book

    def _match_orders(
        self,
        instrument: str,
        book: Union[EncryptedOrderBook, OrderBook],
        encrypted: bool = True,
    ) -> NoReturn:
        """Match one iteration of orders agaist each other"""
        # TODO: We should create some sort of safe state to operate on the book since orders can theoretically be added during matching
        book.sort(type=OrderType.BID, encrypted=encrypted)
        book.sort(type=OrderType.ASK, encrypted=encrypted)

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
                        self.book[instrument].merge(future.result())
                        if self.output is not None:
                            self._write_output(instrument)

                except Exception as e:
                    logger.error(
                        f"Exchange-Matcher (Global): Encountered error during matching: {e}"
                    )
                    import traceback

                    print(traceback.format_exc())
                    raise e from None
