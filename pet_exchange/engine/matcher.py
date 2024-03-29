#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union, Generator, Tuple, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from copy import deepcopy, copy
from datetime import datetime
from random import randint
from pathlib import Path
import logging
import json
import time
import zlib
import sys

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
from pet_exchange.proto.intermediate_pb2 import Challenge, ChallengePlain
from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.common.utils import (
    generate_random_int,
    generate_random_float,
    generate_challenges,
)
from pet_exchange.common.types import OrderType
from pet_exchange.common.crypto import CKKS
from pet_exchange.utils.logging import TRADE_LOG_LEVEL

logger = logging.getLogger("__main__")


class MatchingEngine:
    __name__ = "Matching-Engine"

    def __init__(
        self,
        output: str,
        instruments: List[str],
        encrypted: Optional[str],
        local_compare: bool,
        time_limit: Optional[int],
        compare_iterations: int,
        compare_constant_count: int,
        compare_sigmoid_iterations: int,
        challenge_count: int,
    ):
        self.book: Dict[str, Union[EncryptedOrderBook, OrderBook]] = {}
        self.keys: Dict[str, bytes] = {}
        self.relin_keys: Dict[str, bytes] = {}
        self.pyfhel: Dict[str, Pyfhel] = {}
        self.crypto: Dict[str, CKKS] = {}
        self.output = output
        self.time_limit = time_limit
        self.encrypted = encrypted
        if local_compare and encrypted is not None:
            self.compare_fn = self._compare_local_encrypted
        elif local_compare and encrypted is None:
            self.compare_fn = self._compare_local
        elif not local_compare and encrypted is not None:
            self.compare_fn = self._compare_remote_encrypted
        elif not local_compare and encrypted is None:
            self.compare_fn = self._compare_remote

        self.challenge_count = challenge_count
        if encrypted:
            self.compare_iterations = compare_iterations
            self.compare_constant_count = compare_constant_count
            self.compare_sigmoid_iterations = compare_sigmoid_iterations

        # Cache storing the instrument to length of book to see if something has updated
        self._cache_bid: Dict[str, int] = {}
        self._cache_ask: Dict[str, int] = {}
        self._intermediate_channel = None

    def _write_output(self, instrument: str) -> None:
        """Writes the output of the current book for a certain instrument to a JSON file"""
        _path = Path(self.output)
        _book = {}

        if _path.exists():
            with _path.open(mode="r+") as _file:
                _book = json.load(_file)
                if instrument not in _book:
                    _book[instrument] = {
                        "PERFORMED": {},
                        "METRICS": {},
                    }

                _book[instrument]["PERFORMED"].update(
                    self.book[instrument]._book_performed
                )

                _book[instrument]["METRICS"].update(self.book[instrument]._book_metrics)

        with _path.open(mode="w+") as _file:
            _file.write(json.dumps(_book))

    def _match_encrypted(
        self,
        instrument: str,
        book: EncryptedOrderBook,
        timings: Dict[str, List[float]],
    ) -> Tuple[EncryptedOrderBook, List[str], List[str]]:
        start_match_time = time.time()

        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        b_dropped, a_dropped = [], []
        crypto = self.crypto[instrument]

        ZERO_VOLUME = crypto.encrypt_float(0.0)

        try:
            b_identifier, b_order = next(bid_queue)
        except StopIteration:
            logger.debug(f"{self.__name__} ({instrument}): No more bid orders ...")
            return book, b_dropped, a_dropped

        try:
            a_identifier, a_order = next(ask_queue)
        except StopIteration:
            logger.debug(f"{self.__name__} ({instrument}): No more ask orders ...")
            return book, b_dropped, a_dropped

        while True:
            if b_identifier in b_dropped:
                try:
                    b_identifier, b_order = next(bid_queue)
                except StopIteration:
                    logger.debug(
                        f"{self.__name__} ({instrument}): No more bid orders ..."
                    )
                    return book, b_dropped, a_dropped

            if a_identifier in a_dropped:
                try:
                    a_identifier, a_order = next(ask_queue)
                except StopIteration:
                    logger.debug(
                        f"{self.__name__} ({instrument}): No more ask orders ..."
                    )
                    return book, b_dropped, a_dropped

            if hash(b_order.volume) not in crypto._depth_map:
                b_order_volume = crypto.from_bytes(ctxt=b_order.volume)
                crypto._depth_map[hash(b_order_volume)] = 1
            else:
                b_order_volume = b_order.volume

            if hash(b_order.price) not in crypto._depth_map:
                b_order_price = crypto.from_bytes(ctxt=b_order.price)
                crypto._depth_map[hash(b_order_price)] = 1
            else:
                b_order_price = b_order.price

            if hash(a_order.volume) not in crypto._depth_map:
                a_order_volume = crypto.from_bytes(ctxt=a_order.volume)
                crypto._depth_map[hash(a_order_volume)] = 1
            else:
                a_order_volume = a_order.volume

            if hash(a_order.price) not in crypto._depth_map:
                a_order_price = crypto.from_bytes(ctxt=a_order.price)
                crypto._depth_map[hash(a_order_price)] = 1
            else:
                a_order_price = a_order.price

            _price_pad = crypto.encode_float(values=[generate_random_float()])
            crypto._depth_map[hash(_price_pad)] = 1

            start_time_pad = time.time()
            b_otp_price = crypto.encrypt_add_plain_float(b_order_price, _price_pad)
            end_time_pad = time.time()
            timings["TIME_TO_PAD_ORDER_PRICE_BID"].append(end_time_pad - start_time_pad)

            start_time = time.time()
            a_otp_price = crypto.encrypt_add_plain_float(
                a_order_price,
                _price_pad,
            )
            end_time = time.time()
            timings["TIME_TO_PAD_ORDER_PRICE_ASK"].append(end_time - start_time)

            """
            start_time_gen = time.time()
            expected, challenges = generate_challenges(
                engine=crypto, n=self.challenge_count
            )
            end_time_gen = time.time()
            timings["TIME_TO_GENERATE_CHALLENGES"].append(end_time_gen - start_time_gen)
            index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
            _challenges: List[Challenge] = (
                challenges[:index]
                + [
                    Challenge(
                        first=b_otp_price,
                        second=a_otp_price,
                    )
                ]
                + challenges[index:]
            )
            timings["SIZE_OF_CHALLENGE"].append(sys.getsizeof(_challenges[0]))

            start_time_min_p = time.time()
            challenges = self._intermediate_channel.GetMinimumValue(
                challenges=_challenges, instrument=instrument, encoding="float"
            )
            challenges, duration_price = challenges.challenges, challenges.duration
            end_time_min_p = time.time()
            duration_price = end_time_min_p - start_time_min_p - duration_price

            timings["TIME_TO_GET_MINIMUM_VALUE_PRICE"].append(
                end_time_min_p - start_time_min_p
            )
            timings["TIME_TO_GET_MINIMUM_VALUE_PRICE_NET"].append(duration_price)

            results: List[int] = []
            for _index, challenge in enumerate(challenges):
                if index == _index:
                    continue

                results.append(-1 if challenge.minimum else 1)

            if results != expected:
                logger.error(
                    f"Intermediate returned wrong result for comparing padded price"
                )

            _minimum_price = (
                b_order_price if challenges[index].minimum else a_order_price
            )
            """

            _minimum_price, duration_price = self.compare_fn(
                (b_identifier, b_otp_price),
                (a_identifier, a_otp_price),
                instrument=instrument,
                cache={"direct": {}, "indirect": {}},
                correct_counter=[],
                total_counter=[],
                total_timings=[],
            )

            _minimum_price = b_order_price if _minimum_price == -1 else a_order_price
            if b_order_price == _minimum_price and a_order_price != _minimum_price:
                logger.info(
                    f"{self.__name__} ({instrument}): No more matches possible ..."
                )
                break
            else:
                _volume_pad = crypto.encode_float(values=[float(generate_random_int())])
                crypto._depth_map[hash(_volume_pad)] = 1

                start_time_pad = time.time()
                b_otp_volume = crypto.encrypt_add_plain_float(
                    b_order_volume, _volume_pad
                )
                end_time_pad = time.time()
                timings["TIME_TO_PAD_ORDER_VOLUME_BID"].append(
                    end_time_pad - start_time_pad
                )

                start_time_pad = time.time()
                a_otp_volume = crypto.encrypt_add_plain_float(
                    a_order_volume, _volume_pad
                )
                end_time_pad = time.time()
                timings["TIME_TO_PAD_ORDER_VOLUME_ASK"].append(
                    end_time_pad - start_time_pad
                )

                _minimum_volume, duration_volume = self.compare_fn(
                    (b_identifier, b_otp_volume),
                    (a_identifier, a_otp_volume),
                    instrument=instrument,
                    cache={"direct": {}, "indirect": {}},
                    correct_counter=[],
                    total_counter=[],
                    total_timings=[],
                )

                _minimum_volume = (
                    b_order_volume if _minimum_volume == -1 else a_order_volume
                )
                """
                index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
                _challenges: List[Challenge] = (
                    challenges[:index]
                    + [
                        Challenge(
                            first=b_otp_volume,
                            second=a_otp_volume,
                        )
                    ]
                    + challenges[index:]
                )
                start_time_min_v = time.time()
                challenges = self._intermediate_channel.GetMinimumValue(
                    challenges=_challenges, instrument=instrument, encoding="float"
                )
                challenges, duration_volume = challenges.challenges, challenges.duration
                end_time_min_v = time.time()
                duration_volume = end_time_min_v - start_time_min_v - duration_volume

                timings["TIME_TO_GET_MINIMUM_VALUE_VOLUME"].append(
                    end_time_min_v - start_time_min_v
                )
                timings["TIME_TO_GET_MINIMUM_VALUE_VOLUME_NET"].append(duration_volume)

                results: List[int] = []
                for _index, challenge in enumerate(challenges):
                    if index == _index:
                        continue

                    results.append(-1 if challenge.minimum else 1)

                if results != expected:
                    logger.error(
                        f"Intermediate returned wrong result for comparing padded volume"
                    )

                _minimum_volume = (
                    b_order_volume if challenges[index].minimum else a_order_volume
                )
                """

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_order_volume == _minimum_volume:
                    b_order_volume = ZERO_VOLUME
                    b_order.volume = b_order_volume
                    b_dropped.append(b_identifier)
                else:
                    b_order_volume = crypto.encrypt_sub_ciphertext_float(
                        b_order_volume, _minimum_volume
                    )
                    b_order.volume = b_order_volume

                if a_order_volume == _minimum_volume:
                    a_order_volume = ZERO_VOLUME
                    a_order.volume = a_order_volume
                    a_dropped.append(a_identifier)
                else:
                    a_order_volume = crypto.encrypt_sub_ciphertext_float(
                        a_order_volume, _minimum_volume
                    )
                    a_order.volume = a_order_volume

                end_match_time = time.time()
                timings["TIME_TO_MATCH_ORDER"].append(end_match_time - start_match_time)
                timings["TIME_TO_MATCH_ORDER_NET"].append(
                    duration_price + duration_volume
                )
                timings["SIZE_OF_ORDER"].append(sys.getsizeof(b_order))

                start_time_dec = time.time()
                d_order = self._intermediate_channel.DecryptOrder(
                    order=grpc_buffer_intermediate.CiphertextOrder(
                        type=a_order.type,
                        instrument=instrument,
                        volume=_minimum_volume.to_bytes(),
                        price=a_order_price.to_bytes(),
                    ),
                    entity_bid=b_order.entity,
                    entity_ask=a_order.entity,
                )
                end_time_dec = time.time()
                duration_d = end_time_dec - start_time_dec - d_order.duration

                timings["TIME_TO_DECRYPT_ORDER"].append(end_time_dec - start_time_dec)
                timings["TIME_TO_DECRYPT_ORDER_NET"].append(duration_d)

                logger.log(
                    level=TRADE_LOG_LEVEL,
                    msg=f"{self.__name__} ({instrument}): Trade 'BID' ({b_identifier}) V ({d_order.order.volume}), 'ASK' ({a_identifier}) V ({d_order.order.volume}) for P ({d_order.order.price})",
                )

                book.add_performed(
                    ask_identifier=a_identifier,
                    bid_identifier=b_identifier,
                    ask_entity=d_order.entity_ask,
                    bid_entity=d_order.entity_bid,
                    performed_price=d_order.order.price,
                    performed_volume=d_order.order.volume,
                    performed_time=datetime.now().strftime("%d/%m/%y %H:%M:%S.%f"),
                    bid_order=b_order_c,
                    ask_order=a_order_c,
                )

        return book, b_dropped, a_dropped

    def _match(
        self,
        instrument: str,
        book: OrderBook,
        timings: Dict[str, List[float]],
        remote_compare: bool,
    ) -> Tuple[OrderBook, List[str], List[str]]:
        start_match_time = time.time()

        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        b_dropped, a_dropped = [], []

        try:
            b_identifier, b_order = next(bid_queue)
        except StopIteration:
            logger.debug(f"{self.__name__} ({instrument}): No more bid orders ...")
            return book, b_dropped, a_dropped

        try:
            a_identifier, a_order = next(ask_queue)
        except StopIteration:
            logger.debug(f"{self.__name__} ({instrument}): No more ask orders ...")
            return book, b_dropped, a_dropped

        while True:
            if b_identifier in b_dropped:
                try:
                    b_identifier, b_order = next(bid_queue)
                except StopIteration:
                    logger.debug(
                        f"{self.__name__} ({instrument}): No more bid orders ..."
                    )
                    return book, b_dropped, a_dropped

            if a_identifier in a_dropped:
                try:
                    a_identifier, a_order = next(ask_queue)
                except StopIteration:
                    logger.debug(
                        f"{self.__name__} ({instrument}): No more ask orders ..."
                    )
                    return book, b_dropped, a_dropped

            _price_pad = generate_random_float()
            if remote_compare:
                start_time_pad = time.time()
                a_otp_price = a_order.price + _price_pad
                end_time_pad = time.time()
                timings["TIME_TO_PAD_ORDER_PRICE_ASK"].append(
                    end_time_pad - start_time_pad
                )

                start_time_pad = time.time()
                b_otp_price = b_order.price + _price_pad
                end_time_pad = time.time()
                timings["TIME_TO_PAD_ORDER_PRICE_BID"].append(
                    end_time_pad - start_time_pad
                )

                start_time_gen = time.time()
                expected, challenges = generate_challenges(
                    engine=None, n=self.challenge_count
                )
                end_time_gen = time.time()
                timings["TIME_TO_GENERATE_CHALLENGES"].append(
                    end_time_gen - start_time_gen
                )

                index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
                _challenges: List[ChallengePlain] = (
                    challenges[:index]
                    + [
                        ChallengePlain(
                            first=b_otp_price,
                            second=a_otp_price,
                        )
                    ]
                    + challenges[index:]
                )
                timings["SIZE_OF_CHALLENGE"].append(sys.getsizeof(_challenges[0]))

                start_time_min_p = time.time()
                challenges = self._intermediate_channel.GetMinimumValuePlain(
                    challenges=_challenges, instrument=instrument, encoding="float"
                )
                challenges, duration_price = challenges.challenges, challenges.duration
                end_time_min_p = time.time()
                duration_price = end_time_min_p - start_time_min_p - duration_price

                timings["TIME_TO_GET_MINIMUM_VALUE"].append(
                    end_time_min_p - start_time_min_p
                )
                timings["TIME_TO_GET_MINIMUM_VALUE_NET"].append(duration_price)

                results: List[int] = []
                for _index, challenge in enumerate(challenges):
                    if index == _index:
                        continue

                    results.append(-1 if challenge.minimum else 1)

                if results != expected:
                    logger.error(
                        f"Intermediate returned wrong result for comparing padded volume"
                    )

                _minimum_otp_price = (
                    b_otp_price if challenges[index].minimum else a_otp_price
                )
            else:
                a_otp_price = a_order.price + _price_pad
                b_otp_price = b_order.price + _price_pad
                duration_price = 0
                _minimum_otp_price = min(b_otp_price, a_otp_price)

            if b_otp_price <= a_otp_price:
                logger.info(
                    f"{self.__name__} ({instrument}): No more matches possible ..."
                )
                break
            else:
                if remote_compare:
                    _volume_pad = float(generate_random_int())
                    start_time_pad = time.time()
                    a_otp_volume = a_order.volume + _volume_pad
                    end_time_pad = time.time()
                    timings["TIME_TO_PAD_ORDER_VOLUME_ASK"].append(
                        end_time_pad - start_time_pad
                    )

                    start_time_pad = time.time()
                    b_otp_volume = b_order.volume + _volume_pad
                    end_time_pad = time.time()
                    timings["TIME_TO_PAD_ORDER_VOLUME_BID"].append(
                        end_time_pad - start_time_pad
                    )

                    start_time_pad = time.time()
                    expected, challenges = generate_challenges(
                        engine=None, n=self.challenge_count
                    )
                    end_time_pad = time.time()
                    timings["TIME_TO_GENERATE_CHALLENGES"].append(
                        end_time_pad - start_time_pad
                    )

                    index = (
                        randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
                    )
                    _challenges: List[ChallengePlain] = (
                        challenges[:index]
                        + [
                            ChallengePlain(
                                first=b_otp_volume,
                                second=a_otp_volume,
                            )
                        ]
                        + challenges[index:]
                    )
                    start_time_min_v = time.time()
                    challenges = self._intermediate_channel.GetMinimumValuePlain(
                        challenges=_challenges, instrument=instrument, encoding="float"
                    )
                    challenges, duration_volume = (
                        challenges.challenges,
                        challenges.duration,
                    )
                    end_time_min_v = time.time()
                    duration_volume = (
                        end_time_min_v - start_time_min_v - duration_volume
                    )

                    timings["TIME_TO_GET_MINIMUM_VALUE"].append(
                        end_time_min_v - start_time_min_v
                    )
                    timings["TIME_TO_GET_MINIMUM_VALUE_NET"].append(duration_volume)

                    results: List[int] = []
                    for _index, challenge in enumerate(challenges):
                        if index == _index:
                            continue

                        results.append(-1 if challenge.minimum else 1)

                    if results != expected:
                        logger.error(
                            f"Intermediate returned wrong result for comparing padded volume"
                        )

                    start_time_pad = time.time()
                    min_volume = (
                        (b_otp_volume - _volume_pad)
                        if challenges[index].minimum
                        else (a_otp_volume - _volume_pad)
                    )
                    end_time_pad = time.time()
                    timings["TIME_TO_UNPAD_ORDER_VOLUME_MINIMUM"].append(
                        end_time_pad - start_time_pad
                    )
                else:
                    duration_volume = 0
                    min_volume = min(b_order.volume, a_order.volume)

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_order.volume == min_volume:
                    b_order.volume = 0
                    b_dropped.append(b_identifier)
                else:
                    b_order.volume = int(b_order.volume - min_volume)

                if a_order.volume == min_volume:
                    a_order.volume = 0
                    a_dropped.append(a_identifier)
                else:
                    a_order.volume = int(a_order.volume - min_volume)

                end_match_time = time.time()
                timings["TIME_TO_MATCH_ORDER"].append(end_match_time - start_match_time)
                timings["TIME_TO_MATCH_ORDER_NET"].append(
                    duration_price + duration_volume
                )
                timings["SIZE_OF_ORDER"].append(sys.getsizeof(b_order))

                logger.log(
                    level=TRADE_LOG_LEVEL,
                    msg=f"{self.__name__} ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume}) -> V ({b_order.volume}), 'ASK' ({a_identifier}) V ({a_order_c.volume}) -> V ({a_order.volume}) for P ({a_order.price})",
                )

                # TODO: Publish in a cleartext form since it has been executed
                book.add_performed(
                    ask_identifier=a_identifier,
                    bid_identifier=b_identifier,
                    ask_entity=a_order.entity,
                    bid_entity=b_order.entity,
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
        encrypted: bool,
        local_sort: bool,
    ) -> Tuple[Union[EncryptedOrderBook, OrderBook], List[str], List[str]]:
        """Match one iteration of orders agaist each other"""
        correct_counter_bid = []
        total_counter_bid = []
        total_timings_bid = []

        start_matching_time = time.time()

        if not book.sorted:
            sort_bid_start = time.time()
            book.sort(
                type=OrderType.BID,
                func=lambda first, second: self.compare_fn(
                    first=first,
                    second=second,
                    instrument=instrument,
                    cache=book._book_compared,
                    correct_counter=correct_counter_bid,
                    total_counter=total_counter_bid,
                    total_timings=total_timings_bid,
                )[0],
            )
            sort_bid_end = time.time()
            correct_counter_bid = sum(correct_counter_bid)
            total_counter_bid = len(total_counter_bid)

            self.book[instrument].add_metrics(
                category="sorting",
                section="orders",
                value={
                    "type": "bid",
                    "length": total_counter_bid,
                    "accuracy": (correct_counter_bid / total_counter_bid)
                    if total_counter_bid
                    else 1,
                    "duration": sort_bid_end - sort_bid_start,
                },
            )

            self.book[instrument].add_metrics(
                category="sorting",
                section="pairs",
                value={"type": "bid", "timings": total_timings_bid},
            )

            correct_counter_ask = []
            total_counter_ask = []
            total_timings_ask = []

        if not book.sorted:
            sort_ask_start = time.time()
            book.sort(
                type=OrderType.ASK,
                func=lambda first, second: self.compare_fn(
                    first=first,
                    second=second,
                    instrument=instrument,
                    cache=book._book_compared,
                    correct_counter=correct_counter_ask,
                    total_counter=total_counter_ask,
                    total_timings=total_timings_ask,
                )[0],
            )
            sort_ask_end = time.time()
            correct_counter_ask = sum(correct_counter_ask)
            total_counter_ask = len(total_counter_ask)

            self.book[instrument].add_metrics(
                category="sorting",
                section="orders",
                value={
                    "type": "ask",
                    "length": total_counter_ask,
                    "accuracy": (correct_counter_ask / total_counter_ask)
                    if total_counter_ask
                    else 1,
                    "duration": sort_ask_end - sort_ask_start,
                },
            )

            self.book[instrument].add_metrics(
                category="sorting",
                section="pairs",
                value={"type": "ask", "timings": total_timings_ask},
            )

        book.sorted = True

        timings = {
            "TIME_TO_MATCH_ORDER": [],
            "TIME_TO_MATCH_ORDER_NET": [],
            "TIME_TO_DECRYPT_ORDER": [],
            "TIME_TO_DECRYPT_ORDER_NET": [],
            "TIME_TO_PAD_ORDER_PRICE_BID": [],
            "TIME_TO_PAD_ORDER_PRICE_ASK": [],
            "TIME_TO_UNPAD_ORDER_PRICE_BID": [],
            "TIME_TO_UNPAD_ORDER_PRICE_ASK": [],
            "TIME_TO_PAD_ORDER_VOLUME_BID": [],
            "TIME_TO_PAD_ORDER_VOLUME_ASK": [],
            "TIME_TO_UNPAD_ORDER_VOLUME_BID": [],
            "TIME_TO_UNPAD_ORDER_VOLUME_ASK": [],
            "TIME_TO_GENERATE_CHALLENGES": [],
            "TIME_TO_GET_MINIMUM_VALUE": [],
            "TIME_TO_GET_MINIMUM_VALUE_NET": [],
            "TIME_TO_UNPAD_ORDER_VOLUME_MINIMUM": [],
            "SIZE_OF_CHALLENGE": [],
            "SIZE_OF_ORDER": [],
        }

        if encrypted:
            result = self._match_encrypted(
                instrument=instrument,
                book=book,
                timings=timings,
            )
        else:
            result = self._match(
                instrument=instrument,
                book=book,
                timings=timings,
                remote_compare=not local_sort,
            )

        iteration_time = time.time() - self.matcher_start_time
        logger.info(f"Completed matching iteration at: '{iteration_time}'")

        self.book[instrument].add_metrics(
            category="iteration", section="time", value={"timings": [iteration_time]}
        )

        self.book[instrument].add_metrics(
            category="size",
            section="order",
            value={"sizes": timings["SIZE_OF_ORDER"]},
        )

        self.book[instrument].add_metrics(
            category="size",
            section="challenge",
            value={"sizes": timings["SIZE_OF_CHALLENGE"]},
        )

        self.book[instrument].add_metrics(
            category="match",
            section="pairs",
            value={"timings": timings["TIME_TO_MATCH_ORDER"]},
        )

        self.book[instrument].add_metrics(
            category="match",
            section="pairs_net",
            value={"timings": timings["TIME_TO_MATCH_ORDER_NET"]},
        )

        self.book[instrument].add_metrics(
            category="decrypt",
            section="order",
            value={"timings": timings["TIME_TO_DECRYPT_ORDER"]},
        )

        self.book[instrument].add_metrics(
            category="challenges",
            section="generate",
            value={"timings": timings["TIME_TO_GENERATE_CHALLENGES"]},
        )

        self.book[instrument].add_metrics(
            category="minimum",
            section="value",
            value={"timings": timings["TIME_TO_GET_MINIMUM_VALUE"]},
        )

        self.book[instrument].add_metrics(
            category="minimum",
            section="value_net",
            value={"timings": timings["TIME_TO_GET_MINIMUM_VALUE_NET"]},
        )

        self.book[instrument].add_metrics(
            category="pad",
            section="price",
            value={"type": "bid", "timings": timings["TIME_TO_PAD_ORDER_PRICE_BID"]},
        )

        self.book[instrument].add_metrics(
            category="pad",
            section="price",
            value={"type": "ask", "timings": timings["TIME_TO_PAD_ORDER_PRICE_ASK"]},
        )

        self.book[instrument].add_metrics(
            category="unpad",
            section="price",
            value={
                "type": "bid",
                "timings": timings["TIME_TO_UNPAD_ORDER_PRICE_BID"],
            },
        )

        self.book[instrument].add_metrics(
            category="unpad",
            section="price",
            value={
                "type": "ask",
                "timings": timings["TIME_TO_UNPAD_ORDER_PRICE_ASK"],
            },
        )

        self.book[instrument].add_metrics(
            category="unpad",
            section="minimum",
            value={
                "timings": timings["TIME_TO_UNPAD_ORDER_VOLUME_MINIMUM"],
            },
        )

        self.book[instrument].add_metrics(
            category="pad",
            section="volume",
            value={"type": "bid", "timings": timings["TIME_TO_PAD_ORDER_VOLUME_BID"]},
        )

        self.book[instrument].add_metrics(
            category="pad",
            section="volume",
            value={"type": "ask", "timings": timings["TIME_TO_PAD_ORDER_VOLUME_ASK"]},
        )

        self.book[instrument].add_metrics(
            category="unpad",
            section="volume",
            value={
                "type": "bid",
                "timings": timings["TIME_TO_UNPAD_ORDER_VOLUME_BID"],
            },
        )

        self.book[instrument].add_metrics(
            category="unpad",
            section="volume",
            value={
                "type": "ask",
                "timings": timings["TIME_TO_UNPAD_ORDER_VOLUME_ASK"],
            },
        )

        return result

    def _compare_local(
        self,
        first: Tuple[str, bytes],
        second: Tuple[str, bytes],
        instrument: str,
        cache: Dict[Tuple[str, str], int],
        correct_counter: List[bool],
        total_counter: List[bool],
        total_timings: List[float],
        inserting: bool = False,
    ) -> Union[grpc_buffer.PlaintextLimitOrder, grpc_buffer.PlaintextMarketOrder]:
        """Compare two unencrypted items and return the smallest using the built in compare."""
        start_time = time.time()
        if (
            self.time_limit is not None
            and time.time() - self.matcher_start_time >= self.time_limit
        ):
            raise StopIteration

        first_identifier, first = first
        second_identifier, second = second

        _cache_result = self.is_cached(first_identifier, second_identifier, cache)
        # _cache_result = cache.get((first_identifier, second_identifier))

        # TODO: Perhaps something smarter, that can determine like a binary tree
        if _cache_result is not None:
            return _cache_result, 0

        def compare(first, second):
            """Compare two values unencrypted and return result."""

            result = expected = -1 if first < second else 1
            logger.debug(f"f = {first}, s = {second}")

            return result, expected

        result, expected = compare(
            first,
            second,
        )

        cache["direct"][(first_identifier, second_identifier)] = result
        cache["direct"][(second_identifier, first_identifier)] = -1 * result
        cache["indirect"][first_identifier][result].append(second_identifier)
        cache["indirect"][second_identifier][-1 * result].append(first_identifier)

        end_time = time.time()

        if inserting:
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="local",
                value={
                    "timings": [end_time - start_time],
                },
            )
        else:
            self.book[instrument].add_metrics(
                category="compare",
                section="local",
                value={
                    "timings": [end_time - start_time],
                },
            )

        correct_counter.append(result == expected)
        total_counter.append(True)
        total_timings.append(end_time - start_time)
        return result, 0

    def _compare_remote(
        self,
        first: Tuple[str, bytes],
        second: Tuple[str, bytes],
        instrument: str,
        cache: Dict[Tuple[str, str], int],
        correct_counter: List[bool],
        total_counter: List[bool],
        total_timings: List[float],
        inserting: bool = False,
    ) -> Tuple[int, float]:
        """Compare two items and return the smallest using the remote intermediate."""
        start_time = time.time()
        if (
            self.time_limit is not None
            and time.time() - self.matcher_start_time >= self.time_limit
        ):
            raise StopIteration

        first_identifier, first = first
        second_identifier, second = second

        _cache_result = self.is_cached(first_identifier, second_identifier, cache)
        if _cache_result is not None:
            return _cache_result, 0

        expected, challenges = generate_challenges(engine=None, n=self.challenge_count)
        index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
        _challenges: List[ChallengePlain] = (
            challenges[:index]
            + [ChallengePlain(first=first, second=second)]
            + challenges[index:]
        )

        start_time_min = time.time()
        challenges = self._intermediate_channel.GetMinimumValuePlain(
            challenges=_challenges, instrument=instrument, encoding="float"
        )
        challenges, duration = challenges.challenges, challenges.duration
        end_time_min = time.time()
        duration = end_time_min - start_time_min - duration

        results: List[int] = []
        for _index, challenge in enumerate(challenges):
            if index == _index:
                continue

            results.append(-1 if challenge.minimum else 1)

        if results != expected:
            logger.error(f"Intermediate returned wrong result for remote compare")

        result = -1 if challenges[index].minimum else 1

        expected = -1 if first < second else 1

        cache["direct"][(first_identifier, second_identifier)] = result
        cache["direct"][(second_identifier, first_identifier)] = -1 * result
        cache["indirect"][first_identifier][result].append(second_identifier)
        cache["indirect"][second_identifier][-1 * result].append(first_identifier)

        end_time = time.time()

        if inserting:
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="remote",
                value={
                    "timings": [end_time - start_time],
                },
            )
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="remote_net",
                value={
                    "timings": [duration],
                },
            )
            self.book[instrument].add_metrics(
                category="minimum_insert",
                section="value",
                value={"timings": [end_time - start_time]},
            )
            self.book[instrument].add_metrics(
                category="minimum_insert",
                section="value_net",
                value={"timings": [duration]},
            )

        else:
            self.book[instrument].add_metrics(
                category="compare",
                section="remote",
                value={
                    "timings": [end_time - start_time],
                },
            )
            self.book[instrument].add_metrics(
                category="compare",
                section="remote_net",
                value={
                    "timings": [duration],
                },
            )
            self.book[instrument].add_metrics(
                category="minimum",
                section="value",
                value={"timings": [end_time - start_time]},
            )
            self.book[instrument].add_metrics(
                category="minimum",
                section="value_net",
                value={"timings": [duration]},
            )

        correct_counter.append(result == expected)
        total_counter.append(True)
        total_timings.append(end_time - start_time)
        return result, duration

    def _compare_local_encrypted(
        self,
        first: Tuple[str, bytes],
        second: Tuple[str, bytes],
        instrument: str,
        cache: Dict[Tuple[str, str], int],
        correct_counter: List[bool],
        total_counter: List[bool],
        total_timings: List[float],
        inserting: bool = False,
    ) -> Tuple[int, float]:
        """Compare two encrypted item and return the smallest using the local estimation.
        The identity of max(f, s) follows that:
        max(f, s) = ((f + s) / 2) + (abs(f - s) / 2) = ((f + s) / 2) + (sqrt((f - s)^2) / 2)
        """
        start_time = time.time()
        if (
            self.time_limit is not None
            and time.time() - self.matcher_start_time >= self.time_limit
        ):
            raise StopIteration

        crypto = self.crypto[instrument]

        first_identifier, first = first
        second_identifier, second = second

        _cache_result = self.is_cached(first_identifier, second_identifier, cache)
        # _cache_result = cache.get((first_identifier, second_identifier))

        # TODO: Perhaps something smarter, that can determine like a binary tree
        if _cache_result is not None:
            return _cache_result, 0

        def scale_down(value: PyCtxt, l: float, half: PyPtxt) -> PyCtxt:
            """Scales down the value to the range [0, 1] using a given value l
            _a = 0.5 + (a / 2 ** l)
            """
            denom = crypto.encode_float(values=[1 / (2 ** l)])
            crypto.encrypt_mult_plain_float(
                ciphertext=value, value=denom, to_bytes=False, new_ctxt=False
            )

            crypto.encrypt_add_plain_float(
                ciphertext=value, value=half, to_bytes=False, new_ctxt=False
            )

            return value

        def scale_down2(value: PyCtxt, denom: PyPtxt) -> PyCtxt:
            """Scales down the value to the range [0, 1]"""
            crypto.encrypt_mult_plain_float(
                ciphertext=value, value=denom, to_bytes=False, new_ctxt=False
            )
            crypto._pyfhel.rescale_to_next(value)
            value.round_scale()

            return value

        def compare(first, second, iterations, sigmoid_iterations, constant_count):
            """Compare two values encrypted homomorphically and return result."""
            # x <- a - b
            # for i <- 1 to d do
            #    x <- func(x)
            # end for
            # return (x + 1) / 2

            a = crypto.from_bytes(first)
            b = crypto.from_bytes(second)

            crypto._depth_map[hash(a)] = 1
            crypto._depth_map[hash(b)] = 1

            def func(x, iterations, constant_index, _pre_calc):
                _x = {1: PyCtxt(copy_ctxt=x)}
                crypto._depth_map[hash(_x[1])] = crypto._depth_map[hash(x)]
                _sum = crypto.encrypt_float(value=0.0, to_bytes=False)

                for i in range(1, iterations + 1, 2):
                    if i > 1:
                        for _ in range(2):
                            crypto.encrypt_mult_ciphertext_float(
                                ciphertext=_x[i],
                                value=x,
                                to_bytes=False,
                                new_ctxt=False,
                            )

                    _res = crypto.encrypt_mult_plain_float(
                        ciphertext=_x[i],
                        value=_pre_calc[i],
                        new_ctxt=True,
                        to_bytes=False,
                    )

                    crypto.encrypt_add_ciphertext_float(
                        ciphertext=_sum,
                        value=_res,
                        to_bytes=False,
                        new_ctxt=False,
                    )

                    _x[i + 2] = PyCtxt(copy_ctxt=x)
                    crypto._depth_map[hash(_x[i + 2])] = crypto._depth_map[hash(x)]

                return _sum

            # Depth
            # a -> 1
            # b -> 1

            L = 8
            half = crypto.encode_float(values=[0.5])

            a = scale_down(value=a, l=L, half=half)
            b = scale_down(value=b, l=L, half=half)

            # Depth
            # a -> 2
            # b -> 2

            x = crypto.encrypt_sub_ciphertext_float(
                ciphertext=a, value=b, to_bytes=False, new_ctxt=True
            )

            if constant_count == 3:
                _pre_calc = {1: 3 / 2, 3: -1 / 2}
            elif constant_count == 5:
                _pre_calc = {1: 15 / 8, 3: -10 / 8, 5: 3 / 8}
            elif constant_count == 9:
                _pre_calc = {
                    1: 315 / 128,
                    3: -420 / 128,
                    5: 378 / 128,
                    7: -180 / 128,
                    9: 35 / 128,
                }
            else:
                raise ValueError(
                    f"Invalid constant count: '{constant_count}' for compare"
                )

            for i in range(1, constant_count + 1, 2):
                _pre_calc[i] = crypto.encode_float([_pre_calc[i]])

            for i in range(iterations):
                x = func(x, sigmoid_iterations, i, _pre_calc)
                if i + 1 >= iterations:
                    break

            one = crypto.encode_float([1.0])

            crypto.encrypt_add_plain_float(
                ciphertext=x, value=one, to_bytes=False, new_ctxt=False
            )

            crypto.encrypt_mult_plain_float(
                ciphertext=x, value=half, to_bytes=False, new_ctxt=False
            )

            half = crypto.encrypt_float(0.5, to_bytes=True)
            expected, challenges = generate_challenges(
                engine=crypto, n=self.challenge_count
            )
            index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
            _challenges: List[Challenge] = (
                challenges[:index]
                + [
                    Challenge(
                        first=crypto.to_bytes(ctxt=x),
                        second=half,
                    )
                ]
                + challenges[index:]
            )
            start_time_min = time.time()
            challenges = self._intermediate_channel.GetMinimumValue(
                challenges=_challenges, instrument=instrument, encoding="float"
            )
            end_time_min = time.time()
            challenges, duration = challenges.challenges, challenges.duration
            duration = end_time_min - start_time_min - duration

            if inserting:
                self.book[instrument].add_metrics(
                    category="minimum_insert",
                    section="value",
                    value={"timings": [end_time_min - start_time_min]},
                )
                self.book[instrument].add_metrics(
                    category="minimum_insert",
                    section="value_net",
                    value={"timings": [duration]},
                )
            else:
                self.book[instrument].add_metrics(
                    category="minimum",
                    section="value",
                    value={"timings": [end_time_min - start_time_min]},
                )
                self.book[instrument].add_metrics(
                    category="minimum",
                    section="value_net",
                    value={"timings": [duration]},
                )

            results: List[int] = []
            for _index, challenge in enumerate(challenges):
                if index == _index:
                    continue

                results.append(-1 if challenge.minimum else 1)

            if results != expected:
                logger.error(f"Intermediate returned wrong result for sorting")

            result = -1 if challenges[index].minimum else 1

            # logger.debug(
            #     f"a = {crypto.decrypt_float(a)}, b = {crypto.decrypt_float(b)}, x = {crypto.decrypt_float(x)}, f = {crypto.decrypt_float(first)}, s = {crypto.decrypt_float(second)}"
            # )

            cache["direct"][(first_identifier, second_identifier)] = result
            cache["direct"][(second_identifier, first_identifier)] = -1 * result
            cache["indirect"][first_identifier][result].append(second_identifier)
            cache["indirect"][second_identifier][-1 * result].append(first_identifier)

            return result, expected, duration

        result, expected, duration = compare(
            first,
            second,
            iterations=self.compare_iterations,
            sigmoid_iterations=self.compare_sigmoid_iterations,
            constant_count=self.compare_constant_count,
        )
        end_time = time.time()

        if inserting:
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="local",
                value={
                    "timings": [end_time - start_time],
                },
            )

            self.book[instrument].add_metrics(
                category="compare_insert",
                section="local_net",
                value={
                    "timings": [duration],
                },
            )
        else:
            self.book[instrument].add_metrics(
                category="compare",
                section="local",
                value={
                    "timings": [end_time - start_time],
                },
            )

            self.book[instrument].add_metrics(
                category="compare",
                section="local_net",
                value={
                    "timings": [duration],
                },
            )

        correct_counter.append(result == expected)
        total_counter.append(True)
        total_timings.append(end_time - start_time)
        return result, duration

    def _compare_remote_encrypted(
        self,
        first: Tuple[str, bytes],
        second: Tuple[str, bytes],
        instrument: str,
        cache: Dict[Tuple[str, str], int],
        correct_counter: List[bool],
        total_counter: List[bool],
        total_timings: List[float],
        inserting: bool = False,
    ) -> Tuple[int, float]:
        """Compare two encrypted item and return the smallest using the remote intermediate."""
        start_time = time.time()
        if (
            self.time_limit is not None
            and time.time() - self.matcher_start_time >= self.time_limit
        ):
            raise StopIteration

        crypto = self.crypto[instrument]

        first_identifier, first = first
        second_identifier, second = second

        _cache_result = self.is_cached(first_identifier, second_identifier, cache)
        # _cache_result = cache.get((first_identifier, second_identifier))

        # TODO: Perhaps something smarter, that can determine like a binary tree
        if _cache_result is not None:
            return _cache_result, 0

        expected, challenges = generate_challenges(
            engine=crypto, n=self.challenge_count
        )
        index = randint(0, len(challenges) - 1) if len(challenges) > 0 else 0
        _challenges: List[Challenge] = (
            challenges[:index]
            + [
                Challenge(
                    first=first,
                    second=second,
                )
            ]
            + challenges[index:]
        )
        start_time_min = time.time()
        challenges = self._intermediate_channel.GetMinimumValue(
            challenges=_challenges, instrument=instrument, encoding="float"
        )
        end_time_min = time.time()
        challenges, duration = challenges.challenges, challenges.duration
        duration = end_time_min - start_time_min - duration

        results: List[int] = []
        for _index, challenge in enumerate(challenges):
            if index == _index:
                continue

            results.append(-1 if challenge.minimum else 1)

        if results != expected:
            logger.error(f"Intermediate returned wrong result for remote compare")

        result = -1 if challenges[index].minimum else 1

        # expected = (
        #     -1
        #     if crypto.decrypt_float(first)
        #     < crypto.decrypt_float(second)
        #     else 1
        # )

        # correct_counter.append(result == expected)

        cache["direct"][(first_identifier, second_identifier)] = result
        cache["direct"][(second_identifier, first_identifier)] = -1 * result
        cache["indirect"][first_identifier][result].append(second_identifier)
        cache["indirect"][second_identifier][-1 * result].append(first_identifier)

        end_time = time.time()

        if inserting:
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="remote",
                value={
                    "timings": [end_time - start_time],
                },
            )
            self.book[instrument].add_metrics(
                category="compare_insert",
                section="remote_net",
                value={
                    "timings": [duration],
                },
            )
            self.book[instrument].add_metrics(
                category="minimum_insert",
                section="value",
                value={"timings": [end_time - start_time]},
            )
            self.book[instrument].add_metrics(
                category="minimum_insert",
                section="value_net",
                value={"timings": [duration]},
            )
        else:
            self.book[instrument].add_metrics(
                category="compare",
                section="remote",
                value={
                    "timings": [end_time - start_time],
                },
            )
            self.book[instrument].add_metrics(
                category="compare",
                section="remote_net",
                value={
                    "timings": [duration],
                },
            )
            self.book[instrument].add_metrics(
                category="minimum",
                section="value",
                value={"timings": [end_time - start_time]},
            )
            self.book[instrument].add_metrics(
                category="minimum",
                section="value_net",
                value={"timings": [duration]},
            )

        total_counter.append(True)
        total_timings.append(end_time - start_time)
        return result, duration

    def is_cached(
        self, first: str, second: str, cache: Dict[str, Any]
    ) -> Union[int, None]:
        """Determines if a compare can be derived from previous results.
        1. Checks if pair has been compared before, direct check.
        2. Checks if pair result can be derived from transitive compare, indirect.
        3. Not cached, None
        """
        # The direct cache is only really used when sorting completely
        if (first, second) in cache["direct"]:
            return cache["direct"][(first, second)]

        _indirect = cache["indirect"].setdefault(first, {1: [], -1: []})

        # This requires both of the items to have been compared before but not with each other
        result = None
        if result is None:
            for third in _indirect[1]:
                # first < third

                if second in cache["indirect"][third][1]:
                    # third < second, therefore we have first < second
                    result = 1
                    break
                elif second in cache["indirect"][third][-1]:
                    # second < third, therefore we can't determine the result
                    break

        if result is None:
            for third in _indirect[-1]:
                # first > third

                if second in cache["indirect"][third][-1]:
                    # third > second, therefore we have first > second
                    result = -1
                    break
                elif second in cache["indirect"][third][1]:
                    # second > third, therefore we can't determine the result
                    break

        _indirect = cache["indirect"].setdefault(second, {1: [], -1: []})

        if result is None:
            for third in _indirect[1]:
                # second < third

                if first in cache["indirect"][third][1]:
                    # third < first, therefore we have first > second
                    result = -1
                    break
                elif first in cache["indirect"][third][-1]:
                    # first < third, therefore we can't determine the result
                    break

        if result is None:
            for third in _indirect[-1]:
                # second > third

                if first in cache["indirect"][third][-1]:
                    # third > first, therefore we have first < second
                    result = 1
                    break
                elif first in cache["indirect"][third][1]:
                    # first > third, therefore we can't determine the result
                    break

        if result is not None:
            cache["direct"][(first, second)] = result
            cache["direct"][(second, first)] = -1 * result
            cache["indirect"][first][result].append(second)
            cache["indirect"][second][-1 * result].append(first)

        return result

    def match(
        self,
        encrypted: Optional[str],
        local_sort: bool,
    ) -> NoReturn:
        """Continously match incoming orders against each other
        Runs the sub matchers _match and _match_plaintext depending on if the orders are encrypted
        """
        self.matcher_start_time = None
        with ThreadPoolExecutor(max_workers=10) as pool:
            while True:
                try:
                    future_to_match: Dict[Future, str] = {}
                    for instrument, book in self.book.items():
                        bids = self._cache_bid.setdefault(instrument, 0)
                        asks = self._cache_ask.setdefault(instrument, 0)
                        if bids != book.bid_count or asks != book.ask_count:
                            if self.matcher_start_time is None:
                                self.matcher_start_time = time.time()

                            self._cache_bid[instrument] = book.bid_count
                            self._cache_ask[instrument] = book.ask_count
                            future_to_match[
                                pool.submit(
                                    self._match_orders,
                                    instrument=instrument,
                                    book=book,
                                    encrypted=encrypted,
                                    local_sort=local_sort,
                                )
                            ] = instrument

                    for future in as_completed(future_to_match):
                        instrument = future_to_match[future]
                        if encrypted:
                            crypto = self.crypto[instrument]
                        try:
                            other_book, b_dropped, a_dropped = future.result()

                            self.book[instrument].merge(
                                other_book,
                                b_dropped,
                                a_dropped,
                            )
                            if self.output is not None:
                                self._write_output(instrument)
                        except StopIteration:
                            if self.output is not None:
                                self._write_output(instrument)
                            raise StopIteration from None
                        except Exception as e:
                            raise e from None

                except Exception as e:
                    logger.error(
                        f"{self.__name__} (Global): Encountered error during matching: {e}"
                    )
                    import traceback

                    print(traceback.format_exc())
                    raise e from None
