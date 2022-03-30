#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union, Generator, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from copy import deepcopy, copy
from datetime import datetime
from random import randint
from pathlib import Path
from math import comb
import logging
import json
import time

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
from pet_exchange.proto.intermediate_pb2 import Challenge
from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.common.utils import (
    generate_random_int,
    generate_random_float,
    generate_challenges,
)
from pet_exchange.common.types import OrderType
from pet_exchange.common.crypto import BFV, CKKS
from pet_exchange.utils.logging import TRADE_LOG_LEVEL

logger = logging.getLogger("__main__")


class MatchingEngine:
    __name__ = "Matching-Engine"

    def __init__(self, output: str, instruments: List[str]):
        self.book: Dict[str, Union[EncryptedOrderBook, OrderBook]] = {}
        self.keys: Dict[str, bytes] = {}
        self.relin_keys: Dict[str, bytes] = {}
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
                    _book[instrument] = {"PERFORMED": {}, "METRICS": {}}

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
        scheme: str,
        challenge_count: int,
    ) -> Tuple[EncryptedOrderBook, List[str], List[str]]:
        bid_queue = book.queue(type=OrderType.BID)
        ask_queue = book.queue(type=OrderType.ASK)
        b_dropped, a_dropped = [], []
        _scheme_engine: Union[CKKS, BFV] = None
        if scheme == "bfv":
            _scheme_engine = BFV(pyfhel=self.pyfhel[instrument])
        elif scheme == "ckks":
            _scheme_engine = CKKS(pyfhel=self.pyfhel[instrument])
        else:
            raise ValueError(f"Unknown cryptographic scheme type provided: '{scheme}'")

        ZERO_VOLUME = _scheme_engine.encrypt_float(0.0)

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

        a_order_price = PyCtxt(serialized=a_order.price, pyfhel=_scheme_engine._pyfhel)
        _scheme_engine._depth_map[hash(a_order_price)] = 1

        b_order_price = PyCtxt(serialized=b_order.price, pyfhel=_scheme_engine._pyfhel)
        _scheme_engine._depth_map[hash(b_order_price)] = 1

        a_order_volume = PyCtxt(serialized=a_order.price, pyfhel=_scheme_engine._pyfhel)
        _scheme_engine._depth_map[hash(a_order_volume)] = 1

        b_order_volume = PyCtxt(serialized=b_order.volume, pyfhel=_scheme_engine._pyfhel)
        _scheme_engine._depth_map[hash(b_order_volume)] = 1

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

            _price_pad = _scheme_engine.encode_float(values=[generate_random_float()])
            _scheme_engine._depth_map[hash(_price_pad)] = 1

            b_otp_price, a_otp_price = _scheme_engine.encrypt_add_plain_float(
                b_order_price, _price_pad
            ), _scheme_engine.encrypt_add_plain_float(
                a_order_price,
                _price_pad,
            )

            expected, challenges = generate_challenges(
                engine=_scheme_engine, n=challenge_count
            )
            index = randint(0, len(challenges) - 1)
            _challenges: List[Challenge] = (
                challenges[:index]
                + [Challenge(first=b_otp_price, second=a_otp_price)]
                + challenges[index:]
            )
            start_time = time.time()
            challenges = self._intermediate_channel.GetMinimumValue(
                challenges=_challenges, instrument=instrument, encoding="float"
            ).challenges
            end_time = time.time()

            results: List[int] = []
            for _index, challenge in enumerate(challenges):
                if index == _index:
                    continue

                results.append(
                    -1 if challenge.minimum == _challenges[_index].first else 1
                )

            if results != expected:
                logger.error(
                    f"Intermediate returned wrong result for comparing padded price"
                )

            _minimum_otp_price = challenges[index].minimum

            if b_otp_price == _minimum_otp_price and a_otp_price != _minimum_otp_price:
                logger.info(
                    f"{self.__name__} ({instrument}): No more matches possible ..."
                )
                break
            else:
                _volume_pad = _scheme_engine.encode_float(values=[float(generate_random_int())])
                _scheme_engine._depth_map[hash(_volume_pad)] = 1
                b_otp_volume, a_otp_volume = _scheme_engine.encrypt_add_plain_float(
                    b_order_volume, _volume_pad
                ), _scheme_engine.encrypt_add_plain_float(
                    a_order_volume, _volume_pad
                )

                expected, challenges = generate_challenges(
                    engine=_scheme_engine, n=challenge_count
                )
                index = randint(0, len(challenges) - 1)
                _challenges: List[Challenge] = (
                    challenges[:index]
                    + [Challenge(first=b_otp_volume, second=a_otp_volume)]
                    + challenges[index:]
                )
                start_time = time.time()
                challenges = self._intermediate_channel.GetMinimumValue(
                    challenges=_challenges, instrument=instrument, encoding="float"
                ).challenges
                end_time = time.time()

                results: List[int] = []
                for _index, challenge in enumerate(challenges):
                    if index == _index:
                        continue

                    results.append(
                        -1 if challenge.minimum == _challenges[_index].first else 1
                    )

                if results != expected:
                    logger.error(
                        f"Intermediate returned wrong result for comparing padded volume"
                    )

                _minimum_otp_volume = challenges[index].minimum
                _scheme_engine._depth_map[hash(_minimum_otp_volume)] = _scheme_engine._depth_map[hash(_volume_pad)]

                _minimum_volume = _scheme_engine.encrypt_sub_plain_float(
                    _minimum_otp_volume,
                    _volume_pad,
                )

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_otp_volume == _minimum_otp_volume:
                    b_order_volume = ZERO_VOLUME
                    b_dropped.append(b_identifier)
                else:
                    b_order_volume = _scheme_engine.encrypt_sub_ciphertext_float(
                        b_order_volume, _minimum_volume
                    )

                if a_otp_volume == _minimum_otp_volume:
                    a_order_volume = ZERO_VOLUME
                    a_dropped.append(a_identifier)
                else:
                    a_order_volume = _scheme_engine.encrypt_sub_ciphertext_float(
                        a_order_volume, _minimum_volume
                    )

                logger.log(
                    level=TRADE_LOG_LEVEL,
                    msg=f"{self.__name__} ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume.hex()[:20]}) -> V ({b_order_volume.hex()[:20]}), 'ASK' ({a_identifier}) V ({a_order_c.volume.hex()[:20]}) -> V ({a_order_volume.hex()[:20]}) for P ({a_order_price.to_bytes().hex()[:20]})",
                )

                d_order = self._intermediate_channel.DecryptOrder(
                    order=grpc_buffer_intermediate.CiphertextOrder(
                        type=a_order.type,
                        instrument=instrument,
                        volume=_minimum_volume,
                        price=a_order_price.to_bytes(),
                    ),
                    entity_bid=b_order.entity,
                    entity_ask=a_order.entity,
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
        self, instrument: str, book: OrderBook
    ) -> Tuple[OrderBook, List[str], List[str]]:
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

            if b_order.price < a_order.price:
                logger.info(
                    f"{self.__name__} ({instrument}): No more matches possible ..."
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
        encrypted: Optional[str],
        local_sort: bool,
        compare_iterations: int,
        compare_sigmoid_iterations: int,
        compare_constant_count: int,
        challenge_count: int,
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
                correct_counter: List[bool],
                total_counter: List[bool],
                total_timings: List[float],
            ) -> Union[
                grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
            ]:
                """Compare two encrypted item and return the smallest using the remote intermediate."""
                _, first = first
                _, second = second

                expected, challenges = generate_challenges(
                    engine=_scheme_engine, n=challenge_count
                )
                index = randint(0, len(challenges) - 1)
                _challenges: List[Challenge] = (
                    challenges[:index]
                    + [Challenge(first=x.to_bytes(), second=half)]
                    + challenges[index:]
                )
                start_time = time.time()
                challenges = self._intermediate_channel.GetMinimumValue(
                    challenges=_challenges, instrument=instrument, encoding="float"
                ).challenges
                end_time = time.time()

                results: List[int] = []
                for _index, challenge in enumerate(challenges):
                    if index == _index:
                        continue

                    results.append(
                        -1 if challenge.minimum == _challenges[_index].first else 1
                    )

                if results != expected:
                    logger.error(
                        f"Intermediate returned wrong result for remote compare"
                    )

                result = -1 if challenges[index].minimum == x.to_bytes() else 1

                expected = (
                    -1
                    if _scheme_engine.decrypt_float(first)
                    < _scheme_engine.decrypt_float(second)
                    else 1
                )

                correct_counter.append(result == expected)
                total_counter.append(True)
                total_timings.append(end_time - start_time)
                return _result

            def _local_sort(
                first: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                second: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                _scheme_engine: Union[BFV, CKKS],
                correct_counter: List[bool],
                total_counter: List[bool],
                total_timings: List[float],
            ) -> Union[
                grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
            ]:
                """Compare two encrypted item and return the smallest using the local estimation.
                The identity of max(f, s) follows that:
                max(f, s) = ((f + s) / 2) + (abs(f - s) / 2) = ((f + s) / 2) + (sqrt((f - s)^2) / 2)
                """
                _, first = first
                _, second = second
                first, second = first.price, second.price

                def scale_down(value: PyCtxt, l: float, half: PyPtxt) -> PyCtxt:
                    """Scales down the value to the range [0, 1] using a given value l
                    _a = 0.5 + (a / 2 ** l)
                    """
                    denom = _scheme_engine.encode_float(values=[1 / (2**l)])
                    _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=value, value=denom, to_bytes=False, new_ctxt=False
                    )

                    _scheme_engine.encrypt_add_plain_float(
                        ciphertext=value, value=half, to_bytes=False, new_ctxt=False
                    )

                    return value

                def scale_down2(value: PyCtxt, denom: PyPtxt) -> PyCtxt:
                    """Scales down the value to the range [0, 1]"""
                    _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=value, value=denom, to_bytes=False, new_ctxt=False
                    )
                    _scheme_engine._pyfhel.rescale_to_next(value)
                    value.round_scale()

                    return value

                def compare(
                    first, second, iterations, sigmoid_iterations, constant_count
                ):
                    """Compare two values encrypted homomorphically and return result."""
                    # x <- a - b
                    # for i <- 1 to d do
                    #    x <- func(x)
                    # end for
                    # return (x + 1) / 2

                    a = PyCtxt(serialized=first, pyfhel=_scheme_engine._pyfhel)
                    b = PyCtxt(serialized=second, pyfhel=_scheme_engine._pyfhel)

                    _scheme_engine._depth_map[hash(a)] = 1
                    _scheme_engine._depth_map[hash(b)] = 1

                    def func(x, iterations, constant_index, _pre_calc):
                        _x = {1: PyCtxt(copy_ctxt=x)}
                        _scheme_engine._depth_map[hash(_x[1])] = _scheme_engine._depth_map[hash(x)]
                        _sum = _scheme_engine.encrypt_float(value=0.0, to_bytes=False)

                        for i in range(1, iterations + 1, 2):
                            if i > 1:
                                for _ in range(2):
                                    _scheme_engine.encrypt_mult_ciphertext_float(
                                        ciphertext=_x[i],
                                        value=x,
                                        to_bytes=False,
                                        new_ctxt=False,
                                    )

                            _res = _scheme_engine.encrypt_mult_plain_float(
                                ciphertext=_x[i],
                                value=_pre_calc[i],
                                new_ctxt=True,
                                to_bytes=False,
                            )

                            _scheme_engine.encrypt_add_ciphertext_float(
                                ciphertext=_sum,
                                value=_res,
                                to_bytes=False,
                                new_ctxt=False,
                            )

                            _x[i + 2] = PyCtxt(copy_ctxt=x)
                            _scheme_engine._depth_map[hash(_x[i + 2])] = _scheme_engine._depth_map[hash(x)]

                        return _sum

                    # Depth
                    # a -> 1
                    # b -> 1

                    L = 8
                    half = _scheme_engine.encode_float(values=[0.5])

                    a = scale_down(value=a, l=L, half=half)
                    b = scale_down(value=b, l=L, half=half)

                    # Depth
                    # a -> 2
                    # b -> 2

                    x = _scheme_engine.encrypt_sub_ciphertext_float(
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
                        _pre_calc[i] = _scheme_engine.encode_float(
                            [_pre_calc[i]]
                        )

                    for i in range(iterations):
                        x = func(x, sigmoid_iterations, i, _pre_calc)
                        if i + 1 >= iterations:
                            break

                    one = _scheme_engine.encode_float([1.0])

                    _scheme_engine.encrypt_add_plain_float(
                        ciphertext=x, value=one, to_bytes=False, new_ctxt=False
                    )

                    _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=x, value=half, to_bytes=False, new_ctxt=False
                    )

                    half = _scheme_engine.encrypt_float(0.5)
                    expected, challenges = generate_challenges(
                        engine=_scheme_engine, n=challenge_count
                    )
                    index = randint(0, len(challenges) - 1)
                    _challenges: List[Challenge] = (
                        challenges[:index]
                        + [Challenge(first=x.to_bytes(), second=half)]
                        + challenges[index:]
                    )
                    challenges = self._intermediate_channel.GetMinimumValue(
                        challenges=_challenges, instrument=instrument, encoding="float"
                    ).challenges

                    results: List[int] = []
                    for _index, challenge in enumerate(challenges):
                        if index == _index:
                            continue

                        results.append(
                            -1 if challenge.minimum == _challenges[_index].first else 1
                        )

                    if results != expected:
                        logger.error(f"Intermediate returned wrong result for sorting")

                    result = -1 if challenges[index].minimum == x.to_bytes() else 1

                    logger.debug(
                        f"a = {_scheme_engine.decrypt_float(a)}, b = {_scheme_engine.decrypt_float(b)}, x = {_scheme_engine.decrypt_float(x)}, f = {_scheme_engine.decrypt_float(first)}, s = {_scheme_engine.decrypt_float(second)}"
                    )

                    return result, expected

                start_sort = time.time()
                result, expected = compare(
                    first,
                    second,
                    iterations=compare_iterations,
                    sigmoid_iterations=compare_sigmoid_iterations,
                    constant_count=compare_constant_count,
                )
                end_sort = time.time()

                correct_counter.append(result == expected)
                total_counter.append(True)
                total_timings.append(end_sort - start_sort)
                return result

            if local_sort:
                if encrypted == "bfv":
                    _scheme_engine = BFV(pyfhel=self.pyfhel[instrument])
                elif encrypted == "ckks":
                    _scheme_engine = CKKS(pyfhel=self.pyfhel[instrument])
                else:
                    raise ValueError(
                        f"Unknown cryptographic scheme type provided: '{encrypted}'"
                    )

                correct_counter_bid = []
                total_counter_bid = []
                total_timings_bid = []

                sort_bid_start = time.time()
                book.sort(
                    type=OrderType.BID,
                    func=lambda first, second: _local_sort(
                        first,
                        second,
                        _scheme_engine=_scheme_engine,
                        correct_counter=correct_counter_bid,
                        total_counter=total_counter_bid,
                        total_timings=total_timings_bid,
                    ),
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

                sort_ask_start = time.time()
                book.sort(
                    type=OrderType.ASK,
                    func=lambda first, second: _local_sort(
                        first,
                        second,
                        _scheme_engine=_scheme_engine,
                        correct_counter=correct_counter_ask,
                        total_counter=total_counter_ask,
                        total_timings=total_timings_ask,
                    ),
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
            else:
                correct_counter_bid = []
                total_counter_bid = []
                total_timings_bid = []

                sort_bid_start = time.time()
                book.sort(
                    type=OrderType.BID,
                    func=lambda first, second: _remote_sort(
                        first,
                        second,
                        correct_counter=correct_counter_bid,
                        total_counter=total_counter_bid,
                        total_timings=total_timings_bid,
                    ),
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

                sort_ask_start = time.time()
                book.sort(
                    type=OrderType.ASK,
                    func=lambda first, second: _remote_sort(
                        first,
                        second,
                        correct_counter=correct_counter_ask,
                        total_counter=total_counter_ask,
                        total_timings=total_timings_ask,
                    ),
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
        else:
            sort_bid_start = time.time()
            book.sort(type=OrderType.BID)
            sort_bid_end = time.time()
            correct_counter_bid = len(book._book_bid)
            total_counter_bid = len(book._book_bid)
            total_timings_bid = []

            # Since we can't get the timing pairs for the built-in we estimate it using standard comparison between two items
            for _ in range(len(book._book_bid)):
                start_time = time.time()
                0.4 < 0.41
                end_time = time.time()
                total_timings_bid.append(end_time - start_time)

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

            sort_ask_start = time.time()
            book.sort(type=OrderType.ASK)
            sort_ask_end = time.time()
            correct_counter_ask = len(book._book_ask)
            total_counter_ask = len(book._book_ask)
            total_timings_ask = []

            # Since we can't get the timing pairs for the built-in we estimate it using standard comparison between two items
            for _ in range(len(book._book_ask)):
                start_time = time.time()
                0.4 < 0.41
                end_time = time.time()
                total_timings_ask.append(end_time - start_time)

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

        if encrypted is None:
            return self._match(instrument=instrument, book=book)
        elif encrypted in ("bfv", "ckks"):
            return self._match_encrypted(
                instrument=instrument,
                book=book,
                scheme=encrypted,
                challenge_count=challenge_count,
            )
        else:
            raise ValueError(
                f"Unknown cryptographic scheme type provided: '{encrypted}'"
            )

    def match(
        self,
        encrypted: Optional[str],
        local_sort: bool,
        compare_iterations: int,
        compare_constant_count: int,
        compare_sigmoid_iterations: int,
        challenge_count: int,
    ) -> NoReturn:
        """Continously match incoming orders against each other
        Runs the sub matchers _match and _match_plaintext depending on if the orders are encrypted
        """
        with ThreadPoolExecutor(max_workers=10) as pool:
            while True:
                try:
                    future_to_match: Dict[Future, str] = {}
                    # Shallow copy of book to prevent modifications to the top-level book dictionary while iterating
                    _book = copy(self.book)
                    for instrument, book in _book.items():
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
                                    local_sort=local_sort,
                                    compare_iterations=compare_iterations,
                                    compare_constant_count=compare_constant_count,
                                    compare_sigmoid_iterations=compare_sigmoid_iterations,
                                    challenge_count=challenge_count,
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
                        f"{self.__name__} (Global): Encountered error during matching: {e}"
                    )
                    import traceback

                    print(traceback.format_exc())
                    raise e from None
