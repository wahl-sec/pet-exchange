#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union, Generator, Tuple, List, Optional
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from copy import deepcopy, copy
from datetime import datetime
from pathlib import Path
import logging
import json

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np

import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.common.utils import generate_random_int, generate_random_float
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
                    _book[instrument] = {}

                _book[instrument].update(self.book[instrument]._book_performed)

        with _path.open(mode="w+") as _file:
            _file.write(json.dumps(_book))

    def _match_encrypted(
        self, instrument: str, book: EncryptedOrderBook, scheme: str
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
            b_otp_price, a_otp_price = _scheme_engine.encrypt_add_plain_float(
                b_order.price, _price_pad
            ), _scheme_engine.encrypt_add_plain_float(
                a_order.price,
                _price_pad,
            )

            _minimum_otp_price = self._intermediate_channel.GetMinimumValue(
                first=b_otp_price,
                second=a_otp_price,
                instrument=instrument,
                encoding="float",
            ).minimum

            if b_otp_price == _minimum_otp_price and a_otp_price != _minimum_otp_price:
                logger.info(
                    f"{self.__name__} ({instrument}): No more matches possible ..."
                )
                break
            else:
                _volume_pad = generate_random_int()
                b_otp_volume, a_otp_volume = _scheme_engine.encrypt_add_plain_float(
                    b_order.volume, float(_volume_pad)
                ), _scheme_engine.encrypt_add_plain_float(
                    a_order.volume, float(_volume_pad)
                )

                _minimum_otp_volume = self._intermediate_channel.GetMinimumValue(
                    first=b_otp_volume,
                    second=a_otp_volume,
                    instrument=instrument,
                    encoding="float",
                ).minimum

                _minimum_volume = _scheme_engine.encrypt_sub_plain_float(
                    _minimum_otp_volume,
                    float(_volume_pad),
                )

                b_order_c, a_order_c = deepcopy(b_order), deepcopy(a_order)
                if b_otp_volume == _minimum_otp_volume:
                    b_order.volume = ZERO_VOLUME
                    b_dropped.append(b_identifier)
                else:
                    b_order.volume = _scheme_engine.encrypt_sub_ciphertext_float(
                        b_order.volume, _minimum_volume
                    )

                if a_otp_volume == _minimum_otp_volume:
                    a_order.volume = ZERO_VOLUME
                    a_dropped.append(a_identifier)
                else:
                    a_order.volume = _scheme_engine.encrypt_sub_ciphertext_float(
                        a_order.volume, _minimum_volume
                    )

                logger.log(
                    level=TRADE_LOG_LEVEL,
                    msg=f"{self.__name__} ({instrument}): Trade 'BID' ({b_identifier}) V ({b_order_c.volume.hex()[:20]}) -> V ({b_order.volume.hex()[:20]}), 'ASK' ({a_identifier}) V ({a_order_c.volume.hex()[:20]}) -> V ({a_order.volume.hex()[:20]}) for P ({a_order.price.hex()[:20]})",
                )

                d_order = self._intermediate_channel.DecryptOrder(
                    order=grpc_buffer_intermediate.CiphertextOrder(
                        type=a_order.type,
                        instrument=instrument,
                        volume=_minimum_volume,
                        price=a_order.price,
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
            ) -> Union[
                grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
            ]:
                """Compare two encrypted item and return the smallest using the remote intermediate."""
                _, first = first
                _, second = second
                return (
                    -1
                    if (
                        self._intermediate_channel.GetMinimumValue(
                            first=first.price,
                            second=second.price,
                            instrument=instrument,
                            encoding="float",
                        ).minimum
                        == first.price
                    )
                    else 1
                )

            def _local_sort(
                first: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                second: Union[
                    grpc_buffer.CiphertextLimitOrder, grpc_buffer.CiphertextMarketOrder
                ],
                _scheme_engine: Union[BFV, CKKS],
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

                def inverse_estimation(a: PyCtxt, b: PyCtxt, one: PyPtxt, iterations: int) -> PyCtxt:
                    """Goldschmidt's divison algorithm is used to estimate the inverse of a given value
                    It follows the identity that:
                    (1 / x) = (1 / (1 - (1 - x))) = pSUM_i=0_inf(1 + (1 - x)^(2^i)) ~= pSUM_i=0_d(1 + (1 - x)^(2^i))
                    So for a sufficient d large enough (1 + (1 - x)^(2^i)) converges 1 as 1 -> inf
                    """
                    # a0 <- 2 - x
                    # b0 <- 1 - x
                    # for n <- 0 to d-1 do
                    #     b_n+1 <- b_n^2
                    #     a_n+1 <- a_n * (1 + b_n+1)
                    # end for
                    # return a_d

                    # Depth
                    # a -> 0
                    # b -> 0
                    # one -> 0

                    for index in range(iterations):
                        _scheme_engine.encrypt_square(
                            ciphertext=b,
                            new_ctxt=False,
                            to_bytes=False
                        )
                        _scheme_engine._pyfhel.relinearize(b)
                        _scheme_engine._pyfhel.rescale_to_next(b)
                        b.round_scale()

                        # Depth
                        # a -> 0
                        # b -> 1
                        # one -> 0
                        b_plus_one = _scheme_engine.encrypt_add_plain_float(
                            ciphertext=b,
                            value=one,
                            new_ctxt=True,
                            to_bytes=False
                        )

                        _scheme_engine.encrypt_mult_ciphertext_float(
                            ciphertext=a,
                            value=b_plus_one,
                            new_ctxt=False,
                            to_bytes=False
                        )
                        _scheme_engine._pyfhel.relinearize(a)
                        _scheme_engine._pyfhel.rescale_to_next(a)
                        a.round_scale()

                        # Depth
                        # a -> 1
                        # b -> 1
                        # one -> 0

                        if iterations > 1 and index < (iterations - 1):
                            _scheme_engine._pyfhel.mod_switch_to_next(one)

                        # Depth
                        # a -> 1
                        # b -> 1
                        # one -> (i - 1)

                    # Depth
                    # a -> i
                    # b -> i
                    # one -> (i - 1)

                    return a

                def scale_down(value: PyCtxt, l: float, half: PyPtxt) -> PyCtxt:
                    """Scales down the value to the range [0, 1] using a given value l
                    _a = 0.5 + (a / 2 ** l)
                    """
                    denom = _scheme_engine._pyfhel.encodeFrac(np.array([1 / (2 ** l)]))
                    _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=value,
                        value=denom,
                        to_bytes=False,
                        new_ctxt=False
                    )
                    _scheme_engine._pyfhel.rescale_to_next(value)
                    value.round_scale()

                    _scheme_engine.encrypt_add_plain_float(
                        ciphertext=value,
                        value=half,
                        to_bytes=False,
                        new_ctxt=False
                    )

                    return value

                def compare(
                    first,
                    second,
                    inverse_iterations,
                    inverse_iterations_prim,
                    iterations,
                    approximation_value,
                ):
                    """Compare two values encrypted homomorphically and return result."""
                    # a0 <- (a / 2) * Inv((a + b) / 2, d)
                    # b0 <- (1 - a0)
                    # for n in range(t):
                    #    inv <- Inv((a_n)^m + (b_n)^m, d)
                    #    a_n+1 <- (a_n)^m * inv
                    #    b_n+1 <- 1 - a_n+1
                    # return a_t
                    a = PyCtxt(serialized=first, pyfhel=_scheme_engine._pyfhel)
                    b = PyCtxt(serialized=second, pyfhel=_scheme_engine._pyfhel)

                    # Depth
                    # a -> 1
                    # b -> 1

                    L = 6
                    half = _scheme_engine._pyfhel.encodeFrac(np.array([0.5]))
                    _scheme_engine._pyfhel.mod_switch_to_next(half)
                    a = scale_down(value=a, l=L, half=half)
                    b = scale_down(value=b, l=L, half=half)

                    # Depth
                    # a -> 2
                    # b -> 2
                    # half -> 2

                    one = _scheme_engine._pyfhel.encodeFrac(np.array([1.0]))
                    _scheme_engine._pyfhel.mod_switch_to_next(one)
                    two = _scheme_engine._pyfhel.encodeFrac(np.array([2.0]))
                    _scheme_engine._pyfhel.mod_switch_to_next(two)

                    # Depth
                    # a -> 2
                    # b -> 2
                    # half -> 2
                    # one -> 2
                    # two -> 2

                    a_plus_b = _scheme_engine.encrypt_add_ciphertext_float(
                        ciphertext=a,
                        value=b,
                        new_ctxt=True,
                        to_bytes=False
                    )

                    a_div_two = _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=a,
                        value=half,
                        new_ctxt=True,
                        to_bytes=False
                    )
                    _scheme_engine._pyfhel.relinearize(a_div_two)
                    _scheme_engine._pyfhel.rescale_to_next(a_div_two)
                    a_div_two.round_scale()

                    # Depth
                    # a -> 2
                    # b -> 2
                    # half -> 2
                    # one -> 2
                    # two -> 2
                    # a_plus_b -> 2
                    # a_div_two -> 3

                    a_plus_b_div_two = _scheme_engine.encrypt_mult_plain_float(
                        ciphertext=a_plus_b,
                        value=half,
                        new_ctxt=True,
                        to_bytes=False
                    )
                    _scheme_engine._pyfhel.relinearize(a_plus_b_div_two)
                    _scheme_engine._pyfhel.rescale_to_next(a_plus_b_div_two)
                    a_plus_b_div_two.round_scale()

                    _scheme_engine._pyfhel.mod_switch_to_next(one)
                    _scheme_engine._pyfhel.mod_switch_to_next(two)

                    # Depth
                    # a -> 2
                    # b -> 2
                    # half -> 2
                    # one -> 3
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 3
                    # a_plus_b_div_two -> 3

                    a_plus_b_div_two_neg = _scheme_engine._pyfhel.negate(a_plus_b_div_two, in_new_ctxt=True)
                    _a = _scheme_engine.encrypt_add_plain_float(
                        ciphertext=a_plus_b_div_two_neg,
                        value=two,
                        to_bytes=False,
                        new_ctxt=True
                    )

                    _b = _scheme_engine.encrypt_add_plain_float(
                        ciphertext=a_plus_b_div_two_neg,
                        value=one,
                        to_bytes=False,
                        new_ctxt=True
                    )

                    # Depth
                    # a -> 2
                    # _a -> 3
                    # b -> 2
                    # _b -> 3
                    # half -> 2
                    # one -> 3
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 3
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    _scheme_engine._pyfhel.mod_switch_to_next(one)
                    # for _ in range(inverse_iterations_prim):         B
                    _scheme_engine._pyfhel.mod_switch_to_next(_a)

                    # Depth
                    # a -> 2
                    # _a -> 3 + i'
                    # b -> 2
                    # _b -> 3
                    # half -> 2
                    # one -> 4
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 3
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    inv = inverse_estimation(
                        a=_a, b=_b, one=one,
                        iterations=inverse_iterations_prim
                    )

                    # Depth
                    # a -> 2
                    # _a -> 3 + 2 * i'
                    # b -> 2
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 4 + (i' - 1)
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 3
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    for _ in range(inverse_iterations_prim + 1):
                        _scheme_engine._pyfhel.mod_switch_to_next(a_div_two)  # Mod switch twice to match 'b' parms

                    # Depth
                    # a -> 2
                    # _a -> 3 + 2 * i'
                    # b -> 2
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 4 + (i' - 1)
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    a = _scheme_engine.encrypt_mult_ciphertext_float(
                        ciphertext=a_div_two,
                        value=inv,
                        new_ctxt=True,
                        to_bytes=False
                    )
                    _scheme_engine._pyfhel.relinearize(a)
                    _scheme_engine._pyfhel.rescale_to_next(a)
                    a.round_scale()

                    # Depth
                    # a -> 5 + i'
                    # _a -> 3 + 2 * i'
                    # b -> 2
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 4 + (i' - 1)
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    # one = _scheme_engine._pyfhel.encodeFrac(np.array([1.0]))
                    _scheme_engine._pyfhel.mod_switch_to_next(one)

                    # Depth
                    # a -> 5 + i'
                    # _a -> 3 + 2 * i'
                    # b -> 2
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 4 + i'
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3

                    _scheme_engine._pyfhel.mod_switch_to_next(one)
                    a_neg = _scheme_engine._pyfhel.negate(a, in_new_ctxt=True)
                    b = _scheme_engine.encrypt_add_plain_float(
                        ciphertext=a_neg,
                        value=one,
                        new_ctxt=True,
                        to_bytes=False
                    )

                    # Depth
                    # a -> 5 + i'
                    # _a -> 3 + 2 * i'
                    # b -> 5 + i'
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 5 + i'
                    # two -> 3
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3
                    # a_neg -> 5 + i'

                    for _ in range(3 + inverse_iterations_prim - 1):
                        _scheme_engine._pyfhel.mod_switch_to_next(two)

                    # Depth
                    # a -> 5 + i'
                    # _a -> 3 + 2 * i'
                    # b -> 5 + i'
                    # _b -> 3 + i'
                    # half -> 2
                    # one -> 5 + i'
                    # two -> 5 + i'
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3
                    # a_neg -> 5 + i'

                    for _ in range(iterations):
                        a_pow = PyCtxt(copy_ctxt=a)
                        for _ in range(approximation_value - 1):
                            _scheme_engine.encrypt_mult_ciphertext_float(
                                ciphertext=a_pow,
                                value=a,
                                to_bytes=False,
                                new_ctxt=False
                            )
                            _scheme_engine._pyfhel.relinearize(a_pow)
                            _scheme_engine._pyfhel.rescale_to_next(a_pow)
                            a_pow.round_scale()

                            # Depth
                            # a_pow -> a - 1

                            _scheme_engine._pyfhel.mod_switch_to_next(a)
                            _scheme_engine._pyfhel.mod_switch_to_next(two)

                            # Depth
                            # a -> a - 1
                            # a_pow -> a - 1
                            # two -> a - 1

                        # Depth
                        # a_pow -> a - 1
                        # a -> a - 1
                        # two -> a - 1

                        b_pow = PyCtxt(copy_ctxt=b)
                        for i in range(approximation_value - 1):
                            _scheme_engine.encrypt_mult_ciphertext_float(
                                ciphertext=b_pow,
                                value=b,
                                to_bytes=False,
                                new_ctxt=False
                            )
                            _scheme_engine._pyfhel.relinearize(b_pow)
                            _scheme_engine._pyfhel.rescale_to_next(b_pow)
                            b_pow.round_scale()

                            # Depth
                            # b_pow -> a - 1

                            _scheme_engine._pyfhel.mod_switch_to_next(b)
                            _scheme_engine._pyfhel.mod_switch_to_next(one)

                            # Depth
                            # b -> a - 1
                            # b_pow -> a - 1
                            # one -> a - 1

                        # Depth
                        # a -> a - 1
                        # b -> a - 1
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 1
                        # two -> a - 1

                        a_pow_plus_b_pow = _scheme_engine.encrypt_add_ciphertext_float(
                            ciphertext=a_pow,
                            value=b_pow,
                            new_ctxt=True,
                            to_bytes=False
                        )

                        # Depth
                        # a -> a - 1
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 1
                        # two -> a - 1
                        # a_pow_plus_b_pow -> a - 1

                        a_pow_plus_b_pow_neg = _scheme_engine._pyfhel.negate(a_pow_plus_b_pow, in_new_ctxt=True)
                        a_a = _scheme_engine.encrypt_add_plain_float(
                            ciphertext=a_pow_plus_b_pow_neg,
                            value=two,
                            to_bytes=False,
                            new_ctxt=True
                        )

                        b_b = _scheme_engine.encrypt_add_plain_float(
                            ciphertext=a_pow_plus_b_pow_neg,
                            value=one,
                            to_bytes=False,
                            new_ctxt=True
                        )

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1
                        # b -> a - 1
                        # b_b -> a - 1
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 1
                        # two -> a - 1
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        _scheme_engine._pyfhel.mod_switch_to_next(one)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1
                        # b -> a - 1
                        # b_b -> a - 1
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a
                        # two -> a - 1
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        for _ in range(inverse_iterations):
                            _scheme_engine._pyfhel.mod_switch_to_next(a_a)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1 + i
                        # b -> a - 1
                        # b_b -> a - 1
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a
                        # two -> a - 1
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        for _ in range(inverse_iterations - 1):
                            _scheme_engine._pyfhel.mod_switch_to_next(b_b)
                            _scheme_engine._pyfhel.mod_switch_to_next(one)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1 + i
                        # b -> a - 1
                        # b_b -> a - 2 + i
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 1 + i
                        # two -> a - 1
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        inv = inverse_estimation(a=a_a, b=b_b, one=one, iterations=inverse_iterations)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1 + 2 * i
                        # b -> a - 1
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 2 + 2 * i
                        # two -> a - 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        # for _ in range(inverse_iterations + 1):
                        #    _scheme_engine._pyfhel.mod_switch_to_next(one)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1 + 2 * i
                        # b -> a - 1
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - 1
                        # b_pow -> a - 1
                        # one -> a - 2 + 2 * i
                        # two -> a - 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        for _ in range(2 * inverse_iterations):
                            _scheme_engine._pyfhel.mod_switch_to_next(a_pow)

                        # Depth
                        # a -> a - 1
                        # a_a -> a - 1 + 2 * i
                        # b -> a - 1
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - i + 2 * i
                        # b_pow -> a - 1
                        # one -> a - 2 + 2 * i
                        # two -> a - 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        a = _scheme_engine.encrypt_mult_ciphertext_float(
                            ciphertext=a_pow,
                            value=inv,
                            new_ctxt=True,
                            to_bytes=False
                        )
                        _scheme_engine._pyfhel.relinearize(a)
                        _scheme_engine._pyfhel.rescale_to_next(a)
                        a.round_scale()

                        # Depth
                        # a -> a + 2 * i
                        # a_a -> a - 1 + 2 * i
                        # b -> a - 1
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - 1 + 2 * i
                        # b_pow -> a - 1
                        # one -> a - 2 + 2 * i
                        # two -> a - 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        for _ in range(2):
                            _scheme_engine._pyfhel.mod_switch_to_next(one)

                        # Depth
                        # a -> a + 2 * i
                        # a_a -> a - 1 + 2 * i
                        # b -> a + 1 + i
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - 1 + 2 * i
                        # b_pow -> a - 1
                        # one -> a + 2 * i
                        # two -> a - 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1

                        a_a_neg = _scheme_engine._pyfhel.negate(a, in_new_ctxt=True)
                        b = _scheme_engine.encrypt_add_plain_float(
                            ciphertext=a_a_neg,
                            value=one,
                            new_ctxt=True,
                            to_bytes=False
                        )

                        for _ in range(3):
                            _scheme_engine._pyfhel.mod_switch_to_next(two)

                        # Depth
                        # a -> a + 2 * i
                        # a_a -> a - 1 + 2 * i
                        # b -> a + 1 + i
                        # b_b -> a - 2 + 2 * i
                        # a_pow -> a - 1 + 2 * i
                        # b_pow -> a - 1
                        # one -> a + 2 * i
                        # two -> a + 1
                        # inv -> a - 1 + 2 * i
                        # a_pow_plus_b_pow -> a - 1
                        # a_pow_plus_b_pow_neg -> a - 1
                        # a_a_neg -> a + 2


                    # Depth
                    # a -> 5 + i' + I * (a + 2 * i)
                    # _a -> 3 + 2 * i'
                    # a_a -> I * (a - 1 + 2 * i)
                    # b -> 5 + i' + I * (a + 1 + i)
                    # _b -> 3 + i'
                    # b_b -> I * (a - 2 + 2 * i)
                    # half -> 2
                    # one -> 5 + i' + I * (a + 2 * i)
                    # two -> 5 + i' + I * (a + 1)
                    # a_plus_b -> 2
                    # a_div_two -> 4 + i'
                    # a_plus_b_div_two -> 3
                    # a_plus_b_div_two_neg -> 3
                    # a_neg -> 5 + i'
                    # a_a_neg -> a + 2

                    # 5 + 3 + (2 + 2 * 3) = 16
                    # Our minimal depth is 10 because of the depth of 'a' and 'one'
                    # That means we must have at least 10 + 1 primes, 10 for multiplicate depth and 1 for decryption
                    #
                    # If we want to increase the iterations of say i' to 2 we would get
                    # a multiplicate depth of 5 + 2 + 1 * (2 + 2) = 11 for 'a' meaning we would need 12 primes in total

                    res = _scheme_engine.decrypt_float(a)
                    d_first = _scheme_engine.decrypt_float(first)
                    d_second = _scheme_engine.decrypt_float(second)
                    exp = "<=" if d_first <= d_second else ">="
                    if res > 0.5:
                        level = 30 if exp == ">=" else 50
                        logger.log(level=level, msg=f"{d_first} >= {d_second}, {(d_first, d_second)} ({res}), expected: ({exp})")
                    else:
                        level = 30 if exp == "<=" else 50
                        logger.log(level=level, msg=f"{d_first} <= {d_second}, {(d_first, d_second)} ({res}), expected: ({exp})")
                    return 1 if a == _scheme_engine.encrypt_float(1.0) else -1

                return compare(
                    first,
                    second,
                    inverse_iterations=2,
                    inverse_iterations_prim=1,
                    iterations=1,
                    approximation_value=2 ** 1,
                )

            if local_sort:
                if encrypted == "bfv":
                    _scheme_engine = BFV(pyfhel=self.pyfhel[instrument])
                elif encrypted == "ckks":
                    _scheme_engine = CKKS(pyfhel=self.pyfhel[instrument])
                else:
                    raise ValueError(
                        f"Unknown cryptographic scheme type provided: '{encrypted}'"
                    )

                book.sort(
                    type=OrderType.BID,
                    func=lambda first, second: _local_sort(
                        first, second, _scheme_engine=_scheme_engine
                    ),
                )
                book.sort(
                    type=OrderType.ASK,
                    func=lambda first, second: _local_sort(
                        first, second, _scheme_engine=_scheme_engine
                    ),
                )
            else:
                book.sort(
                    type=OrderType.BID,
                    func=lambda first, second: _remote_sort(first, second),
                )
                book.sort(
                    type=OrderType.ASK,
                    func=lambda first, second: _remote_sort(first, second),
                )
        else:
            book.sort(type=OrderType.BID)
            book.sort(type=OrderType.ASK)

        if encrypted is None:
            return self._match(instrument=instrument, book=book)
        elif encrypted in ("bfv", "ckks"):
            return self._match_encrypted(
                instrument=instrument, book=book, scheme=encrypted
            )
        else:
            raise ValueError(
                f"Unknown cryptographic scheme type provided: '{encrypted}'"
            )

    def match(self, encrypted: Optional[str], local_sort: bool) -> NoReturn:
        """Continously match incoming orders against each other
        Runs the sub matchers _match and _match_plaintext depending on if the orders are encrypted
        """
        # TODO: Number of threads should be based on some estimation of instruments being traded at the same time
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
