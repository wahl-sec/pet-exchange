#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, List, Optional
from pathlib import Path
import concurrent.futures
import logging
import json
import time

from Pyfhel import Pyfhel
import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import OrderType, ExchangeOrderType
from pet_exchange.engine.matcher import MatchingEngine
from pet_exchange.common.utils import MAX_GRPC_MESSAGE_LENGTH
from pet_exchange.common.crypto import CKKS, CKKS_PARAMETERS
from pet_exchange.exchange.client import ExchangeClient
from pet_exchange.exchange.book import EncryptedOrderBook, OrderBook
from pet_exchange.utils.logging import route_logger

logger = logging.getLogger("__main__")

# TODO: Parameter validation for the incoming requests


class ExchangeServer(grpc_services.ExchangeProtoServicer):
    __name__ = "Exchange-Server"

    def __init__(
        self,
        listen_addr: str,
        intermediate_host: str,
        intermediate_port: int,
        matcher: MatchingEngine,
        instruments: List[str],
        compress: Optional[int] = None,
        precision: Optional[int] = None,
    ):
        self.listen_addr = listen_addr
        self._intermediate_host, self._intermediate_port = (
            intermediate_host,
            intermediate_port,
        )
        self._instruments = instruments
        self._matcher = matcher
        self._compress = compress
        self._precision = precision

        self._intermediate_channel = ExchangeClient(
            listen_addr=self.listen_addr,
            channel=grpc.aio.insecure_channel(
                f"{self._intermediate_host}:{self._intermediate_port}",
                options=[
                    ("grpc.max_send_message_length", MAX_GRPC_MESSAGE_LENGTH),
                    ("grpc.max_receive_message_length", MAX_GRPC_MESSAGE_LENGTH),
                ],
            ),
        )

        # Synchronous channel used by the matcher.
        self._matcher._intermediate_channel = ExchangeClient(
            listen_addr=self.listen_addr,
            channel=grpc.insecure_channel(
                f"{self._intermediate_host}:{self._intermediate_port}",
                options=[
                    ("grpc.max_send_message_length", MAX_GRPC_MESSAGE_LENGTH),
                    ("grpc.max_receive_message_length", MAX_GRPC_MESSAGE_LENGTH),
                ],
            ),
        )
        super(grpc_services.ExchangeProtoServicer).__init__()

    @route_logger(grpc_buffer.GetPublicKeyReply)
    async def GetPublicKey(
        self,
        request: grpc_buffer.GetPublicKeyRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.GetPublicKeyReply:
        if request.instrument in self._matcher.keys:
            return grpc_buffer.GetPublicKeyReply(
                public=self._matcher.keys[request.instrument]
            )

        _key = await self._intermediate_channel.GetPublicKey(
            instrument=request.instrument,
            request=request,
            context=context,
        )

        start_time = time.time()
        # TODO: This needs to change if we want to be able to key-switch
        if request.instrument not in self._matcher.pyfhel:
            _pyfhel = Pyfhel()
            _pyfhel.contextGen(scheme="CKKS", **CKKS_PARAMETERS)

            _pyfhel.from_bytes_context(_key.context)
            _pyfhel.from_bytes_public_key(_key.public)
            # _pyfhel.from_bytes_secret_key(_key.secret)
            _pyfhel.from_bytes_relin_key(_key.relin)

            self._matcher.keys[request.instrument] = _key.public
            self._matcher.relin_keys[request.instrument] = _key.relin
            self._matcher.pyfhel[request.instrument] = _pyfhel
            self._matcher.crypto[request.instrument] = CKKS(
                pyfhel=_pyfhel, compress=self._compress
            )

        return grpc_buffer.GetPublicKeyReply(
            public=_key.public, duration=_key.duration + (time.time() - start_time)
        )

    @route_logger(grpc_buffer.AddOrderLimitReply)
    async def AddOrderLimit(
        self,
        request: grpc_buffer.AddOrderLimitRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderLimitReply:
        start_time = time.time()
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            EncryptedOrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.LIMIT,
            ),
        )

        return grpc_buffer.AddOrderLimitReply(
            uuid=_order_book.add(order=request.order, matcher=self._matcher),
            duration=time.time() - start_time,
        )

    @route_logger(grpc_buffer.AddOrderLimitPlainReply)
    async def AddOrderLimitPlain(
        self,
        request: grpc_buffer.AddOrderLimitPlainRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderLimitPlainReply:
        start_time = time.time()
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            OrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.LIMIT,
            ),
        )

        return grpc_buffer.AddOrderLimitPlainReply(
            uuid=_order_book.add(order=request.order, matcher=self._matcher),
            duration=time.time() - start_time,
        )

    @route_logger(grpc_buffer.AddOrderMarketReply)
    async def AddOrderMarket(
        self,
        request: grpc_buffer.AddOrderMarketRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderMarketReply:
        start_time = time.time()
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            EncryptedOrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.MARKET,
            ),
        )

        return grpc_buffer.AddOrderMarketReply(
            uuid=_order_book.add(order=request.order, matcher=self._matcher),
            duration=time.time() - start_time,
        )

    @route_logger(grpc_buffer.AddOrderMarketPlainReply)
    async def AddOrderMarketPlain(
        self,
        request: grpc_buffer.AddOrderMarketPlainRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderMarketPlainReply:
        start_time = time.time()
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            OrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.MARKET,
            ),
        )

        return grpc_buffer.AddOrderMarketPlainReply(
            uuid=_order_book.add(order=request.order, matcher=self._matcher),
            duration=time.time() - start_time,
        )


def _start_matcher(
    matcher: MatchingEngine,
    encrypted: Optional[str],
    local_sort: bool,
    compare_iterations: int,
    compare_constant_count: int,
    compare_sigmoid_iterations: int,
    challenge_count: int,
    delay_start: Optional[int],
) -> NoReturn:
    if delay_start is not None:
        logger.info(f"Delaying start of matcher with: '{delay_start}' seconds ...")
        start_time = time.time()
        while time.time() - start_time <= delay_start:
            pass

    matcher.match(
        encrypted=encrypted,
        local_sort=local_sort,
    )


async def serve(
    exchange_host: str,
    exchange_port: int,
    intermediate_host: str,
    intermediate_port: int,
    encrypted: Optional[str],
    exchange_output: str,
    instruments: List[str],
    local_sort: bool,
    compare_iterations: int,
    compare_constant_count: int,
    compare_sigmoid_iterations: int,
    challenge_count: int,
    time_limit: Optional[int],
    delay_start: Optional[int],
    compress: Optional[int],
    precision: Optional[int],
) -> NoReturn:
    server = grpc.aio.server(
        options=[
            ("grpc.max_send_message_length", MAX_GRPC_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_GRPC_MESSAGE_LENGTH),
        ]
    )
    listen_addr = f"{exchange_host}:{exchange_port}"

    if exchange_output is not None:
        _path = Path(exchange_output)
        with _path.open(mode="w+") as _file:
            _file.write(json.dumps({}))  # Clears the file

    matcher = MatchingEngine(
        output=exchange_output,
        instruments=instruments,
        local_compare=local_sort,
        encrypted=encrypted,
        time_limit=time_limit,
        compare_iterations=compare_iterations,
        compare_constant_count=compare_constant_count,
        compare_sigmoid_iterations=compare_sigmoid_iterations,
        challenge_count=challenge_count,
    )

    # TODO: Make this variable maybe, like multiple matchers for different books
    #       Alternatively we can make it so that the matcher creates another layer of threads to handle different books.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:

        exchange = ExchangeServer(
            listen_addr=listen_addr,
            intermediate_host=intermediate_host,
            intermediate_port=intermediate_port,
            matcher=matcher,
            instruments=instruments,
            compress=compress,
            precision=precision,
        )

        # Runs on the main child-process thread
        logger.info(
            f"Exchange-Server ({listen_addr}): Listening for incoming messages ..."
        )
        grpc_services.add_ExchangeProtoServicer_to_server(
            exchange,
            server,
        )
        server.add_insecure_port(listen_addr)
        await server.start()

        # Runs in the spawned child-thread
        logger.info(f"{matcher.__name__} ({listen_addr}): Waiting for orders ...")
        pool.submit(
            _start_matcher,
            matcher=matcher,
            encrypted=encrypted,
            local_sort=local_sort,
            compare_iterations=compare_iterations,
            compare_constant_count=compare_constant_count,
            compare_sigmoid_iterations=compare_sigmoid_iterations,
            challenge_count=challenge_count,
            delay_start=delay_start,
        )

        await server.wait_for_termination()


if __name__ == "__main__":
    pass  # TODO: Implement argparse for each individual component to ease up running them separately, collect the arguments to this group together with the ones in __main__
