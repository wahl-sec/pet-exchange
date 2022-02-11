#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Dict, Union
from pathlib import Path
import concurrent.futures
import asyncio
import logging
import json

import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import OrderType, ExchangeOrderType
from pet_exchange.exchange.matcher import ExchangeMatcher
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
        matcher: ExchangeMatcher,
    ):
        self.listen_addr = listen_addr
        self._intermediate_host, self._intermediate_port = (
            intermediate_host,
            intermediate_port,
        )
        self._matcher = matcher

        self._intermediate_channel = ExchangeClient(
            listen_addr=self.listen_addr,
            channel=grpc.aio.insecure_channel(
                f"{self._intermediate_host}:{self._intermediate_port}"
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
            instrument=request.instrument, request=request, context=context
        )
        self._matcher.keys[request.instrument] = _key.public

        return grpc_buffer.GetPublicKeyReply(public=_key.public, context=_key.context)

    @route_logger(grpc_buffer.AddOrderLimitReply)
    async def AddOrderLimit(
        self,
        request: grpc_buffer.AddOrderLimitRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderLimitReply:
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            EncryptedOrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.LIMIT,
            ),
        )

        return grpc_buffer.AddOrderLimitReply(uuid=_order_book.add(order=request.order))

    @route_logger(grpc_buffer.AddOrderLimitPlainReply)
    async def AddOrderLimitPlain(
        self,
        request: grpc_buffer.AddOrderLimitPlainRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderLimitPlainReply:
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            OrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.LIMIT,
            ),
        )

        return grpc_buffer.AddOrderLimitPlainReply(
            uuid=_order_book.add(order=request.order)
        )

    @route_logger(grpc_buffer.AddOrderMarketReply)
    async def AddOrderMarket(
        self,
        request: grpc_buffer.AddOrderMarketRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderMarketReply:
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            EncryptedOrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.MARKET,
            ),
        )

        return grpc_buffer.AddOrderMarketReply(
            uuid=_order_book.add(order=request.order)
        )

    @route_logger(grpc_buffer.AddOrderMarketPlainReply)
    async def AddOrderMarketPlain(
        self,
        request: grpc_buffer.AddOrderMarketPlainRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.AddOrderMarketPlainReply:
        _order_book = self._matcher.book.setdefault(
            request.order.instrument,
            OrderBook(
                instrument=request.order.instrument,
                exchange_order_type=ExchangeOrderType.MARKET,
            ),
        )

        return grpc_buffer.AddOrderMarketPlainReply(
            uuid=_order_book.add(order=request.order)
        )


def _start_matcher(matcher, encrypted: bool) -> NoReturn:
    matcher.match(encrypted=encrypted)


async def serve(
    exchange_host: str,
    exchange_port: int,
    intermediate_host: str,
    intermediate_port: int,
    encrypted: bool,
    exchange_output: str,
) -> NoReturn:
    server = grpc.aio.server()
    listen_addr = f"{exchange_host}:{exchange_port}"

    if exchange_output is not None:
        _path = Path(exchange_output)
        with _path.open(mode="w+") as _file:
            _file.write(json.dumps({}))  # Clears the file

    matcher = ExchangeMatcher(output=exchange_output)

    # TODO: Make this variable maybe, like multiple matchers for different books
    #       Alternatively we can make it so that the matcher creates another layer of threads to handle different books.
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        logger.info(f"Exchange-Matcher ({listen_addr}): Waiting for orders ...")
        pool.submit(_start_matcher, matcher=matcher, encrypted=encrypted)

        # Runs on the main child-process
        logger.info(
            f"Exchange-Server ({listen_addr}): Listening for incoming messages ..."
        )

        exchange = ExchangeServer(
            listen_addr=listen_addr,
            intermediate_host=intermediate_host,
            intermediate_port=intermediate_port,
            matcher=matcher,
        )

        grpc_services.add_ExchangeProtoServicer_to_server(
            exchange,
            server,
        )
        server.add_insecure_port(
            listen_addr
        )  # TODO: Kolla om hur matcher/boken ska delas mellan processerna

        await server.start()
        await server.wait_for_termination()


if __name__ == "__main__":
    pass  # TODO: Implement argparse for each individual component to ease up running them separately, collect the arguments to this group together with the ones in __main__