#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn
import logging

import grpc
import pet_exchange.proto.intermediate_pb2 as grpc_buffer
import pet_exchange.proto.intermediate_pb2_grpc as grpc_services

from pet_exchange.utils.logging import route_logger, route_logger_sync
from pet_exchange.intermediate.keys import KeyEngine

logger = logging.getLogger("__main__")

# TODO: Logging decorator to log the incoming requests
# TODO: Parameter validation for the incoming requests


class IntermediateServer(grpc_services.IntermediateProtoServicer):
    __name__ = "Intermediate-Server"

    def __init__(self, listen_addr: str, exchange_host: str, exchange_port: int):
        self.listen_addr = listen_addr
        self._exchange_host, self._exchange_port = (exchange_host, exchange_port)

        self._exchange_channel = grpc.aio.insecure_channel(
            f"{self._exchange_host}:{self._exchange_port}"
        )
        self._key_engine = KeyEngine()
        super(grpc_services.IntermediateProtoServicer).__init__()

    @route_logger(grpc_buffer.KeyGenReply)
    async def KeyGen(
        self, request: grpc_buffer.KeyGenRequest, context: grpc.aio.ServicerContext
    ) -> grpc_buffer.KeyGenReply:
        handler = self._key_engine.generate_key_handler(instrument=request.instrument)
        return grpc_buffer.KeyGenReply(
            public=handler.key_pair.public, context=handler.context
        )

    @route_logger(grpc_buffer.EncryptOrderReply)
    async def EncryptOrder(
        self,
        request: grpc_buffer.EncryptOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.EncryptOrderReply:
        handler = self._key_engine.key_handler(instrument=request.order.instrument)
        return grpc_buffer.EncryptOrderReply(
            order=handler.encrypt(plaintext=request.order),
        )

    @route_logger(grpc_buffer.EncryptOrderBookReply)
    async def EncryptOrderBook(
        self,
        request: grpc_buffer.EncryptOrderBookRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.EncryptOrderBookReply:
        handler = self._key_engine.key_handler(instrument=request.order.instrument)
        return grpc_buffer.EncryptOrderBookReply(
            book=handler.encrypt_book(plaintexts=request.book)
        )

    @route_logger(grpc_buffer.DecryptOrderReply)
    async def DecryptOrder(
        self,
        request: grpc_buffer.DecryptOrderRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.DecryptOrderReply:
        handler = self._key_engine.key_handler(instrument=request.order.instrument)

        return grpc_buffer.DecryptOrderReply(
            order=handler.decrypt(ciphertext=request.order),
            entity_bid="".join(
                chr(ch)
                for ch in handler.decrypt_array(ciphertext=request.entity_bid)
                if ch
            )
            if hasattr(request, "entity_bid")
            else None,
            entity_ask="".join(
                chr(ch)
                for ch in handler.decrypt_array(ciphertext=request.entity_ask)
                if ch  # This will skip 0 (terminator) values, maybe we need to have a better solution?
            )
            if hasattr(request, "entity_ask")
            else None,
        )

    @route_logger(grpc_buffer.DecryptOrderBookReply)
    async def DecryptOrderBook(
        self,
        request: grpc_buffer.DecryptOrderBookRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer.DecryptOrderBookReply:
        handler = self._key_engine.key_handler(instrument=request.order.instrument)
        return grpc_buffer.DecryptOrderBookReply(
            book=handler.decrypt_book(ciphertexts=request.book)
        )

    @route_logger_sync(grpc_buffer.GetMinimumValueReply)
    def GetMinimumValueInt(
        self, request: grpc_buffer.GetMinimumValueRequest, context: grpc.ServicerContext
    ) -> grpc_buffer.GetMinimumValueReply:
        handler = self._key_engine.key_handler(instrument=request.instrument)
        if handler.decrypt_int(request.first) < handler.decrypt_int(request.second):
            return grpc_buffer.GetMinimumValueReply(minimum=request.first)
        else:
            return grpc_buffer.GetMinimumValueReply(minimum=request.second)

    @route_logger_sync(grpc_buffer.GetMinimumValueReply)
    def GetMinimumValueFloat(
        self, request: grpc_buffer.GetMinimumValueRequest, context: grpc.ServicerContext
    ) -> grpc_buffer.GetMinimumValueReply:
        handler = self._key_engine.key_handler(instrument=request.instrument)
        if handler.decrypt_float(request.first) < handler.decrypt_float(request.second):
            return grpc_buffer.GetMinimumValueReply(minimum=request.first)
        else:
            return grpc_buffer.GetMinimumValueReply(minimum=request.second)


async def serve(
    intermediate_host: str,
    intermediate_port: int,
    exchange_host: str,
    exchange_port: int,
    encrypted: bool,
) -> NoReturn:
    server = grpc.aio.server()

    listen_addr = f"{intermediate_host}:{intermediate_port}"
    grpc_services.add_IntermediateProtoServicer_to_server(
        IntermediateServer(
            listen_addr=listen_addr,
            exchange_host=exchange_host,
            exchange_port=exchange_port,
        ),
        server,
    )
    server.add_insecure_port(listen_addr)
    logger.info(
        f"Intermediate-Server ({listen_addr}): Listening for incoming messages ..."
    )

    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    pass  # TODO: Implement argparse for each individual component to ease up running them separately, collect the arguments to this group together with the ones in __main__
