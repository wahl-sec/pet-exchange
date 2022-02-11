#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer_exchange
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
import pet_exchange.proto.intermediate_pb2_grpc as grpc_services_intermediate

from pet_exchange.utils.logging import route_logger


class ExchangeClient:
    __name__ = "Exchange-Client"

    def __init__(self, listen_addr: str, channel: grpc.Channel):
        self.listen_addr = listen_addr
        self.channel = channel

        self.stub = grpc_services_intermediate.IntermediateProtoStub(self.channel)

    @route_logger(grpc_buffer_intermediate.KeyGenReply)
    async def GetPublicKey(
        self,
        instrument: str,
        request: grpc_buffer_exchange.GetPublicKeyRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer_intermediate.KeyGenReply:
        return await self.stub.KeyGen(
            grpc_buffer_intermediate.KeyGenRequest(instrument=instrument)
        )
