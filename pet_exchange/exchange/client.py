#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Optional

import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer_exchange
import pet_exchange.proto.intermediate_pb2 as grpc_buffer_intermediate
import pet_exchange.proto.intermediate_pb2_grpc as grpc_services_intermediate

from pet_exchange.common.types import CiphertextOrder
from pet_exchange.utils.logging import route_logger


class ExchangeClient:
    __name__ = "Exchange-Client"

    def __init__(self, listen_addr: str, channel: Optional[grpc.Channel]):
        self.listen_addr = listen_addr
        self.channel = channel

        self.timings = {}  # TODO: Implement this

        if self.channel is not None:
            self.stub = grpc_services_intermediate.IntermediateProtoStub(self.channel)

    # @route_logger(grpc_buffer_intermediate.KeyGenReply)
    async def GetPublicKey(
        self,
        instrument: str,
        request: grpc_buffer_exchange.GetPublicKeyRequest,
        context: grpc.aio.ServicerContext,
    ) -> grpc_buffer_intermediate.KeyGenReply:
        return await self.stub.KeyGen(
            grpc_buffer_intermediate.KeyGenRequest(instrument=instrument)
        )

    # @route_logger(grpc_buffer_intermediate.GetMinimumValueReply)
    def GetMinimumValue(
        self, challenges, instrument, encoding
    ) -> grpc_buffer_intermediate.GetMinimumValueReply:
        return self.stub.GetMinimumValue(
            grpc_buffer_intermediate.GetMinimumValueRequest(
                challenges=challenges, instrument=instrument
            )
        )

    def GetMinimumValuePlain(
        self, challenges, instrument, encoding
    ) -> grpc_buffer_intermediate.GetMinimumValuePlainReply:
        return self.stub.GetMinimumValuePlain(
            grpc_buffer_intermediate.GetMinimumValuePlainRequest(
                challenges=challenges, instrument=instrument
            )
        )

    def DecryptOrder(
        self,
        order: CiphertextOrder,
        entity_bid: Optional[bytes] = None,
        entity_ask: Optional[bytes] = None,
    ) -> grpc_buffer_intermediate.DecryptOrderReply:
        return self.stub.DecryptOrder(
            grpc_buffer_intermediate.DecryptOrderRequest(
                order=order, entity_bid=entity_bid, entity_ask=entity_ask
            )
        )
