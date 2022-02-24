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

    def __init__(self, listen_addr: str, channel: grpc.Channel):
        self.listen_addr = listen_addr
        self.channel = channel

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
        self, first, second, instrument, encoding
    ) -> grpc_buffer_intermediate.GetMinimumValueReply:
        if encoding == "float":
            return self.stub.GetMinimumValueFloat(
                grpc_buffer_intermediate.GetMinimumValueRequest(
                    first=first, second=second, instrument=instrument
                )
            )
        else:
            return self.stub.GetMinimumValueInt(
                grpc_buffer_intermediate.GetMinimumValueRequest(
                    first=first, second=second, instrument=instrument
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
