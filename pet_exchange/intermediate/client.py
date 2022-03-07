#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import NoReturn, Tuple
import asyncio
import logging

import grpc
import proto.intermediate_pb2 as grpc_buffer
import proto.intermediate_pb2_grpc as grpc_services


async def client(host: Tuple[str, int]) -> NoReturn:
    addr, port = host
    async with grpc.aio.insecure_channel(f"{addr}:{port}") as channel:
        stub = grpc_services.IntermediateProtoStub(channel)
        # TODO: Is this used?
        response = await stub.KeyGen(grpc_buffer.KeyGenRequest(instrument="NSDQ"))


if __name__ == "__main__":
    logging.basicConfig()
    asyncio.run(client(("[::]", 50051)))
