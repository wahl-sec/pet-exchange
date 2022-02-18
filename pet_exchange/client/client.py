#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple, Union, List, Dict, Any, NoReturn, Optional
from datetime import datetime
from pathlib import Path
import logging
import time
import json

logger = logging.getLogger("__main__")

from Pyfhel import Pyfhel

import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import ExchangeOrderType
from pet_exchange.common.crypto import encrypt_frac, encrypt_int


def _write_output(output: str, client: str, orders: Dict[str, Any]) -> NoReturn:
    """Writes the output of the current book for a certain instrument to a JSON file"""
    _path = Path(output)
    _book = {}

    try:
        if _path.exists():
            with _path.open(mode="r+") as _file:
                _book = json.load(_file)
                if client not in _book["CLIENTS"]:
                    _book["CLIENTS"][client] = {}

                _book["CLIENTS"][client].update(orders)

        with _path.open(mode="w+") as _file:
            _file.write(json.dumps(_book))
    except Exception as e:
        logger.error(
            f"Client '{client}': Failed to write output to: '{output}' due to: {e}"
        )


async def client(
    exchange: Tuple[str, int],
    client: str,
    orders: List[Dict[str, Any]],
    encrypted: bool,
    client_output: str,
    exchange_order_type: ExchangeOrderType,
    _start: Optional[int] = None,
) -> NoReturn:
    host, port = exchange
    listen_addr = f"{host}:{port}"
    logger.info(f"Client '{client}': Starting ...")
    keys: Dict[str, Pyfhel] = {}
    placed_orders: Dict[str, Dict[str, Any]] = {}

    if client_output is not None:
        _path = Path(client_output)
        with _path.open(mode="w+") as _file:
            _file.write(json.dumps({"CLIENTS": {}}))  # Clears the file

    # TODO: Fetch the public key and encrypt before sending to exchange, maybe we could implement a non-crypto exchange to benchmark against, just need to implement non-encrypted variants for all messages and check message type
    async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
        logger.info(f"Client '{client}': Waiting for exchange to wake up ...")
        await channel.channel_ready()
        stub = grpc_services.ExchangeProtoStub(channel)

        _message: Union[stub.AddOrder, stub.AddOrderPlain] = (
            (
                stub.AddOrderLimit
                if exchange_order_type == ExchangeOrderType.LIMIT
                else stub.AddOrderMarket
            )
            if encrypted
            else (
                stub.AddOrderLimitPlain
                if exchange_order_type == ExchangeOrderType.LIMIT
                else stub.AddOrderMarketPlain
            )
        )

        _request: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ] = (
            (
                grpc_buffer.AddOrderLimitRequest
                if exchange_order_type == ExchangeOrderType.LIMIT
                else grpc_buffer.AddOrderMarketRequest
            )
            if encrypted
            else (
                grpc_buffer.AddOrderLimitPlainRequest
                if exchange_order_type == ExchangeOrderType.LIMIT
                else grpc_buffer.AddOrderMarketPlainRequest
            )
        )

        _order: Union[
            grpc_buffer.CiphertextLimitOrder,
            grpc_buffer.PlaintextLimitOrder,
            grpc_buffer.CiphertextMarketOrder,
            grpc_buffer.PlaintextMarketOrder,
        ] = (
            (
                grpc_buffer.CiphertextLimitOrder
                if exchange_order_type == ExchangeOrderType.LIMIT
                else grpc_buffer.CiphertextMarketOrder
            )
            if encrypted
            else (
                grpc_buffer.PlaintextLimitOrder
                if exchange_order_type == ExchangeOrderType.LIMIT
                else grpc_buffer.PlaintextMarketOrder
            )
        )

        for order in orders:
            if encrypted and order["instrument"] not in keys:
                _key = await stub.GetPublicKey(
                    grpc_buffer.GetPublicKeyRequest(instrument=order["instrument"])
                )
                _pyfhel = Pyfhel()
                _pyfhel.from_bytes_context(_key.context)
                _pyfhel.from_bytes_publicKey(_key.public)
                keys[order["instrument"]] = _pyfhel

            if encrypted:
                _processed_order = _order(
                    **{
                        "instrument": order["instrument"],
                        "type": order["type"],
                        "volume": encrypt_int(
                            value=order["volume"], pyfhel=keys[order["instrument"]]
                        ),
                    },
                    **(
                        {
                            "price": encrypt_frac(
                                value=order["price"], pyfhel=keys[order["instrument"]]
                            )
                        }
                        if exchange_order_type == ExchangeOrderType.LIMIT
                        else {}
                    ),
                )
            else:
                _processed_order = _order(
                    **{
                        "instrument": order["instrument"],
                        "type": order["type"],
                        "volume": order["volume"],
                    },
                    **(
                        {
                            "price": order["price"],
                        }
                        if exchange_order_type == ExchangeOrderType.LIMIT
                        else {}
                    ),
                )

            while (
                _start is not None
                and order.get("offset")
                and time.time() - _start <= float(order.get("offset"))
            ):
                pass
            else:
                if _start is not None:
                    _start = time.time()

            response = await _message(_request(order=_processed_order))
            logger.debug(
                f"Client ({client}): Order for instrument: '{order['instrument']}' sent with identifier: '{response.uuid}'"
            )
            placed_orders[response.uuid] = {
                "client": client,
                "placed": datetime.now().strftime("%d/%m/%y %H:%M:%S.%f"),
                "instrument": order["instrument"],
                "type": order["type"],
                "volume": order["volume"],
                **(
                    {"price": order["price"]}
                    if exchange_order_type == ExchangeOrderType.LIMIT
                    else {}
                ),
            }

        if client_output is not None:
            _write_output(client_output, client, placed_orders)
