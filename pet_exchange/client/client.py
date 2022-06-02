#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Tuple, Union, List, Dict, Any, NoReturn, Optional
from datetime import datetime
from pathlib import Path
import logging
import time
import json
import sys

logger = logging.getLogger("__main__")

from Pyfhel import Pyfhel

import grpc
import pet_exchange.proto.exchange_pb2 as grpc_buffer
import pet_exchange.proto.exchange_pb2_grpc as grpc_services

from pet_exchange.exchange import ExchangeOrderType
from pet_exchange.common.utils import MAX_GRPC_MESSAGE_LENGTH
from pet_exchange.common.crypto import CKKS_PARAMETERS, CKKS


def _write_output(
    output: str,
    client: str,
    orders: Dict[str, Dict[str, Any]],
    metrics: Dict[str, List[float]],
) -> NoReturn:
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

                if client not in _book["METRICS"]:
                    _book["METRICS"][client] = {}

                _book["METRICS"][client].update(metrics)

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
    encrypted: Optional[str],
    client_output: str,
    exchange_order_type: ExchangeOrderType,
    _start: Optional[int] = None,
    static_offset: Optional[float] = None,
    run_forever: bool = False,
    encrypt_entity: bool = False,
    compress: Optional[int] = None,
    precision: Optional[int] = None,
) -> NoReturn:
    host, port = exchange
    listen_addr = f"{host}:{port}"
    logger.info(f"Client '{client}': Starting ...")
    keys: Dict[str, Pyfhel] = {}
    placed_orders: Dict[str, Dict[str, Any]] = {}
    metrics: Dict[str, List[float]] = {
        "MAX_TIME_TO_GET_PUBLIC_KEY": None,
        "MIN_TIME_TO_GET_PUBLIC_KEY": None,
        "AVERAGE_TIME_TO_GET_PUBLIC_KEY": None,
        "TIME_TO_GET_PUBLIC_KEY": [],
        "TIME_TO_GET_PUBLIC_KEY_NET": [],
        "MAX_TIME_TO_PROCESS_ORDER": None,
        "MIN_TIME_TO_PROCESS_ORDER": None,
        "AVERAGE_TIME_TO_PROCESS_ORDER": None,
        "TIME_TO_PROCESS_ORDER": [],
        "MAX_TIME_TO_SEND_ORDER": None,
        "MIN_TIME_TO_SEND_ORDER": None,
        "AVERAGE_TIME_TO_SEND_ORDER": None,
        "TIME_TO_SEND_ORDER": [],
        "TIME_TO_SEND_ORDER_NET": [],
        "AVERAGE_SIZE_OF_ORDER": None,
        "MAX_SIZE_OF_ORDER": None,
        "MIN_SIZE_OF_ORDER": None,
        "SIZE_OF_ORDER": [],
    }

    if client_output is not None:
        _path = Path(client_output)
        with _path.open(mode="w+") as _file:
            _file.write(json.dumps({"CLIENTS": {}, "METRICS": {}}))  # Clears the file

    # TODO: Fetch the public key and encrypt before sending to exchange, maybe we could implement a non-crypto exchange to benchmark against, just need to implement non-encrypted variants for all messages and check message type
    async with grpc.aio.insecure_channel(
        f"{host}:{port}",
        options=[
            ("grpc.max_send_message_length", MAX_GRPC_MESSAGE_LENGTH),
            ("grpc.max_receive_message_length", MAX_GRPC_MESSAGE_LENGTH),
        ],
    ) as channel:
        logger.info(f"Client '{client}': Waiting for exchange to wake up ...")
        start_wait_time = time.time()
        await channel.channel_ready()
        end_wait_time = time.time()

        stub = grpc_services.ExchangeProtoStub(channel)

        _message: Union[stub.AddOrder, stub.AddOrderPlain] = (
            (
                stub.AddOrderLimit
                if exchange_order_type == ExchangeOrderType.LIMIT
                else stub.AddOrderMarket
            )
            if encrypted is not None
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
            if encrypted is not None
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
            if encrypted is not None
            else (
                grpc_buffer.PlaintextLimitOrder
                if exchange_order_type == ExchangeOrderType.LIMIT
                else grpc_buffer.PlaintextMarketOrder
            )
        )

        while True:
            for order in orders:
                crypto: Optional[CKKS] = None

                if encrypted is not None and order["instrument"] not in keys:
                    start_time = time.time()
                    _key = await stub.GetPublicKey(
                        grpc_buffer.GetPublicKeyRequest(instrument=order["instrument"])
                    )
                    end_time = time.time()
                    metrics["TIME_TO_GET_PUBLIC_KEY"].append(end_time - start_time)
                    metrics["TIME_TO_GET_PUBLIC_KEY_NET"].append(
                        end_time - start_time - _key.duration
                    )

                    _pyfhel = Pyfhel()
                    _pyfhel.contextGen(scheme="CKKS", **CKKS_PARAMETERS)
                    _pyfhel.from_bytes_public_key(_key.public)

                    crypto = CKKS(pyfhel=_pyfhel, compress=compress)

                    keys[order["instrument"]] = (_pyfhel, crypto)

                if not isinstance(order["price"], float):
                    order["price"] = float(order["price"])

                if not isinstance(order["volume"], float):
                    order["volume"] = float(order["volume"])

                if precision is not None:
                    _len = int(len(str(order["price"]).split(".")[-1]))
                    if _len > precision:
                        _price = round(order["price"], precision)
                        logger.warning(
                            f"Order price: '{order['price']}' ({_len}) rounded off to '{_price}' ({precision})"
                        )
                        order["price"] = _price

                    _len = int(len(str(order["volume"]).split(".")[-1]))
                    if _len > precision:
                        _volume = round(order["volume"], precision)
                        logger.warning(
                            f"Order volume: '{order['volume']}' ({_len}) rounded off to '{_volume}' ({precision})"
                        )
                        order["volume"] = _volume

                if encrypted is not None:
                    _, crypto = keys[order["instrument"]]
                    start_time = time.time()
                    _processed_order = _order(
                        **{
                            "instrument": order["instrument"],
                            "type": order["type"],
                            "entity": crypto.encrypt_string(client)
                            if encrypt_entity
                            else client.encode("utf-8"),
                            "volume": crypto.encrypt_float(
                                value=float(order["volume"])
                            ),
                        },
                        **(
                            {"price": crypto.encrypt_float(value=float(order["price"]))}
                            if exchange_order_type == ExchangeOrderType.LIMIT
                            else {}
                        ),
                    )
                else:
                    start_time = time.time()
                    _processed_order = _order(
                        **{
                            "instrument": order["instrument"],
                            "type": order["type"],
                            "entity": client,
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

                end_time = time.time()
                # Time to encrypt / process order
                metrics["TIME_TO_PROCESS_ORDER"].append(end_time - start_time)

                while (
                    _start is not None
                    and order.get("offset") is not None
                    and time.time() - _start <= float(order.get("offset"))
                ):
                    pass  # Wait until offset has finished
                else:
                    if _start is not None:
                        _start = time.time()

                metrics["SIZE_OF_ORDER"].append(_processed_order.ByteSize())
                try:
                    start_time = time.time()
                    response = await _message(_request(order=_processed_order))
                    end_time = time.time()

                    metrics["TIME_TO_SEND_ORDER"].append(end_time - start_time)
                    metrics["TIME_TO_SEND_ORDER_NET"].append(
                        end_time - start_time - response.duration
                    )
                except Exception as e:
                    print(e)
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
                if static_offset is not None:
                    # We don't want to sleep the entire thread as multiple clients might share it
                    start_time = time.time()
                    while time.time() - start_time < static_offset:
                        pass

            metrics.update(
                {
                    "EXCHANGE_WAIT_TIME": end_wait_time - start_wait_time,
                    "WAIT_TIMEMAX_TIME_TO_GET_PUBLIC_KEY": max(
                        metrics["TIME_TO_GET_PUBLIC_KEY"]
                    )
                    if metrics["TIME_TO_GET_PUBLIC_KEY"]
                    else None,
                    "MIN_TIME_TO_GET_PUBLIC_KEY": min(metrics["TIME_TO_GET_PUBLIC_KEY"])
                    if metrics["TIME_TO_GET_PUBLIC_KEY"]
                    else None,
                    "AVERAGE_TIME_TO_GET_PUBLIC_KEY": (
                        sum(metrics["TIME_TO_GET_PUBLIC_KEY"])
                        / len(metrics["TIME_TO_GET_PUBLIC_KEY"])
                    )
                    if metrics["TIME_TO_GET_PUBLIC_KEY"]
                    else None,
                    "MAX_TIME_TO_GET_PUBLIC_KEY_NET": max(
                        metrics["TIME_TO_GET_PUBLIC_KEY_NET"]
                    )
                    if metrics["TIME_TO_GET_PUBLIC_KEY_NET"]
                    else None,
                    "MIN_TIME_TO_GET_PUBLIC_KEY_NET": min(
                        metrics["TIME_TO_GET_PUBLIC_KEY_NET"]
                    )
                    if metrics["TIME_TO_GET_PUBLIC_KEY_NET"]
                    else None,
                    "AVERAGE_TIME_TO_GET_PUBLIC_KEY_NET": (
                        sum(metrics["TIME_TO_GET_PUBLIC_KEY_NET"])
                        / len(metrics["TIME_TO_GET_PUBLIC_KEY_NET"])
                    )
                    if metrics["TIME_TO_GET_PUBLIC_KEY_NET"]
                    else None,
                    "MAX_TIME_TO_PROCESS_ORDER": max(metrics["TIME_TO_PROCESS_ORDER"]),
                    "MIN_TIME_TO_PROCESS_ORDER": min(metrics["TIME_TO_PROCESS_ORDER"]),
                    "AVERAGE_TIME_TO_PROCESS_ORDER": sum(
                        metrics["TIME_TO_PROCESS_ORDER"]
                    )
                    / len(metrics["TIME_TO_PROCESS_ORDER"]),
                    "TIME_TO_SEND_ORDER": metrics["TIME_TO_SEND_ORDER"],
                    "MAX_TIME_TO_SEND_ORDER": max(metrics["TIME_TO_SEND_ORDER"]),
                    "MIN_TIME_TO_SEND_ORDER": min(metrics["TIME_TO_SEND_ORDER"]),
                    "AVERAGE_TIME_TO_SEND_ORDER": sum(metrics["TIME_TO_SEND_ORDER"])
                    / len(metrics["TIME_TO_SEND_ORDER"]),
                    "TIME_TO_SEND_ORDER_NET": metrics["TIME_TO_SEND_ORDER_NET"],
                    "MAX_TIME_TO_SEND_ORDER_NET": max(
                        metrics["TIME_TO_SEND_ORDER_NET"]
                    ),
                    "MIN_TIME_TO_SEND_ORDER_NET": min(
                        metrics["TIME_TO_SEND_ORDER_NET"]
                    ),
                    "AVERAGE_TIME_TO_SEND_ORDER_NET": sum(
                        metrics["TIME_TO_SEND_ORDER_NET"]
                    )
                    / len(metrics["TIME_TO_SEND_ORDER_NET"]),
                    "AVERAGE_SIZE_OF_ORDER": sum(metrics["SIZE_OF_ORDER"])
                    / len(metrics["SIZE_OF_ORDER"]),
                    "MAX_SIZE_OF_ORDER": max(metrics["SIZE_OF_ORDER"]),
                    "MIN_SIZE_OF_ORDER": min(metrics["SIZE_OF_ORDER"]),
                }
            )

            if client_output is not None:
                _write_output(client_output, client, placed_orders, metrics)

            if not run_forever:
                break
