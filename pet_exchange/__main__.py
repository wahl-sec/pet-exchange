#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# TODO: Control and run the different components using argparse here, i.e `python pet-exchange -i -a 0.0.0.0 -p 8080` to start the intermediate component on 0.0.0.0:8080
from argparse import ArgumentParser, RawTextHelpFormatter, Namespace
from typing import NoReturn, Callable, Dict, List, Any
from pathlib import Path
import concurrent.futures
import logging.config
import logging
import asyncio
import json
import time
import sys

sys.setrecursionlimit(100000)

from pet_exchange.utils import LOGGER_CONFIG
from pet_exchange.client import client as exchange_client
from pet_exchange.exchange import server as exchange_server
from pet_exchange.exchange import ExchangeOrderType
from pet_exchange.intermediate import server as intermediate_server
from pet_exchange.utils.logging import CustomFormatter, proc_logger


DESCRIPTION = """
Privacy-Enhanced Trading Exchange (PET-Exchange)

An implementation of a cryptographically private trading scheme implemented using homomorphic encryption and padding. The goal of the project is to create a scheme for creating privacy for a trade from creation to execution.

The arguments passed define which components to start on the machine which this program is run on
"""

INIT_LOOKUP = {
    "exchange_server": exchange_server,
    "intermediate_server": intermediate_server,
}

SERVER_VARIABLES = {
    "exchange": [
        "exchange_host",
        "exchange_port",
        "intermediate_host",
        "intermediate_port",
        "plaintext",
        "exchange_output",
    ],
    "intermediate": [
        "intermediate_host",
        "intermediate_port",
        "exchange_host",
        "exchange_port",
        "plaintext",
    ],
}

logger = proc_logger()


def _determine_process_count(args: Namespace) -> int:
    _counter = 0
    for arg in vars(args):
        if arg.endswith("_server") and getattr(args, arg):
            _counter += 1

    return _counter


def _start_server(parameters: Dict[str, Any]) -> NoReturn:
    # TODO: Change the debug to be a parameter of this function or something instead
    name, kwargs = parameters
    asyncio.run(
        INIT_LOOKUP[name].serve(**kwargs),
    )


def _start_client(parameters: Dict[str, Any]) -> NoReturn:
    _name, _kwargs = parameters
    _start = time.time()
    asyncio.run(
        exchange_client.client(
            exchange=(_kwargs["EXCHANGE_HOST"], _kwargs["EXCHANGE_PORT"]),
            client=_name,
            orders=_kwargs["ORDERS"],
            encrypted=_kwargs["ENCRYPTED"],
            client_output=_kwargs["CLIENT_OUTPUT"],
            exchange_order_type=_kwargs["EXCHANGE_ORDER_TYPE"],
            _start=None if not _kwargs["USE_OFFSET"] else _start,
        )
    )


def _resolve_client_information_files(args) -> Dict[str, Dict[str, Any]]:
    _paths: List[Path] = []
    for path in args.client_input_files:
        _path = Path(path)
        if not _path.exists():
            logger.error(
                f"Main: Client information file at: '{path}' does not exist, skipping"
            )
            continue

        _paths.append(_path)

    _total = 0
    _clients: Dict[str, Dict[str, Any]] = {}
    for _path in _paths:
        logger.info(f"Main: Loading orders from: '{str(_path)}' ... ")
        with _path.open(mode="r") as _orders:
            for client, orders in json.load(_orders).items():
                _clients[client] = {
                    "ORDERS": orders,
                    "EXCHANGE_HOST": args.exchange_host,
                    "EXCHANGE_PORT": args.exchange_port,
                    "ENCRYPTED": not args.plaintext,
                    "CLIENT_OUTPUT": args.client_output,
                    "USE_OFFSET": args.client_offset,
                    "EXCHANGE_ORDER_TYPE": args.client_order_type
                    if args.client_order_type
                    else (
                        ExchangeOrderType.LIMIT
                        if "price" in orders[0]
                        else ExchangeOrderType.MARKET
                    ),
                }

                if not args.client_order_type:
                    logger.warning(
                        f"Main: No exchange order type was defined for client: '{client}' from file: '{str(_path)}', auto-detected exchange order type: '{'LIMIT' if 'price' in orders[0] else 'MARKET'}'"
                    )

                logger.info(
                    f"Main: Loaded ({len(orders)}) orders for client: '{client}' from file: '{str(_path)}'"
                )

                _total += len(orders)

    logger.info(f"Main: Loaded ({_total}) orders from all clients to be replayed ...")

    return _clients


async def start(args: Namespace):
    logger.info(
        f"Starting PET-Exchange instance in: {'plaintext' if args.plaintext else 'ciphertext'} mode"
    )
    _count = _determine_process_count(args)
    if _count == 0:
        raise ValueError("Couldn't identify and components to start")

    _clients = _resolve_client_information_files(args)
    try:
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=_count + len(_clients)
        ) as pool:
            _servers = {}
            for arg in vars(args):
                if not arg.endswith("_server") or not getattr(args, arg):
                    continue

                _component, _ = arg.split("_server")

                logger.info(
                    f"Main: Starting '{_component.title()}' component process ..."
                )

                _servers[arg] = {
                    key: getattr(args, key)
                    for key in vars(args)
                    if key in SERVER_VARIABLES[_component] and key != arg
                }

                # Rename to encrypted just for consistency
                _servers[arg]["encrypted"] = not _servers[arg]["plaintext"]
                del _servers[arg]["plaintext"]

            pool.map(
                _start_server,
                ((server, config) for server, config in _servers.items()),
            )

            pool.map(
                _start_client,
                ((client, config) for client, config in _clients.items()),
            )
    except KeyboardInterrupt as exc:
        logger.info("Killing all components ...")
        raise exc from None


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange",
        description=DESCRIPTION,
        formatter_class=RawTextHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="Run the components with debug logging enabled",
        action="store_true",
    )
    parser.add_argument(
        "-p",
        "--plaintext",
        help="Run the exchange in plaintext mode",
        action="store_true",
    )

    exchange = parser.add_argument_group("Exchange")
    exchange.add_argument(
        "-e:s",
        "--exchange-server",
        help="Host the intermediate component server on this machine",
        action="store_true",
    )
    exchange.add_argument(
        "-e:h",
        "--exchange-host",
        help="Host address to run the exchange component on, e.g `[::]` or `0.0.0.0`",
        type=str,
        default="[::]",
    )
    exchange.add_argument(
        "-e:p",
        "--exchange-port",
        help="Host port used to communicate with the exchange component on, e.g `50050`",
        type=int,
        default=50050,
    )
    exchange.add_argument(
        "-e:o",
        "--exchange-output",
        help="Path to the file where the output of the exchange's performed order list together with the assigned identifier and a timestamp should be stored",
        type=str,
        default=None,
    )

    intermediate = parser.add_argument_group("Intermediate")
    intermediate.add_argument(
        "-i:s",
        "--intermediate-server",
        help="Host the intermediate component server on this machine",
        action="store_true",
    )
    intermediate.add_argument(
        "-i:h",
        "--intermediate-host",
        help="Host address to run the intermediate component on, e.g `[::]` or `0.0.0.0`",
        type=str,
        default="[::]",
    )
    intermediate.add_argument(
        "-i:p",
        "--intermediate-port",
        help="Host port used to communicate with the exchange component on, e.g `50051`",
        type=int,
        default=50051,
    )

    client = parser.add_argument_group("Client")
    client.add_argument(
        "-c:i",
        "--client-input-files",
        help="Path(s) to client input file detailing the orders to replay for the trading for each client/broker, can be multiple order files\n"
        "Example files can be found under `pet-exchange/example/orders.json` and should detail the clients plaintext order intentions",
        nargs="*",
        type=str,
        default=[],
    )
    client.add_argument(
        "-c:t",
        "--client-order-type",
        help="The client exchange order type to use, if not present the order type is auto-detected from orders file",
        choices=["LIMIT", "MARKET"],
        type=str,
        default=None,
    )
    client.add_argument(
        "-c:o",
        "--client-output",
        help="Path to the file where the output of the client's placed order list together with the assigned identifier and a timestamp should be stored",
        type=str,
        default=None,
    )
    client.add_argument(
        "-c:w",
        "--client-offset",
        help="Apply the offset defined in the order specification, if this is used and no offset is defined the order is sent immediatly otherwise wait until offset is achieved."
        "The offset is applied in relation to the previous order",
        action="store_true",
    )

    args = parser.parse_args()

    if args.debug:
        LOGGER_CONFIG["loggers"]["__main__"]["handlers"] = ["DEBUG"]
        LOGGER_CONFIG["loggers"]["__main__"]["level"] = "DEBUG"

    logger = proc_logger(logger_config=LOGGER_CONFIG)
    (handler,) = logger.handlers

    handler.setFormatter(
        CustomFormatter(
            LOGGER_CONFIG["formatters"]["DEBUG" if args.debug else "INFO"]["format"]
        )
    )

    if (
        not any([getattr(args, arg) for arg in vars(args) if arg.endswith("_server")])
        and not args.client.client_input_files
    ):
        logging.error(
            "No components/clients were defined please see `-h`, `--help` for more details on usage"
        )
        parser.print_usage()
        exit(0)

    try:
        asyncio.run(start(args))
    except KeyboardInterrupt:
        logger.info("PET-Exchange killed ...")