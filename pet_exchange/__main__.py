#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# TODO: Control and run the different components using argparse here, i.e 'python pet-exchange -i -a 0.0.0.0 -p 8080' to start the intermediate component on 0.0.0.0:8080
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
        "cryptographic",
        "exchange_output",
        "exchange_local_sort",
        "exchange_compare_iterations",
        "exchange_compare_constant_count",
        "exchange_compare_sigmoid_iterations",
        "exchange_challenge_count",
        "exchange_time_limit",
        "exchange_delay_start",
    ],
    "intermediate": [
        "intermediate_host",
        "intermediate_port",
        "intermediate_output",
        "exchange_host",
        "exchange_port",
        "plaintext",
        "cryptographic",
    ],
}

logger = proc_logger()


def _determine_process_count(args: Namespace) -> int:
    _counter = 0
    for arg in vars(args):
        if arg.endswith("_server") and getattr(args, arg):
            _counter += 1

    if args.client_input_files and _counter == 0:
        _counter += 1

    return _counter


def _start_server(parameters: Dict[str, Any]) -> NoReturn:
    name, kwargs = parameters
    try:
        asyncio.run(
            INIT_LOOKUP[name].serve(**kwargs),
        )
    except Exception as e:
        logger.exception(
            f"Main: Exchange '{name}' failed due to unexpected error: '{e}'"
        )


def _start_client(parameters: Dict[str, Any]) -> NoReturn:
    _name, _kwargs = parameters
    _start = time.time()
    try:
        asyncio.run(
            exchange_client.client(
                exchange=(_kwargs["EXCHANGE_HOST"], _kwargs["EXCHANGE_PORT"]),
                client=_name,
                orders=_kwargs["ORDERS"],
                encrypted=_kwargs["ENCRYPTED"],
                client_output=_kwargs["CLIENT_OUTPUT"],
                exchange_order_type=_kwargs["EXCHANGE_ORDER_TYPE"],
                static_offset=_kwargs["CLIENT_STATIC_OFFSET"],
                run_forever=_kwargs["CLIENT_RUN_FOREVER"],
                _start=None if not _kwargs["USE_OFFSET"] else _start,
            )
        )
    except Exception as e:
        logger.exception(
            f"Main: Client '{_name}' failed due to unexpected error: '{e}'"
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
            for client, orders in json.load(_orders)["CLIENTS"].items():
                _clients[client] = {
                    "ORDERS": orders,
                    "EXCHANGE_HOST": args.exchange_host,
                    "EXCHANGE_PORT": args.exchange_port,
                    "ENCRYPTED": None if args.plaintext else args.cryptographic,
                    "CLIENT_OUTPUT": args.client_output,
                    "USE_OFFSET": args.client_offset,
                    "CLIENT_STATIC_OFFSET": args.client_static_offset,
                    "CLIENT_RUN_FOREVER": args.client_run_forever,
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


def get_instruments(files: List[str]) -> List[str]:
    instruments: List[str] = []
    for path in files:
        _path = Path(path)
        if not _path.exists():
            raise FileNotFoundError

        with _path.open(mode="r") as file_obj:
            struct = json.load(file_obj)
            for instrument in struct["INSTRUMENTS"]:
                if instrument not in instruments:
                    instruments.append(instrument)

    return instruments


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

                if args.exchange_instruments:
                    _servers[arg]["instruments"] = args.exchange_instruments
                elif args.client_input_files:
                    _servers[arg]["instruments"] = get_instruments(
                        args.client_input_files
                    )
                else:
                    _servers[arg]["instruments"] = None

                # Derive mode of the exchange, plaintext or cryptographic
                _servers[arg]["encrypted"] = (
                    None
                    if _servers[arg]["plaintext"]
                    else _servers[arg]["cryptographic"]
                )
                del _servers[arg]["plaintext"]
                del _servers[arg]["cryptographic"]

                if _component == "exchange":
                    _servers[arg]["local_sort"] = _servers[arg]["exchange_local_sort"]
                    del _servers[arg]["exchange_local_sort"]
                    _servers[arg]["compare_iterations"] = _servers[arg][
                        "exchange_compare_iterations"
                    ]
                    del _servers[arg]["exchange_compare_iterations"]
                    _servers[arg]["compare_constant_count"] = _servers[arg][
                        "exchange_compare_constant_count"
                    ]
                    del _servers[arg]["exchange_compare_constant_count"]
                    _servers[arg]["compare_sigmoid_iterations"] = _servers[arg][
                        "exchange_compare_sigmoid_iterations"
                    ]
                    del _servers[arg]["exchange_compare_sigmoid_iterations"]
                    _servers[arg]["challenge_count"] = _servers[arg][
                        "exchange_challenge_count"
                    ]
                    del _servers[arg]["exchange_challenge_count"]
                    _servers[arg]["time_limit"] = _servers[arg]["exchange_time_limit"]
                    del _servers[arg]["exchange_time_limit"]
                    _servers[arg]["delay_start"] = _servers[arg]["exchange_delay_start"]
                    del _servers[arg]["exchange_delay_start"]

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
    except Exception as exc:
        logger.error("Unknown exception occured when starting components")
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
    parser.add_argument(
        "-c",
        "--cryptographic",
        help="Run the exchange in cryptographic mode",
        action="store_true"
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
        help="Host address to run the exchange component on, e.g '[::]' or '0.0.0.0'",
        type=str,
        default="[::]",
    )
    exchange.add_argument(
        "-e:p",
        "--exchange-port",
        help="Host port used to communicate with the exchange component on, e.g '50050'",
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
    exchange.add_argument(
        "-e:i",
        "--exchange-instruments",
        help="The instruments to allow orders for, this will also be used in the cryptographic version to generate keys for each instrument"
        "If it is not provided then the instruments will be derived from the client input files, and if those are not available the keys will be generated on the fly",
        type=str,
        nargs="+",
        default=None,
    )
    exchange.add_argument(
        "-e:l",
        "--exchange-local-sort",
        help="Sort the orders locally on the exchange even for encrypted trading, this slows down trading severly due to the performance of the matching",
        action="store_true",
    )
    exchange.add_argument(
        "-e:c",
        "--exchange-compare",
        help="Compare function to use for the semi-local approximative compare",
        type=int,
        choices=[1, 2],
        default=1,
    )
    exchange.add_argument(
        "-e:ci",
        "--exchange-compare-iterations",
        help="Number of iterations to run the approximative compare algorithm for, works for both compare functions",
        type=int,
        default=1,
    )
    exchange.add_argument(
        "-e:csi",
        "--exchange-compare-sigmoid-iterations",
        help="Number of iterations to run the sigmoid approximation for the second compare function '2'",
        type=int,
        default=3,
    )
    exchange.add_argument(
        "-e:ccc",
        "--exchange-compare-constant-count",
        help="The constant count to use for the second compare function '2'",
        type=int,
        choices=[3, 5, 9],
        default=3,
    )
    exchange.add_argument(
        "-e:cc",
        "--exchange-challenge-count",
        help="Number of challenges to add to minimal value requests for the intermediate",
        type=int,
        default=3,
    )
    exchange.add_argument(
        "-e:tl",
        "--exchange-time-limit",
        help="Stop matching after a given number of seconds, useful for benchmarking volume matched over a given timeframe",
        type=int,
        default=None,
    )
    exchange.add_argument(
        "-e:ds",
        "--exchange-delay-start",
        help="Delay start of matching so exchange is accepting orders but not matching until time limit is reached, useful for simulating pre-market open orders",
        type=int,
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
        help="Host address to run the intermediate component on, e.g '[::]' or '0.0.0.0'",
        type=str,
        default="[::]",
    )
    intermediate.add_argument(
        "-i:p",
        "--intermediate-port",
        help="Host port used to communicate with the exchange component on, e.g '50051'",
        type=int,
        default=50051,
    )
    intermediate.add_argument(
        "-i:o",
        "--intermediate-output",
        help="Path to the file where the output of the intermediate's metrics",
        type=str,
        default=None,
    )

    client = parser.add_argument_group("Client")
    client.add_argument(
        "-c:i",
        "--client-input-files",
        help="Path(s) to client input file detailing the orders to replay for the trading for each client/broker, can be multiple order files\n"
        "Example files can be found under 'pet-exchange/example/orders.json' and should detail the clients plaintext order intentions",
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
        "-c:so",
        "--client-static-offset",
        help="Static offset to apply after each processed order, helps reduce the amount of orders collected at the same time at the exchange, defaults to no static offset",
        type=float,
        default=None,
    )
    client.add_argument(
        "-c:rf",
        "--client-run-forever",
        help="Run the client orders forever, will repeat from the beginning and continue to publish to the exchange, useful in combination with --exchange-time-limit",
        action="store_true",
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

    if not args.cryptographic and not args.plaintext:
        logger.warning(f"No mode set, defaulting to plaintext")
        args.plaintext = True

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
        and not args.client_input_files
    ):
        logging.error(
            "No components/clients were defined please see '-h', '--help' for more details on usage"
        )
        parser.print_usage()
        exit(0)

    try:
        asyncio.run(start(args))
    except KeyboardInterrupt:
        logger.info("PET-Exchange killed ...")
    except Exception as e:
        print(e)
