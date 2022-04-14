#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any
from argparse import ArgumentParser
from pathlib import Path
import json


def validate_client(path: str) -> bool:
    _path = Path(path)
    if not _path.exists():
        raise ValueError(f"Path to client output file: '{path}' is not accessible")

    with _path.open(mode="r") as client_file:
        client_file_json = json.load(client_file)

        for client, orders in client_file_json["CLIENTS"].items():
            print(f"Validating client: '{client}' submitted orders")

            for identifier, order in orders.items():
                if order["volume"] < 0:
                    raise ValueError(
                        f"Volume for order: '{identifier}' was less than 0"
                    )

                if order["price"] < 0:
                    raise ValueError(f"Price for order: '{identifier}' was less than 0")

    return True


def validate_exchange(path: str, precision: int) -> bool:
    _path = Path(path)
    if not _path.exists():
        raise ValueError(f"Path to exchange output file: '{path}' is not accessible")

    with _path.open(mode="r") as exchange_file:
        exchange_file_json = json.load(exchange_file)

        print(f"Validating exchange for submitted orders")
        for instrument, struct in exchange_file_json.items():
            for identifier, order in struct["PERFORMED"].items():
                if precision is not None:
                    order["performed"]["volume"] = round(
                        order["performed"]["volume"], precision
                    )
                    order["performed"]["price"] = round(
                        order["performed"]["price"], precision
                    )

                if order["instrument"] != instrument:
                    raise ValueError(
                        f"Instrument mismatch for order: '{identifier}', '{instrument}' != '{order['instrument']}'"
                    )

                if order["performed"]["volume"] < 0:
                    raise ValueError(
                        f"Order volume performed for order: '{identifier}' was less than 0"
                    )

                if order["performed"]["price"] < 0:
                    raise ValueError(
                        f"Order price performed for order: '{identifier}' was less than 0"
                    )

    return True


def validate_executed(
    path_clients: List[str], path_exchange: str, precision: int
) -> bool:
    _path_client = Path(path_client)
    if not _path_client.exists():
        raise ValueError(
            f"Path to client output file: '{_path_client}' is not accessible"
        )

    _path_exchange = Path(path_exchange)
    if not _path_exchange.exists():
        raise ValueError(
            f"Path to exchange output file: '{_path_exchange}' is not accessible"
        )

    with _path_exchange.open(mode="r") as exchange_file:
        exchange_file_json = json.load(exchange_file)

        total_orders: Dict[str, Any] = {}
        total_orders_results: Dict[str, bool] = {}

        with _path_client.open(mode="r") as client_file:
            client_file_json = json.load(client_file)
            for client, orders in client_file_json["CLIENTS"].items():
                for identifier, order in orders.items():
                    total_orders[identifier] = order
                    total_orders_results[identifier] = True

            print(f"Validating exchange for executed orders for clients")
            for instrument, struct in exchange_file_json.items():
                for executed_identifier, executed_order in struct["PERFORMED"].items():
                    if precision is not None:
                        executed_order["performed"]["price"] = round(
                            executed_order["performed"]["price"], precision
                        )
                        executed_order["performed"]["volume"] = round(
                            executed_order["performed"]["volume"], precision
                        )

                    for order_type in executed_order["references"].keys():

                        if executed_order["references"][order_type] not in total_orders:
                            print(
                                f"Executed order reference: '{executed_identifier}' for order '{executed_order['references'][order_type]}' not in client: '{executed_order['entity'][order_type]}' orders"
                            )
                            total_orders_results[
                                executed_order["references"][order_type]
                            ] = False
                            continue

                        total_orders[executed_order["references"][order_type]][
                            "volume"
                        ] -= executed_order["performed"]["volume"]
                        if (
                            total_orders[executed_order["references"][order_type]][
                                "volume"
                            ]
                            < 0
                        ):
                            print(
                                f"Executed order reference: '{executed_identifier}' for order: '{executed_order['references'][order_type]}' resulted in a volume < 0 ({total_orders[executed_order['references'][order_type]]['volume']})"
                            )
                            total_orders_results[
                                executed_order["references"][order_type]
                            ] = False
                            continue

                    if (
                        executed_order["performed"]["price"]
                        != total_orders[executed_order["references"]["ask"]]["price"]
                    ):
                        print(
                            f"Executed order: '{executed_identifier}' performed price: ({executed_order['performed']['price']}) was not the ASK price: ({total_orders[executed_order['references']['ask']]['price']}) for order: '{executed_order['references']['ask']}'"
                        )
                        total_orders_results[
                            executed_order["references"][order_type]
                        ] = False
                        continue

                    if (
                        executed_order["performed"]["price"]
                        > total_orders[executed_order["references"]["bid"]]["price"]
                    ):
                        print(
                            f"Executed order: '{executed_identifier}' performed price: ({executed_order['performed']['price']}) was greater than the BID price: ({total_orders[executed_order['references']['bid']]['price']}) for order: '{executed_order['references']['bid']}'"
                        )
                        total_orders_results[
                            executed_order["references"][order_type]
                        ] = False
                        continue

    print(
        f"Correct Count: ({len(list(filter(lambda _: _, total_orders_results.values())))}), Wrong Count: ({len(list(filter(lambda _: not _, total_orders_results.values())))})"
    )

    return True


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange Validator",
        description="Validate the executed trades to ensure that no orders were invalid",
    )
    parser.add_argument(
        "-p:e",
        "--path-exchange",
        help="Path to the exchange summary",
        type=str,
        required=True,
    )
    parser.add_argument(
        "-p:c",
        "--path-clients",
        help="Path to the each client summary to include",
        type=str,
        nargs="+",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--precision",
        help="The precision of the floating point values used",
        type=int,
        default=None,
    )
    args = parser.parse_args()

    print("PET-Exchange Validator Starting ...", end="\n\n")
    for path_client in args.path_clients:
        if validate_client(path_client):
            print(f"Client file: '{path_client}' OK!")
        else:
            print(f"Client file: '{path_client}' contained issue(s)")

    print()

    if validate_exchange(args.path_exchange, args.precision):
        print(f"Exchange file: '{args.path_exchange}' OK!")
    else:
        print(f"Exchange file: '{args.path_exchange}' contained issue(s)")

    print()

    if validate_executed(
        path_clients=args.path_clients,
        path_exchange=args.path_exchange,
        precision=args.precision,
    ):
        print(
            f"Executed orders for client: '{path_client}' and exchange: '{args.path_exchange}' OK!"
        )
    else:
        print(
            f"Executed orders for client: '{path_client}' and exchange: '{args.path_exchange}' contained issue(s)"
        )
