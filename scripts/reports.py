#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, NoReturn, Any, Dict, Tuple
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
import json

DATE_FORMAT = "%d/%m/%y %H:%M:%S.%f"

CLIENTS_STRUCTURE = {"CLIENT": {}, "INSTRUMENTS": {}, "TRADES": [], "METRICS": {}}

CLIENTS_CLIENT_STRUCTURE = {
    "HOST": None,
    "PORT": None,
}

CLIENTS_INSTRUMENT_STRUCTURE = {
    "FIRST_SUBMIT": None,
    "FIRST_SUBMIT_BID": None,
    "FIRST_SUBMIT_ASK": None,
    "LAST_SUBMIT": None,
    "LAST_SUBMIT_BID": None,
    "LAST_SUBMIT_ASK": None,
    "TOTAL_SUBMIT": None,
    "TOTAL_SUBMIT_BID": None,
    "TOTAL_SUBMIT_ASK": None,
    "TOTAL_VOLUME": None,
    "TOTAL_VOLUME_BID": None,
    "TOTAL_VOLUME_ASK": None,
    "TOTAL_PRICE": None,
    "TOTAL_PRICE_BID": None,
    "TOTAL_PRICE_ASK": None,
    "AVERAGE_VOLUME": None,
    "AVERAGE_VOLUME_BID": None,
    "AVERAGE_VOLUME_ASK": None,
    "AVERAGE_PRICE": None,
    "AVERAGE_PRICE_BID": None,
    "AVERAGE_PRICE_ASK": None,
    "MIN_VOLUME": None,
    "MIN_VOLUME_BID": None,
    "MIN_VOLUME_ASK": None,
    "MAX_VOLUME": None,
    "MAX_VOLUME_BID": None,
    "MAX_VOLUME_ASK": None,
    "MIN_PRICE": None,
    "MIN_PRICE_BID": None,
    "MIN_PRICE_ASK": None,
    "MAX_PRICE": None,
    "MAX_PRICE_BID": None,
    "MAX_PRICE_ASK": None,
}

TRADE_STRUCTURE = {
    "INSTRUMENT": None,
    "ORDER_TYPE": None,
    "MARKET_TYPE": None,
    "SUBMIT_DATE": None,
    "EXECUTED": None,
    "IDENTIFIER": None,
    "TOTAL_VOLUME": None,
    "TOTAL_VOLUME_EXECUTED": None,
    "TOTAL_PRICE_EXECUTED": None,
    "TOTAL_PRICE_EXPECTED": None,
    "TIME_TO_EXECUTE": None,
}

CLIENTS_METRIC_STRUCTURE = {
    "FIRST_SUBMIT_TIME": None,
    "FIRST_SUBMIT_TIME_BID": None,
    "FIRST_SUBMIT_TIME_ASK": None,
    "LAST_SUBMIT_TIME": None,
    "LAST_SUBMIT_TIME_BID": None,
    "LAST_SUBMIT_TIME_ASK": None,
    "FIRST_EXCHANGE_TIME": None,
    "FIRST_EXCHANGE_TIME_BID": None,
    "FIRST_EXCHANGE_TIME_ASK": None,
    "LAST_EXCHANGE_TIME": None,
    "LAST_EXCHANGE_TIME_BID": None,
    "LAST_EXCHANGE_TIME_ASK": None,
    "AVERAGE_TIME_TO_EXCHANGE": None,
    "AVERAGE_TIME_TO_EXCHANGE_BID": None,
    "AVERAGE_TIME_TO_EXCHANGE_ASK": None,
    "MAX_TIME_TO_EXCHANGE": None,
    "MAX_TIME_TO_EXCHANGE_BID": None,
    "MAX_TIME_TO_EXCHANGE_ASK": None,
    "MIN_TIME_TO_EXCHANGE": None,
    "MIN_TIME_TO_EXCHANGE_BID": None,
    "MIN_TIME_TO_EXCHANGE_ASK": None,
    "TOTAL_SUBMITTED": None,
    "TOTAL_SUBMITTED_BID": None,
    "TOTAL_SUBMITTED_ASK": None,
    "TOTAL_VOLUME_SUBMITTED": None,
    "TOTAL_VOLUME_SUBMITTED_BID": None,
    "TOTAL_VOLUME_SUBMITTED_ASK": None,
    "TOTAL_PRICE_SUBMITTED": None,
    "TOTAL_PRICE_SUBMITTED_BID": None,
    "TOTAL_PRICE_SUBMITTED_ASK": None,
    "TOTAL_MATCHED": None,
    "TOTAL_MATCHED_BID": None,
    "TOTAL_MATCHED_ASK": None,
    "TOTAL_VOLUME_MATCHED": None,
    "TOTAL_PRICE_MATCHED": None,
}

INSTRUMENTS_STRUCTURE = {"METRICS": {}}

INSTRUMENTS_METRICS_STRUCTURE = {
    "FIRST_SUBMIT_TIME": None,
    "FIRST_SUBMIT_TIME_BID": None,
    "FIRST_SUBMIT_TIME_ASK": None,
    "LAST_SUBMIT_TIME": None,
    "LAST_SUBMIT_TIME_BID": None,
    "LAST_SUBMIT_TIME_ASK": None,
    "FIRST_EXECUTE_TIME": None,
    "LAST_EXECUTE_TIME": None,
    "INSTRUMENTS_SUBMITTED_COUNT": None,
    "INSTRUMENTS_SUBMITTED_COUNT_BID": None,
    "INSTRUMENTS_SUBMITTED_COUNT_ASK": None,
    "INSTRUMENTS_SUBMITTED": None,
    "INSTRUMENTS_SUBMITTED_BID": None,
    "INSTRUMENTS_SUBMITTED_ASK": None,
    "INSTRUMENTS_EXECUTED_COUNT": None,
    "INSTRUMENTS_EXECUTED": None,
}

METRICS_STRUCTURE = {
    "TOTAL_ORDERS_SUBMITTED": None,
    "TOTAL_ORDERS_SUBMITTED_BID": None,
    "TOTAL_ORDERS_SUBMITTED_ASK": None,
    "TOTAL_ORDERS_EXECUTED": None,
    "FIRST_ORDER_SUBMITTED_DATE": None,
    "LAST_ORDER_SUBMITTED_DATE": None,
    "TOTAL_TIME_TO_SORT": None,
    "TOTAL_TIME_TO_SORT_BID": None,
    "TOTAL_TIME_TO_SORT_ASK": None,
    "MAX_TIME_TO_SORT": None,
    "MAX_TIME_TO_SORT_BID": None,
    "MAX_TIME_TO_SORT_ASK": None,
    "MIN_TIME_TO_SORT": None,
    "MIN_TIME_TO_SORT_BID": None,
    "MIN_TIME_TO_SORT_ASK": None,
    "AVERAGE_TIME_TO_SORT": None,
    "AVERAGE_TIME_TO_SORT_BID": None,
    "AVERAGE_TIME_TO_SORT_ASK": None,
    "MAX_TIME_TO_COMPLETION": None,
    "MAX_TIME_TO_COMPLETION_BID": None,
    "MAX_TIME_TO_COMPLETION_ASK": None,
    "MIN_TIME_TO_COMPLETION": None,
    "MIN_TIME_TO_COMPLETION_BID": None,
    "MIN_TIME_TO_COMPLETION_ASK": None,
    "AVERAGE_TIME_TO_COMPLETION": None,
    "AVERAGE_TIME_TO_COMPLETION_BID": None,
    "AVERAGE_TIME_TO_COMPLETION_ASK": None,
    "TOTAL_TIME_TO_MATCH_ORDER": None,
    "MAX_TIME_TO_MATCH_ORDER": None,
    "MIN_TIME_TO_MATCH_ORDER": None,
    "AVERAGE_TIME_TO_MATCH_ORDER": None,
    "TOTAL_TIME_TO_MATCH_ORDER_NET": None,
    "MAX_TIME_TO_MATCH_ORDER_NET": None,
    "MIN_TIME_TO_MATCH_ORDER_NET": None,
    "AVERAGE_TIME_TO_MATCH_ORDER_NET": None,
    "MAX_TIME_TO_DECRYPT_ORDER": None,
    "MAX_TIME_TO_DECRYPT_ORDER_BID": None,
    "MAX_TIME_TO_DECRYPT_ORDER_ASK": None,
    "MIN_TIME_TO_DECRYPT_ORDER": None,
    "MIN_TIME_TO_DECRYPT_ORDER_BID": None,
    "MIN_TIME_TO_DECRYPT_ORDER_ASK": None,
    "AVERAGE_TIME_TO_DECRYPT_ORDER": None,
    "AVERAGE_TIME_TO_DECRYPT_ORDER_BID": None,
    "AVERAGE_TIME_TO_DECRYPT_ORDER_ASK": None,
    "MAX_TIME_TO_GENERATE_CHALLENGES": None,
    "MIN_TIME_TO_GENERATE_CHALLENGES": None,
    "AVERAGE_TIME_TO_GENERATE_CHALLENGES": None,
    "TOTAL_VOLUME_SUBMITTED": None,
    "TOTAL_VOLUME_SUBMITTED_BID": None,
    "TOTAL_VOLUME_SUBMITTED_ASK": None,
    "AVERAGE_VOLUME_SUBMITTED": None,
    "AVERAGE_VOLUME_SUBMITTED_BID": None,
    "AVERAGE_VOLUME_SUBMITTED_ASK": None,
    "TOTAL_PRICE_SUBMITTED": None,
    "TOTAL_PRICE_SUBMITTED_BID": None,
    "TOTAL_PRICE_SUBMITTED_ASK": None,
    "AVERAGE_PRICE_SUBMITTED": None,
    "AVERAGE_PRICE_SUBMITTED_BID": None,
    "AVERAGE_PRICE_SUBMITTED_ASK": None,
    "TOTAL_VOLUME_EXECUTED": None,
    "AVERAGE_VOLUME_EXECUTED": None,
    "TOTAL_PRICE_EXECUTED": None,
    "AVERAGE_PRICE_EXECUTED": None,
    "TOTAL_PRICE_PER_EXECUTED": None,
    "AVERAGE_PRICE_PER_EXECUTED": None,
}


STRUCTURE = {"CLIENTS": {}, "INSTRUMENTS": {}, "TRADES": {}, "METRICS": {}}


def _get_client_instrument_struct(orders: Dict[str, Any]) -> Dict[str, Any]:
    struct: Dict[str, Any] = {}
    for identifier, order in orders.items():
        _struct: Dict[str, Any] = struct.setdefault(
            order["instrument"], CLIENTS_INSTRUMENT_STRUCTURE.copy()
        )

        if _struct["FIRST_SUBMIT"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(_struct["FIRST_SUBMIT"], DATE_FORMAT):
            _struct["FIRST_SUBMIT"] = order["placed"]

        if _struct[f"FIRST_SUBMIT_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(_struct[f"FIRST_SUBMIT_{order['type']}"], DATE_FORMAT):
            _struct[f"FIRST_SUBMIT_{order['type']}"] = order["placed"]

        if _struct["LAST_SUBMIT"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(_struct["LAST_SUBMIT"], DATE_FORMAT):
            _struct["LAST_SUBMIT"] = order["placed"]

        if _struct[f"LAST_SUBMIT_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(_struct[f"LAST_SUBMIT_{order['type']}"], DATE_FORMAT):
            _struct[f"LAST_SUBMIT_{order['type']}"] = order["placed"]

        _struct["TOTAL_SUBMIT"] = (
            1 if _struct["TOTAL_SUBMIT"] is None else _struct["TOTAL_SUBMIT"] + 1
        )

        _struct[f"TOTAL_SUBMIT_{order['type']}"] = (
            1
            if _struct[f"TOTAL_SUBMIT_{order['type']}"] is None
            else _struct[f"TOTAL_SUBMIT_{order['type']}"] + 1
        )

        _struct["TOTAL_VOLUME"] = (
            order["volume"]
            if _struct["TOTAL_VOLUME"] is None
            else _struct["TOTAL_VOLUME"] + order["volume"]
        )

        _struct[f"TOTAL_VOLUME_{order['type']}"] = (
            order["volume"]
            if _struct[f"TOTAL_VOLUME_{order['type']}"] is None
            else _struct[f"TOTAL_VOLUME_{order['type']}"] + order["volume"]
        )

        _struct["TOTAL_PRICE"] = (
            order["price"]
            if _struct["TOTAL_PRICE"] is None
            else _struct["TOTAL_PRICE"] + order["price"]
        )

        _struct[f"TOTAL_PRICE_{order['type']}"] = (
            order["price"]
            if _struct[f"TOTAL_PRICE_{order['type']}"] is None
            else _struct[f"TOTAL_PRICE_{order['type']}"] + order["price"]
        )

        if _struct["MIN_VOLUME"] is None or order["volume"] < _struct["MIN_VOLUME"]:
            _struct["MIN_VOLUME"] = order["volume"]

        if (
            _struct[f"MIN_VOLUME_{order['type']}"] is None
            or order["volume"] < _struct[f"MIN_VOLUME_{order['type']}"]
        ):
            _struct[f"MIN_VOLUME_{order['type']}"] = order["volume"]

        if _struct["MAX_VOLUME"] is None or order["volume"] < _struct["MAX_VOLUME"]:
            _struct["MAX_VOLUME"] = order["volume"]

        if (
            _struct[f"MAX_VOLUME_{order['type']}"] is None
            or order["volume"] > _struct[f"MAX_VOLUME_{order['type']}"]
        ):
            _struct[f"MAX_VOLUME_{order['type']}"] = order["volume"]

        if _struct["MIN_PRICE"] is None or order["price"] < _struct["MIN_PRICE"]:
            _struct["MIN_PRICE"] = order["price"]

        if (
            _struct[f"MIN_PRICE_{order['type']}"] is None
            or order["price"] < _struct[f"MIN_PRICE_{order['type']}"]
        ):
            _struct[f"MIN_PRICE_{order['type']}"] = order["price"]

        if _struct["MAX_PRICE"] is None or order["price"] > _struct["MAX_PRICE"]:
            _struct["MAX_PRICE"] = order["price"]

        if (
            _struct[f"MAX_PRICE_{order['type']}"] is None
            or order["price"] > _struct[f"MAX_PRICE_{order['type']}"]
        ):
            _struct[f"MAX_PRICE_{order['type']}"] = order["price"]

    for instrument_struct in struct.values():
        instrument_struct.update(
            {
                "AVERAGE_VOLUME": (
                    instrument_struct["TOTAL_VOLUME"]
                    / instrument_struct["TOTAL_SUBMIT"]
                )
                if instrument_struct["TOTAL_SUBMIT"]
                else None,
                "AVERAGE_VOLUME_BID": (
                    instrument_struct["TOTAL_VOLUME"]
                    / instrument_struct["TOTAL_SUBMIT_BID"]
                )
                if instrument_struct["TOTAL_SUBMIT_BID"]
                else None,
                "AVERAGE_VOLUME_ASK": (
                    instrument_struct["TOTAL_VOLUME"]
                    / instrument_struct["TOTAL_SUBMIT_ASK"]
                )
                if instrument_struct["TOTAL_SUBMIT_ASK"]
                else None,
                "AVERAGE_PRICE": (
                    instrument_struct["TOTAL_PRICE"] / instrument_struct["TOTAL_SUBMIT"]
                )
                if instrument_struct["TOTAL_SUBMIT"]
                else None,
                "AVERAGE_PRICE_BID": (
                    instrument_struct["TOTAL_PRICE_BID"]
                    / instrument_struct["TOTAL_SUBMIT_BID"]
                )
                if instrument_struct["TOTAL_SUBMIT_BID"]
                else None,
                "AVERAGE_PRICE_ASK": (
                    instrument_struct["TOTAL_PRICE_ASK"]
                    / instrument_struct["TOTAL_SUBMIT_ASK"]
                )
                if instrument_struct["TOTAL_SUBMIT_ASK"]
                else None,
            }
        )

    return struct


def _get_client_trades_struct(
    orders: Dict[str, Any], executed_orders: Dict[str, Any]
) -> Dict[str, Any]:
    structs: List[Any] = []
    for identifier, order in orders.items():
        _executed_orders = dict(
            filter(
                lambda _order: identifier
                in (_order[1]["references"]["ask"], _order[1]["references"]["bid"]),
                executed_orders[order["instrument"]]["PERFORMED"].items(),
            )
        )

        _executed_struct = {
            "EXECUTED": {},
            "TOTAL_VOLUME_EXECUTED": None,
            "TOTAL_PRICE_EXECUTED": None,
            "TIME_TO_EXECUTE": None,
        }
        for executed_identifier, executed_order in _executed_orders.items():
            _order_type = (
                "bid" if identifier == executed_order["references"]["bid"] else "ask"
            )
            _executed_struct["EXECUTED"][executed_order["performed"]["at"]] = {
                "EXECUTED_VOLUME": executed_order["performed"]["volume"],
                "EXECUTED_PRICE": executed_order["performed"]["price"],
                "EXECUTED_AT": executed_order["performed"]["at"],
                "TIME_TO_EXECUTE": (
                    datetime.strptime(executed_order["performed"]["at"], DATE_FORMAT)
                    - datetime.strptime(order["placed"], DATE_FORMAT)
                ).total_seconds(),
            }

            _executed_struct["TIME_TO_EXECUTE"] = (
                datetime.strptime(executed_order["performed"]["at"], DATE_FORMAT)
                - datetime.strptime(order["placed"], DATE_FORMAT)
            ).total_seconds()

            _executed_struct["TOTAL_VOLUME_EXECUTED"] = (
                executed_order["performed"]["volume"]
                if _executed_struct["TOTAL_VOLUME_EXECUTED"] is None
                else _executed_struct["TOTAL_VOLUME_EXECUTED"]
                + executed_order["performed"]["volume"]
            )

            _executed_struct["TOTAL_PRICE_EXECUTED"] = (
                executed_order["performed"]["price"]
                * executed_order["performed"]["volume"]
                if _executed_struct["TOTAL_PRICE_EXECUTED"] is None
                else _executed_struct["TOTAL_PRICE_EXECUTED"]
                + (
                    executed_order["performed"]["price"]
                    * executed_order["performed"]["volume"]
                )
            )

        _struct = TRADE_STRUCTURE.copy()
        _struct.update(
            {
                "INSTRUMENT": order["instrument"],
                "ORDER_TYPE": order["type"],
                "SUBMIT_DATE": order["placed"],
                "IDENTIFIER": identifier,
                "TOTAL_VOLUME": order["volume"],
                "PRICE_PER": order["price"],
                "TOTAL_PRICE_EXPECTED": order["price"] * order["volume"],
                **_executed_struct,
            }
        )
        structs.append(_struct)

    return structs


def _get_client_metrics_struct(
    orders: List[Any], executed_orders: Dict[str, Any]
) -> Dict[str, Any]:
    struct: Dict[str, Any] = CLIENTS_METRIC_STRUCTURE.copy()

    _submit_times: Dict[str, datetime] = {}
    _submit_times_bid: Dict[str, datetime] = {}
    _submit_times_ask: Dict[str, datetime] = {}
    _exchange_times: Dict[str, datetime] = {}
    _exchange_times_bid: Dict[str, datetime] = {}
    _exchange_times_ask: Dict[str, datetime] = {}
    _matched_bid: Dict[str, Any] = {}
    _matched_ask: Dict[str, Any] = {}

    for identifier, order in orders.items():
        _submit_times[identifier] = datetime.strptime(order["placed"], DATE_FORMAT)
        if order["type"] == "BID":
            _submit_times_bid[identifier] = datetime.strptime(
                order["placed"], DATE_FORMAT
            )
        else:
            _submit_times_ask[identifier] = datetime.strptime(
                order["placed"], DATE_FORMAT
            )

        _executed_orders = dict(
            filter(
                lambda _order: identifier
                in (_order[1]["references"]["ask"], _order[1]["references"]["bid"]),
                executed_orders[order["instrument"]]["PERFORMED"].items(),
            )
        )

        _executed_struct = {
            "FIRST_EXCHANGE_TIME": None,
            "FIRST_EXCHANGE_TIME_BID": None,
            "FIRST_EXCHANGE_TIME_ASK": None,
            "LAST_EXCHANGE_TIME": None,
            "LAST_EXCHANGE_TIME_BID": None,
            "LAST_EXCHANGE_TIME_ASK": None,
        }

        for executed_identifier, executed_order in _executed_orders.items():
            _order_type = (
                "BID" if identifier == executed_order["references"]["bid"] else "ASK"
            )

            if executed_identifier not in _exchange_times:
                _exchange_times[identifier] = datetime.strptime(
                    executed_order["performed"]["at"], DATE_FORMAT
                )

            if _order_type == "BID" and executed_identifier not in _exchange_times_bid:
                _exchange_times_bid[identifier] = datetime.strptime(
                    executed_order["performed"]["at"], DATE_FORMAT
                )

            if _order_type == "ASK" and executed_identifier not in _exchange_times_ask:
                _exchange_times_ask[identifier] = datetime.strptime(
                    executed_order["performed"]["at"], DATE_FORMAT
                )

            executed_bid, executed_ask = (
                executed_order["references"]["bid"],
                executed_order["references"]["ask"],
            )
            if executed_bid not in _matched_bid:
                _matched_bid[executed_bid] = {
                    "VOLUME": executed_order["performed"]["volume"],
                    "PRICE": executed_order["performed"]["price"]
                    * executed_order["performed"]["volume"],
                }
            else:
                _matched_bid[executed_bid] = {
                    "VOLUME": _matched_bid[executed_bid]["VOLUME"]
                    + executed_order["performed"]["volume"],
                    "PRICE": _matched_bid[executed_bid]["PRICE"]
                    + (
                        executed_order["performed"]["price"]
                        * executed_order["performed"]["volume"]
                    ),
                }

            if executed_ask not in _matched_ask:
                _matched_ask[executed_ask] = {
                    "VOLUME": executed_order["performed"]["volume"],
                    "PRICE": executed_order["performed"]["price"]
                    * executed_order["performed"]["volume"],
                }
            else:
                _matched_ask[executed_ask] = {
                    "VOLUME": _matched_ask[executed_ask]["VOLUME"]
                    + executed_order["performed"]["volume"],
                    "PRICE": _matched_ask[executed_ask]["PRICE"]
                    + (
                        executed_order["performed"]["price"]
                        * executed_order["performed"]["volume"]
                    ),
                }

            if _executed_struct["FIRST_EXCHANGE_TIME"] is None or datetime.strptime(
                executed_order["performed"]["at"], DATE_FORMAT
            ) < datetime.strptime(_executed_struct["FIRST_EXCHANGE_TIME"], DATE_FORMAT):
                struct["FIRST_EXCHANGE_TIME"] = executed_order["performed"]["at"]

            if _executed_struct[
                f"FIRST_EXCHANGE_TIME_{_order_type}"
            ] is None or datetime.strptime(
                executed_order["performed"]["at"], DATE_FORMAT
            ) < datetime.strptime(
                _executed_struct[f"FIRST_EXCHANGE_TIME_{_order_type}"], DATE_FORMAT
            ):
                struct[f"FIRST_EXCHANGE_TIME_{_order_type}"] = executed_order[
                    "performed"
                ]["at"]

            if _executed_struct["LAST_EXCHANGE_TIME"] is None or datetime.strptime(
                executed_order["performed"]["at"], DATE_FORMAT
            ) > datetime.strptime(_executed_struct["LAST_EXCHANGE_TIME"], DATE_FORMAT):
                struct["LAST_EXCHANGE_TIME"] = executed_order["performed"]["at"]

            if _executed_struct[
                f"LAST_EXCHANGE_TIME_{_order_type}"
            ] is None or datetime.strptime(
                executed_order["performed"]["at"], DATE_FORMAT
            ) > datetime.strptime(
                _executed_struct[f"LAST_EXCHANGE_TIME_{_order_type}"], DATE_FORMAT
            ):
                struct[f"LAST_EXCHANGE_TIME_{_order_type}"] = executed_order[
                    "performed"
                ]["at"]

        if struct["FIRST_SUBMIT_TIME"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(struct["FIRST_SUBMIT_TIME"], DATE_FORMAT):
            struct["FIRST_SUBMIT_TIME"] = order["placed"]

        if struct[f"FIRST_SUBMIT_TIME_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(
            struct[f"FIRST_SUBMIT_TIME_{order['type']}"], DATE_FORMAT
        ):
            struct[f"FIRST_SUBMIT_TIME_{order['type']}"] = order["placed"]

        if struct["LAST_SUBMIT_TIME"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(struct["LAST_SUBMIT_TIME"], DATE_FORMAT):
            struct["LAST_SUBMIT_TIME"] = order["placed"]

        if struct[f"LAST_SUBMIT_TIME_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(struct[f"LAST_SUBMIT_TIME_{order['type']}"], DATE_FORMAT):
            struct[f"LAST_SUBMIT_TIME_{order['type']}"] = order["placed"]

        struct["TOTAL_SUBMITTED"] = (
            1 if struct["TOTAL_SUBMITTED"] is None else struct["TOTAL_SUBMITTED"] + 1
        )

        struct[f"TOTAL_SUBMITTED_{order['type']}"] = (
            1
            if struct[f"TOTAL_SUBMITTED_{order['type']}"] is None
            else struct[f"TOTAL_SUBMITTED_{order['type']}"] + 1
        )

        struct["TOTAL_VOLUME_SUBMITTED"] = (
            order["volume"]
            if struct["TOTAL_VOLUME_SUBMITTED"] is None
            else struct["TOTAL_VOLUME_SUBMITTED"] + order["volume"]
        )

        struct[f"TOTAL_VOLUME_SUBMITTED_{order['type']}"] = (
            order["volume"]
            if struct[f"TOTAL_VOLUME_SUBMITTED_{order['type']}"] is None
            else struct[f"TOTAL_VOLUME_SUBMITTED_{order['type']}"] + order["volume"]
        )

        struct["TOTAL_PRICE_SUBMITTED"] = (
            order["price"] * order["volume"]
            if struct["TOTAL_PRICE_SUBMITTED"] is None
            else struct["TOTAL_PRICE_SUBMITTED"] + (order["price"] * order["volume"])
        )

        struct[f"TOTAL_PRICE_SUBMITTED_{order['type']}"] = (
            order["price"] * order["volume"]
            if struct[f"TOTAL_PRICE_SUBMITTED_{order['type']}"] is None
            else struct[f"TOTAL_PRICE_SUBMITTED_{order['type']}"]
            + (order["price"] * order["volume"])
        )

    struct.update(
        {
            "AVERAGE_TIME_TO_EXCHANGE": (
                sum(
                    [
                        (
                            _exchange_times[identifier] - _submit_times[identifier]
                        ).total_seconds()
                        for identifier in _exchange_times
                    ]
                )
                / len(_exchange_times)
            )
            if len(_exchange_times)
            else None,
            "AVERAGE_TIME_TO_EXCHANGE_BID": (
                sum(
                    [
                        (
                            _exchange_times_bid[identifier]
                            - _submit_times_bid[identifier]
                        ).total_seconds()
                        for identifier in _exchange_times_bid
                    ]
                )
                / len(_exchange_times_bid)
            )
            if len(_exchange_times_bid)
            else None,
            "AVERAGE_TIME_TO_EXCHANGE_ASK": (
                sum(
                    [
                        (
                            _exchange_times_ask[identifier]
                            - _submit_times_ask[identifier]
                        ).total_seconds()
                        for identifier in _exchange_times_ask
                    ]
                )
                / len(_exchange_times_ask)
            )
            if len(_exchange_times_ask)
            else None,
            "MAX_TIME_TO_EXCHANGE": (
                (
                    max(_exchange_times.values())
                    - _submit_times[max(_exchange_times, key=_exchange_times.get)]
                ).total_seconds()
            )
            if len(_exchange_times)
            else None,
            "MAX_TIME_TO_EXCHANGE_BID": (
                (
                    max(_exchange_times_bid.values())
                    - _submit_times_bid[
                        max(_exchange_times_bid, key=_exchange_times_bid.get)
                    ]
                ).total_seconds()
            )
            if len(_exchange_times_bid)
            else None,
            "MAX_TIME_TO_EXCHANGE_ASK": (
                (
                    max(_exchange_times_ask.values())
                    - _submit_times_ask[
                        max(_exchange_times_ask, key=_exchange_times_ask.get)
                    ]
                ).total_seconds()
            )
            if len(_exchange_times_ask)
            else None,
            "MIN_TIME_TO_EXCHANGE": (
                (
                    min(_exchange_times.values())
                    - _submit_times[min(_exchange_times, key=_exchange_times.get)]
                ).total_seconds()
            )
            if len(_exchange_times)
            else None,
            "MIN_TIME_TO_EXCHANGE_BID": (
                (
                    min(_exchange_times_bid.values())
                    - _submit_times_bid[
                        min(_exchange_times_bid, key=_exchange_times_bid.get)
                    ]
                ).total_seconds()
            )
            if len(_exchange_times_bid)
            else None,
            "MIN_TIME_TO_EXCHANGE_ASK": (
                (
                    min(_exchange_times_ask.values())
                    - _submit_times_ask[
                        min(_exchange_times_ask, key=_exchange_times_ask.get)
                    ]
                ).total_seconds()
            )
            if len(_exchange_times_ask)
            else None,
            "TOTAL_MATCHED": len(_matched_bid) + len(_matched_ask),
            "TOTAL_MATCHED_BID": len(_matched_bid),
            "TOTAL_MATCHED_ASK": len(_matched_ask),
            "TOTAL_VOLUME_MATCHED": sum(
                [_order["VOLUME"] for _order in _matched_bid.values()]
            ),
            "TOTAL_PRICE_MATCHED": sum(
                [_order["PRICE"] for _order in _matched_bid.values()]
            ),
        }
    )
    return struct


def _get_instruments_metrics_struct(
    orders: List[Any], executed_orders: Dict[str, any]
) -> Dict[str, Any]:
    struct: Dict[str, Any] = INSTRUMENTS_METRICS_STRUCTURE.copy()
    _instrument: Dict[str, Any] = {}
    _instrument_executed: Dict[str, Any] = {}
    for order in orders:
        if struct["FIRST_SUBMIT_TIME"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(struct["FIRST_SUBMIT_TIME"], DATE_FORMAT):
            struct["FIRST_SUBMIT_TIME"] = order["placed"]

        if struct[f"FIRST_SUBMIT_TIME_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) < datetime.strptime(
            struct[f"FIRST_SUBMIT_TIME_{order['type']}"], DATE_FORMAT
        ):
            struct[f"FIRST_SUBMIT_TIME_{order['type']}"] = order["placed"]

        if struct["LAST_SUBMIT_TIME"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(struct["LAST_SUBMIT_TIME"], DATE_FORMAT):
            struct["LAST_SUBMIT_TIME"] = order["placed"]

        if struct[f"LAST_SUBMIT_TIME_{order['type']}"] is None or datetime.strptime(
            order["placed"], DATE_FORMAT
        ) > datetime.strptime(struct[f"LAST_SUBMIT_TIME_{order['type']}"], DATE_FORMAT):
            struct[f"LAST_SUBMIT_TIME_{order['type']}"] = order["placed"]

        if order["instrument"] not in _instrument:
            _instrument[order["instrument"]] = {
                "COUNT": 1,
                "BID": {"COUNT": 1 if order["type"] == "BID" else 0},
                "ASK": {"COUNT": 1 if order["type"] == "ASK" else 0},
            }
        else:
            _instrument[order["instrument"]].update(
                {
                    "COUNT": _instrument[order["instrument"]]["COUNT"] + 1,
                    order["type"]: {
                        "COUNT": _instrument[order["instrument"]][order["type"]][
                            "COUNT"
                        ]
                        + 1
                    },
                }
            )

    for identifier, order in executed_orders["PERFORMED"].items():
        if struct["FIRST_EXECUTE_TIME"] is None or datetime.strptime(
            order["performed"]["at"], DATE_FORMAT
        ) < datetime.strptime(struct["FIRST_EXECUTE_TIME"], DATE_FORMAT):
            struct["FIRST_EXECUTE_TIME"] = order["performed"]["at"]

        if struct["LAST_EXECUTE_TIME"] is None or datetime.strptime(
            order["performed"]["at"], DATE_FORMAT
        ) > datetime.strptime(struct["LAST_EXECUTE_TIME"], DATE_FORMAT):
            struct["LAST_EXECUTE_TIME"] = order["performed"]["at"]

        if order["instrument"] not in _instrument_executed:
            _instrument_executed[order["instrument"]] = {
                "COUNT": 1,
            }
        else:
            _instrument_executed[order["instrument"]].update(
                {
                    "COUNT": _instrument_executed[order["instrument"]]["COUNT"] + 1,
                }
            )

    struct.update(
        {
            "INSTRUMENTS_SUBMITTED_COUNT": len(_instrument),
            "INSTRUMENTS_SUBMITTED_COUNT_BID": len(
                [val for val in _instrument.values() if val["BID"]["COUNT"] > 0]
            ),
            "INSTRUMENTS_SUBMITTED_COUNT_ASK": len(
                [val for val in _instrument.values() if val["ASK"]["COUNT"] > 0]
            ),
            "INSTRUMENTS_SUBMITTED": {
                key: {"COUNT": value["COUNT"]} for key, value in _instrument.items()
            },
            "INSTRUMENTS_SUBMITTED_BID": {
                key: {"COUNT": value["BID"]["COUNT"]}
                for key, value in _instrument.items()
                if value["BID"]["COUNT"] > 0
            },
            "INSTRUMENTS_SUBMITTED_ASK": {
                key: {"COUNT": value["ASK"]["COUNT"]}
                for key, value in _instrument.items()
                if value["ASK"]["COUNT"] > 0
            },
            "INSTRUMENTS_EXECUTED_COUNT": len(_instrument_executed),
            "INSTRUMENTS_EXECUTED": {
                key: {"COUNT": value["COUNT"]}
                for key, value in _instrument_executed.items()
            },
        }
    )
    return struct


def create_report(
    path_exchange: str, path_clients: List[str], path_output: str
) -> NoReturn:
    """Create an aggregated report in JSON format
    Details the connected trades, volumes, prices, timings etc
    """
    _path_exchange = Path(path_exchange)
    if not _path_exchange.exists():
        raise ValueError(
            f"Path to exchange output file: '{path_exchange}' is not accessible"
        )

    _client_paths: List[Path] = []
    for path in path_clients:
        _path = Path(path)
        if not _path.exists():
            raise ValueError(f"Path to client output file: '{path}' is not accessible")

        _client_paths.append(_path)

    struct = STRUCTURE.copy()
    with _path_exchange.open(mode="r") as exchange_file:
        exchange_file_json = json.load(exchange_file)

        total_orders: List[Dict[str, Any]] = []
        all_orders: Dict[str, Any] = {}
        all_orders_executed: Dict[str, Any] = {}
        all_metrics_executed: Dict[str, Any] = {}
        client_metrics: Dict[str, Any] = {}

        for _client_path in _client_paths:
            with _client_path.open(mode="r") as client_file:
                client_file_json = json.load(client_file)
                for client, metrics in client_file_json["METRICS"].items():
                    client_metrics[client] = metrics

                for client, orders in client_file_json["CLIENTS"].items():
                    if client in struct["CLIENTS"]:
                        print(
                            f"Client: '{client}' is already added to the report, will overwrite ..."
                        )

                    struct["CLIENTS"][client] = CLIENTS_STRUCTURE.copy()
                    struct["CLIENTS"][client].update(
                        {
                            "CLIENT": CLIENTS_CLIENT_STRUCTURE.copy(),
                            "INSTRUMENTS": _get_client_instrument_struct(orders),
                            "TRADES": _get_client_trades_struct(
                                orders, exchange_file_json
                            ),
                            "METRICS": _get_client_metrics_struct(
                                orders, exchange_file_json
                            ),
                        }
                    )

                    for identifier, order in orders.items():
                        all_orders[identifier] = order

                    total_orders.extend(orders.values())

        struct["INSTRUMENTS"] = INSTRUMENTS_STRUCTURE.copy()
        for instrument in exchange_file_json:
            struct["INSTRUMENTS"][instrument] = {
                "METRICS": _get_instruments_metrics_struct(
                    total_orders, exchange_file_json[instrument]
                )
            }

        for instrument, exchange_struct in exchange_file_json.items():
            if instrument not in struct["TRADES"]:
                struct["TRADES"][instrument] = {}

            for identifier, order in exchange_struct["PERFORMED"].items():
                if order["references"]["bid"] not in all_orders_executed:
                    all_orders_executed[order["references"]["bid"]] = order

                if order["references"]["ask"] not in all_orders_executed:
                    all_orders_executed[order["references"]["ask"]] = order

            for category, sections in exchange_struct["METRICS"].items():
                if category not in all_metrics_executed:
                    all_metrics_executed[category] = {}

                for section in sections:
                    if section not in all_metrics_executed[category]:
                        all_metrics_executed[category][section] = []

                    all_metrics_executed[category][section].extend(sections[section])

            struct["TRADES"][instrument].update(
                {
                    key: {
                        **value,
                        "references": {
                            "ask": {
                                "identifier": value["references"]["ask"],
                                **all_orders[value["references"]["ask"]],
                            },
                            "bid": {
                                "identifier": value["references"]["bid"],
                                **all_orders[value["references"]["bid"]],
                            },
                        },
                    }
                    for key, value in exchange_struct["PERFORMED"].items()
                }
            )

        first_submitted_date: str = None
        last_submitted_date: str = None
        for identifier, order in all_orders.items():
            if first_submitted_date is None or datetime.strptime(
                order["placed"], DATE_FORMAT
            ) < datetime.strptime(first_submitted_date, DATE_FORMAT):
                first_submitted_date = order["placed"]

            if last_submitted_date is None or datetime.strptime(
                order["placed"], DATE_FORMAT
            ) > datetime.strptime(first_submitted_date, DATE_FORMAT):
                last_submitted_date = order["placed"]

        time_completion_bid: List[float] = []
        time_completion_ask: List[float] = []
        for identifier, order in all_orders_executed.items():
            time_completion_bid.append(
                (
                    datetime.strptime(order["performed"]["at"], DATE_FORMAT)
                    - datetime.strptime(
                        all_orders[order["references"]["bid"]]["placed"], DATE_FORMAT
                    )
                ).total_seconds()
            )
            time_completion_ask.append(
                (
                    datetime.strptime(order["performed"]["at"], DATE_FORMAT)
                    - datetime.strptime(
                        all_orders[order["references"]["ask"]]["placed"], DATE_FORMAT
                    )
                ).total_seconds()
            )

        time_sort_bid: List[float] = []
        time_sort_ask: List[float] = []
        time_sort_orders_bid: List[float] = []
        time_sort_orders_ask: List[float] = []
        time_insert_orders_bid: List[float] = []
        time_insert_orders_ask: List[float] = []
        time_insert_orders_bid_net: List[float] = []
        time_insert_orders_ask_net: List[float] = []
        time_match_orders: List[float] = []
        time_match_orders_net: List[float] = []
        time_compare_local: List[float] = []
        time_compare_local_net: List[float] = []
        time_compare_remote: List[float] = []
        time_compare_remote_net: List[float] = []
        time_compare_insert_local: List[float] = []
        time_compare_insert_local_net: List[float] = []
        time_compare_insert_remote: List[float] = []
        time_compare_insert_remote_net: List[float] = []
        time_decrypt_orders: List[float] = []
        time_generate_challenges: List[float] = []
        time_get_minimum_value: List[float] = []
        time_get_minimum_value_net: List[float] = []
        time_get_minimum_insert_value: List[float] = []
        time_get_minimum_insert_value_net: List[float] = []
        time_pad_orders_price_bid: List[float] = []
        time_pad_orders_price_ask: List[float] = []
        time_unpad_orders_price_bid: List[float] = []
        time_unpad_orders_price_ask: List[float] = []
        time_pad_orders_volume_bid: List[float] = []
        time_pad_orders_volume_ask: List[float] = []
        time_unpad_orders_volume_bid: List[float] = []
        time_unpad_orders_volume_ask: List[float] = []
        time_unpad_orders_volume_minimum: List[float] = []
        time_send_orders_net: List[float] = []
        size_orders: List[float] = []
        size_challenges: List[float] = []
        time_iterations: List[float] = []
        for category_name, category_struct in all_metrics_executed.items():
            for section_name, section_struct in category_struct.items():
                if category_name == "sorting":
                    if section_name == "pairs":
                        _bid_list = time_sort_bid
                        _ask_list = time_sort_ask
                    elif section_name == "orders":
                        _bid_list = time_sort_orders_bid
                        _ask_list = time_sort_orders_ask
                    elif section_name == "insert":
                        _bid_list = time_insert_orders_bid
                        _ask_list = time_insert_orders_ask
                    elif section_name == "insert_net":
                        _bid_list = time_insert_orders_bid_net
                        _ask_list = time_insert_orders_ask_net
                elif category_name == "match":
                    if section_name == "pairs":
                        _bid_list = time_match_orders
                    elif section_name == "pairs_net":
                        _bid_list = time_match_orders_net
                elif category_name == "compare":
                    if section_name == "local":
                        _bid_list = time_compare_local
                    elif section_name == "local_net":
                        _bid_list = time_compare_local_net
                    elif section_name == "remote":
                        _bid_list = time_compare_remote
                    elif section_name == "remote_net":
                        _bid_list = time_compare_remote_net
                elif category_name == "compare_insert":
                    if section_name == "local":
                        _bid_list = time_compare_insert_local
                    elif section_name == "local_net":
                        _bid_list = time_compare_insert_local_net
                    elif section_name == "remote":
                        _bid_list = time_compare_insert_remote
                    elif section_name == "remote_net":
                        _bid_list = time_compare_insert_remote_net
                elif category_name == "decrypt":
                    if section_name == "order":
                        _bid_list = time_decrypt_orders
                elif category_name == "challenges":
                    if section_name == "generate":
                        _bid_list = time_generate_challenges
                elif category_name == "minimum":
                    if section_name == "value":
                        _bid_list = time_get_minimum_value
                    elif section_name == "value_net":
                        _bid_list = time_get_minimum_value_net
                elif category_name == "minimum_insert":
                    if section_name == "value":
                        _bid_list = time_get_minimum_insert_value
                    elif section_name == "value_net":
                        _bid_list = time_get_minimum_insert_value_net
                elif category_name == "pad":
                    if section_name == "price":
                        _bid_list = time_pad_orders_price_bid
                        _ask_list = time_pad_orders_price_ask
                    elif section_name == "volume":
                        _bid_list = time_pad_orders_volume_bid
                        _ask_list = time_pad_orders_volume_ask
                elif category_name == "unpad":
                    if section_name == "price":
                        _bid_list = time_unpad_orders_price_bid
                        _ask_list = time_unpad_orders_price_ask
                    elif section_name == "volume":
                        _bid_list = time_unpad_orders_volume_bid
                        _ask_list = time_unpad_orders_volume_ask
                    elif section_name == "minimum":
                        _bid_list = time_unpad_orders_volume_minimum
                elif category_name == "size":
                    if section_name == "order":
                        _bid_list = size_orders
                    elif section_name == "challenge":
                        _bid_list = size_challenges
                elif category_name == "iteration":
                    if section_name == "time":
                        _bid_list = time_iterations

                for section_struct_value in section_struct:
                    if "timings" in section_struct_value:
                        if "type" in section_struct_value:
                            if section_struct_value["type"] == "bid":
                                _bid_list.extend(section_struct_value["timings"])
                            else:
                                _ask_list.extend(section_struct_value["timings"])
                        else:
                            _bid_list.extend(section_struct_value["timings"])
                    elif "sizes" in section_struct_value:
                        if "type" in section_struct_value:
                            if section_struct_value["type"] == "bid":
                                _bid_list.extend(section_struct_value["sizes"])
                            else:
                                _ask_list.extend(section_struct_value["sizes"])
                        else:
                            _bid_list.extend(section_struct_value["sizes"])
                    elif "accuracy" in section_struct_value:
                        print(section_struct_value)

        print(max(time_iterations))
        struct["METRICS"] = METRICS_STRUCTURE.copy()
        struct["METRICS"].update(
            {
                "TOTAL_TIME_TO_SEND_ORDER": sum(
                    [
                        sum(client_metrics[client]["TIME_TO_SEND_ORDER"])
                        for client in client_metrics
                    ]
                ),
                "TOTAL_TIME_TO_SEND_ORDER_NET": sum(
                    [
                        sum(client_metrics[client]["TIME_TO_SEND_ORDER_NET"])
                        for client in client_metrics
                    ]
                ),
                "TOTAL_ORDERS_SUBMITTED": len(total_orders),
                "TOTAL_ORDERS_SUBMITTED_BID": len(
                    [_order for _order in total_orders if _order["type"] == "BID"]
                ),
                "TOTAL_ORDERS_SUBMITTED_ASK": len(
                    [_order for _order in total_orders if _order["type"] == "ASK"]
                ),
                "TOTAL_ORDERS_EXECUTED": len(all_orders_executed),
                "FIRST_ORDER_SUBMITTED_DATE": first_submitted_date,
                "LAST_ORDER_SUBMITTED_DATE": last_submitted_date,
                "MAX_TIME_TO_COMPLETION": max(time_completion_bid + time_completion_ask)
                if time_completion_bid or time_completion_ask
                else None,
                "MAX_TIME_TO_COMPLETION_BID": max(time_completion_bid)
                if time_completion_bid
                else None,
                "MAX_TIME_TO_COMPLETION_ASK": max(time_completion_ask)
                if time_completion_ask
                else None,
                "MIN_TIME_TO_COMPLETION": min(time_completion_bid + time_completion_ask)
                if time_completion_bid or time_completion_ask
                else None,
                "MIN_TIME_TO_COMPLETION_BID": min(time_completion_bid)
                if time_completion_bid
                else None,
                "MIN_TIME_TO_COMPLETION_ASK": min(time_completion_ask)
                if time_completion_ask
                else None,
                "AVERAGE_TIME_TO_COMPLETION": sum(
                    time_completion_bid + time_completion_ask
                )
                / (len(time_completion_ask) + len(time_completion_bid))
                if time_completion_bid or time_completion_ask
                else None,
                "AVERAGE_TIME_TO_COMPLETION_BID": sum(time_completion_bid)
                / len(time_completion_bid)
                if time_completion_bid
                else None,
                "AVERAGE_TIME_TO_COMPLETION_ASK": sum(time_completion_ask)
                / len(time_completion_ask)
                if time_completion_ask
                else None,
                "TOTAL_TIME_TO_SORT": sum(time_sort_bid + time_sort_ask),
                "TOTAL_TIME_TO_SORT_BID": sum(time_sort_bid),
                "TOTAL_TIME_TO_SORT_ASK": sum(time_sort_ask),
                "MAX_TIME_TO_SORT": max(time_sort_bid + time_sort_ask)
                if time_sort_bid or time_sort_ask
                else None,
                "MAX_TIME_TO_SORT_BID": max(time_sort_bid) if time_sort_bid else None,
                "MAX_TIME_TO_SORT_ASK": max(time_sort_ask) if time_sort_ask else None,
                "MIN_TIME_TO_SORT": min(time_sort_bid + time_sort_ask)
                if time_sort_bid or time_sort_ask
                else None,
                "MIN_TIME_TO_SORT_BID": min(time_sort_bid) if time_sort_bid else None,
                "MIN_TIME_TO_SORT_ASK": min(time_sort_ask) if time_sort_ask else None,
                "AVERAGE_TIME_TO_SORT": (
                    sum(time_sort_bid + time_sort_ask)
                    / (len(time_sort_bid) + len(time_sort_ask))
                )
                if time_sort_bid or time_sort_ask
                else None,
                "AVERAGE_TIME_TO_SORT_BID": (sum(time_sort_bid) / len(time_sort_bid))
                if time_sort_bid
                else None,
                "AVERAGE_TIME_TO_SORT_ASK": (sum(time_sort_ask) / len(time_sort_ask))
                if time_sort_ask
                else None,
                "TOTAL_TIME_TO_INSERT_ORDERS": sum(
                    time_insert_orders_bid + time_insert_orders_ask
                ),
                "TOTAL_TIME_TO_INSERT_ORDERS_NET": sum(
                    time_insert_orders_bid_net + time_insert_orders_ask_net
                ),
                "TOTAL_TIME_TO_INSERT_ORDERS_BID": sum(time_insert_orders_bid),
                "TOTAL_TIME_TO_INSERT_ORDERS_BID_NET": sum(time_insert_orders_bid_net),
                "TOTAL_TIME_TO_INSERT_ORDERS_ASK": sum(time_insert_orders_ask),
                "TOTAL_TIME_TO_INSERT_ORDERS_ASK_NET": sum(time_insert_orders_ask_net),
                "TOTAL_VOLUME_SUBMITTED": sum(
                    [_order["volume"] for _order in all_orders.values()]
                ),
                "MAX_TIME_TO_INSERT_ORDERS": max(
                    time_insert_orders_bid + time_insert_orders_ask
                )
                if time_insert_orders_bid or time_insert_orders_ask
                else None,
                "MAX_TIME_TO_INSERT_ORDERS_BID": max(time_insert_orders_bid)
                if time_insert_orders_bid
                else None,
                "MAX_TIME_TO_INSERT_ORDERS_ASK": max(time_insert_orders_ask)
                if time_insert_orders_ask
                else None,
                "MIN_TIME_TO_INSERT_ORDERS": min(
                    time_insert_orders_bid + time_insert_orders_ask
                )
                if time_insert_orders_bid or time_insert_orders_ask
                else None,
                "MIN_TIME_TO_INSERT_ORDERS_BID": min(time_insert_orders_bid)
                if time_insert_orders_bid
                else None,
                "MIN_TIME_TO_INSERT_ORDERS_ASK": min(time_insert_orders_ask)
                if time_insert_orders_ask
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS": (
                    sum(time_insert_orders_bid + time_insert_orders_ask)
                    / (len(time_insert_orders_bid) + len(time_insert_orders_ask))
                )
                if time_insert_orders_bid or time_insert_orders_ask
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS_NET": (
                    sum(time_insert_orders_bid_net + time_insert_orders_ask_net)
                    / (
                        len(time_insert_orders_bid_net)
                        + len(time_insert_orders_ask_net)
                    )
                )
                if time_insert_orders_bid_net or time_insert_orders_ask_net
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS_BID": (
                    sum(time_insert_orders_bid) / (len(time_insert_orders_bid))
                )
                if time_insert_orders_bid
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS_BID_NET": (
                    sum(time_insert_orders_bid_net) / (len(time_insert_orders_bid_net))
                )
                if time_insert_orders_bid_net
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS_ASK": (
                    sum(time_insert_orders_ask) / (len(time_insert_orders_ask))
                )
                if time_insert_orders_ask
                else None,
                "AVERAGE_TIME_TO_INSERT_ORDERS_ASK_NET": (
                    sum(time_insert_orders_ask_net) / (len(time_insert_orders_ask_net))
                )
                if time_insert_orders_ask_net
                else None,
                "TOTAL_VOLUME_SUBMITTED_BID": sum(
                    [
                        _order["volume"]
                        for _order in all_orders.values()
                        if _order["type"] == "BID"
                    ]
                ),
                "TOTAL_VOLUME_SUBMITTED_ASK": sum(
                    [
                        _order["volume"]
                        for _order in all_orders.values()
                        if _order["type"] == "ASK"
                    ]
                ),
                "AVERAGE_VOLUME_SUBMITTED": sum(
                    [_order["volume"] for _order in all_orders.values()]
                )
                / len([_order["volume"] for _order in all_orders.values()]),
                "AVERAGE_VOLUME_SUBMITTED_BID": sum(
                    [
                        _order["volume"]
                        for _order in all_orders.values()
                        if _order["type"] == "BID"
                    ]
                )
                / len(
                    [
                        _order["volume"]
                        for _order in all_orders.values()
                        if _order["type"] == "BID"
                    ]
                ),
                "AVERAGE_VOLUME_SUBMITTED_ASK": (
                    sum(
                        [
                            _order["volume"]
                            for _order in all_orders.values()
                            if _order["type"] == "ASK"
                        ]
                    )
                    / len(
                        [
                            _order["volume"]
                            for _order in all_orders.values()
                            if _order["type"] == "ASK"
                        ]
                    )
                )
                if len(
                    [
                        _order["volume"]
                        for _order in all_orders.values()
                        if _order["type"] == "ASK"
                    ]
                )
                else None,
                "TOTAL_PRICE_SUBMITTED": sum(
                    [_order["price"] for _order in all_orders.values()]
                ),
                "TOTAL_PRICE_SUBMITTED_BID": sum(
                    [
                        _order["price"]
                        for _order in all_orders.values()
                        if _order["type"] == "BID"
                    ]
                ),
                "TOTAL_PRICE_SUBMITTED_ASK": sum(
                    [
                        _order["price"]
                        for _order in all_orders.values()
                        if _order["type"] == "ASK"
                    ]
                ),
                "AVERAGE_PRICE_SUBMITTED": sum(
                    [_order["price"] for _order in all_orders.values()]
                )
                / len([_order["price"] for _order in all_orders.values()]),
                "AVERAGE_PRICE_SUBMITTED_BID": (
                    sum(
                        [
                            _order["price"]
                            for _order in all_orders.values()
                            if _order["type"] == "BID"
                        ]
                    )
                    / len(
                        [
                            _order["price"]
                            for _order in all_orders.values()
                            if _order["type"] == "BID"
                        ]
                    )
                )
                if len(
                    [
                        _order["price"]
                        for _order in all_orders.values()
                        if _order["type"] == "BID"
                    ]
                )
                else None,
                "AVERAGE_PRICE_SUBMITTED_ASK": (
                    sum(
                        [
                            _order["price"]
                            for _order in all_orders.values()
                            if _order["type"] == "ASK"
                        ]
                    )
                    / len(
                        [
                            _order["price"]
                            for _order in all_orders.values()
                            if _order["type"] == "ASK"
                        ]
                    )
                )
                if len(
                    [
                        _order["price"]
                        for _order in all_orders.values()
                        if _order["type"] == "ASK"
                    ]
                )
                else None,
                "TOTAL_VOLUME_EXECUTED": sum(
                    [
                        _order["performed"]["volume"]
                        for _order in all_orders_executed.values()
                    ]
                ),
                "AVERAGE_VOLUME_EXECUTED": (
                    sum(
                        [
                            _order["performed"]["volume"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                    / len(
                        [
                            _order["performed"]["volume"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                )
                if len(
                    [
                        _order["performed"]["volume"]
                        for _order in all_orders_executed.values()
                    ]
                )
                else None,
                "TOTAL_PRICE_EXECUTED": sum(
                    [
                        _order["performed"]["price"] * _order["performed"]["volume"]
                        for _order in all_orders_executed.values()
                    ]
                ),
                "AVERAGE_PRICE_EXECUTED": (
                    sum(
                        [
                            _order["performed"]["price"] * _order["performed"]["volume"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                    / len(
                        [
                            _order["performed"]["price"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                )
                if len(
                    [
                        _order["performed"]["price"]
                        for _order in all_orders_executed.values()
                    ]
                )
                else None,
                "TOTAL_PRICE_PER_EXECUTED": sum(
                    [
                        _order["performed"]["price"]
                        for _order in all_orders_executed.values()
                    ]
                ),
                "AVERAGE_PRICE_PER_EXECUTED": (
                    sum(
                        [
                            _order["performed"]["price"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                    / len(
                        [
                            _order["performed"]["price"]
                            for _order in all_orders_executed.values()
                        ]
                    )
                )
                if len(
                    [
                        _order["performed"]["price"]
                        for _order in all_orders_executed.values()
                    ]
                )
                else None,
                "TOTAL_TIME_TO_MATCH_ORDER": sum(time_match_orders),
                "MAX_TIME_TO_MATCH_ORDER": max(time_match_orders)
                if time_match_orders
                else None,
                "MIN_TIME_TO_MATCH_ORDER": min(time_match_orders)
                if time_match_orders
                else None,
                "AVERAGE_TIME_TO_MATCH_ORDER": (
                    sum(time_match_orders) / (len(time_match_orders))
                )
                if time_match_orders
                else None,
                "TOTAL_TIME_TO_MATCH_ORDER_NET": sum(time_match_orders_net),
                "MAX_TIME_TO_MATCH_ORDER_NET": max(time_match_orders_net)
                if time_match_orders_net
                else None,
                "MIN_TIME_TO_MATCH_ORDER_NET": min(time_match_orders_net)
                if time_match_orders_net
                else None,
                "AVERAGE_TIME_TO_MATCH_ORDER_NET": (
                    sum(time_match_orders_net) / (len(time_match_orders_net))
                )
                if time_match_orders_net
                else None,
                "MAX_TIME_TO_DECRYPT_ORDER": max(time_decrypt_orders)
                if time_decrypt_orders
                else None,
                "MIN_TIME_TO_DECRYPT_ORDER": min(time_decrypt_orders)
                if time_decrypt_orders
                else None,
                "AVERAGE_TIME_TO_DECRYPT_ORDER": (
                    sum(time_decrypt_orders) / (len(time_decrypt_orders))
                )
                if time_decrypt_orders
                else None,
                "MAX_TIME_TO_GENERATE_CHALLENGES": max(time_generate_challenges)
                if time_generate_challenges
                else None,
                "MIN_TIME_TO_GENERATE_CHALLENGES": min(time_generate_challenges)
                if time_generate_challenges
                else None,
                "AVERAGE_TIME_TO_GENERATE_CHALLENGES": (
                    sum(time_generate_challenges) / (len(time_generate_challenges))
                )
                if time_generate_challenges
                else None,
                "TOTAL_TIME_TO_GET_MINIMUM_VALUE": sum(time_get_minimum_value)
                if time_get_minimum_value
                else None,
                "MAX_TIME_TO_GET_MINIMUM_VALUE": max(time_get_minimum_value)
                if time_get_minimum_value
                else None,
                "MIN_TIME_TO_GET_MINIMUM_VALUE": min(time_get_minimum_value)
                if time_get_minimum_value
                else None,
                "AVERAGE_TIME_TO_GET_MINIMUM_VALUE": (
                    sum(time_get_minimum_value) / (len(time_get_minimum_value))
                )
                if time_get_minimum_value
                else None,
                "TOTAL_TIME_TO_GET_MINIMUM_VALUE_NET": sum(time_get_minimum_value_net)
                if time_get_minimum_value_net
                else None,
                "MAX_TIME_TO_GET_MINIMUM_VALUE_NET": max(time_get_minimum_value_net)
                if time_get_minimum_value_net
                else None,
                "MIN_TIME_TO_GET_MINIMUM_VALUE_NET": min(time_get_minimum_value_net)
                if time_get_minimum_value_net
                else None,
                "AVERAGE_TIME_TO_GET_MINIMUM_VALUE_NET": (
                    sum(time_get_minimum_value_net) / (len(time_get_minimum_value_net))
                )
                if time_get_minimum_value_net
                else None,
                "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE": sum(
                    time_get_minimum_insert_value
                )
                if time_get_minimum_insert_value
                else None,
                "MAX_TIME_TO_GET_MINIMUM_INSERT_VALUE": max(
                    time_get_minimum_insert_value
                )
                if time_get_minimum_insert_value
                else None,
                "MIN_TIME_TO_GET_MINIMUM_INSERT_VALUE": min(
                    time_get_minimum_insert_value
                )
                if time_get_minimum_insert_value
                else None,
                "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE": (
                    sum(time_get_minimum_insert_value)
                    / (len(time_get_minimum_insert_value))
                )
                if time_get_minimum_insert_value
                else None,
                "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": sum(
                    time_get_minimum_insert_value_net
                )
                if time_get_minimum_insert_value_net
                else None,
                "MAX_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": max(
                    time_get_minimum_insert_value_net
                )
                if time_get_minimum_insert_value_net
                else None,
                "MIN_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": min(
                    time_get_minimum_insert_value_net
                )
                if time_get_minimum_insert_value_net
                else None,
                "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": (
                    sum(time_get_minimum_insert_value_net)
                    / (len(time_get_minimum_insert_value_net))
                )
                if time_get_minimum_insert_value_net
                else None,
                "TOTAL_TIME_TO_COMPARE_LOCAL": sum(time_compare_local)
                if time_compare_local
                else None,
                "MAX_TIME_TO_COMPARE_LOCAL": max(time_compare_local)
                if time_compare_local
                else None,
                "MIN_TIME_TO_COMPARE_LOCAL": min(time_compare_local)
                if time_compare_local
                else None,
                "AVERAGE_TIME_TO_COMPARE_LOCAL": (
                    sum(time_compare_local) / (len(time_compare_local))
                )
                if time_compare_local
                else None,
                "TOTAL_TIME_TO_COMPARE_LOCAL_NET": sum(time_compare_local_net)
                if time_compare_local_net
                else None,
                "MAX_TIME_TO_COMPARE_LOCAL_NET": max(time_compare_local_net)
                if time_compare_local_net
                else None,
                "MIN_TIME_TO_COMPARE_LOCAL_NET": min(time_compare_local_net)
                if time_compare_local_net
                else None,
                "AVERAGE_TIME_TO_COMPARE_LOCAL_NET": (
                    sum(time_compare_local_net) / (len(time_compare_local_net))
                )
                if time_compare_local_net
                else None,
                "TOTAL_TIME_TO_COMPARE_REMOTE": sum(time_compare_remote)
                if time_compare_remote
                else None,
                "MAX_TIME_TO_COMPARE_REMOTE": max(time_compare_remote)
                if time_compare_remote
                else None,
                "MIN_TIME_TO_COMPARE_REMOTE": min(time_compare_remote)
                if time_compare_remote
                else None,
                "AVERAGE_TIME_TO_COMPARE_REMOTE": (
                    sum(time_compare_remote) / (len(time_compare_remote))
                )
                if time_compare_remote
                else None,
                "TOTAL_TIME_TO_COMPARE_REMOTE_NET": sum(time_compare_remote_net)
                if time_compare_remote_net
                else None,
                "MAX_TIME_TO_COMPARE_REMOTE_NET": max(time_compare_remote_net)
                if time_compare_remote_net
                else None,
                "MIN_TIME_TO_COMPARE_REMOTE_NET": min(time_compare_remote_net)
                if time_compare_remote_net
                else None,
                "AVERAGE_TIME_TO_COMPARE_REMOTE_NET": (
                    sum(time_compare_remote_net) / (len(time_compare_remote_net))
                )
                if time_compare_remote_net
                else None,
                "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL": sum(time_compare_insert_local)
                if time_compare_insert_local
                else None,
                "MAX_TIME_TO_COMPARE_INSERT_LOCAL": max(time_compare_insert_local)
                if time_compare_insert_local
                else None,
                "MIN_TIME_TO_COMPARE_INSERT_LOCAL": min(time_compare_insert_local)
                if time_compare_insert_local
                else None,
                "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL": (
                    sum(time_compare_insert_local) / (len(time_compare_insert_local))
                )
                if time_compare_insert_local
                else None,
                "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL_NET": sum(
                    time_compare_insert_local_net
                )
                if time_compare_insert_local_net
                else None,
                "MAX_TIME_TO_COMPARE_INSERT_LOCAL_NET": max(
                    time_compare_insert_local_net
                )
                if time_compare_insert_local_net
                else None,
                "MIN_TIME_TO_COMPARE_INSERT_LOCAL_NET": min(
                    time_compare_insert_local_net
                )
                if time_compare_insert_local_net
                else None,
                "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL_NET": (
                    sum(time_compare_insert_local_net)
                    / (len(time_compare_insert_local_net))
                )
                if time_compare_insert_local_net
                else None,
                "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE": sum(time_compare_insert_remote)
                if time_compare_insert_remote
                else None,
                "MAX_TIME_TO_COMPARE_INSERT_REMOTE": max(time_compare_insert_remote)
                if time_compare_insert_remote
                else None,
                "MIN_TIME_TO_COMPARE_INSERT_REMOTE": min(time_compare_insert_remote)
                if time_compare_insert_remote
                else None,
                "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE": (
                    sum(time_compare_insert_remote) / (len(time_compare_insert_remote))
                )
                if time_compare_insert_remote
                else None,
                "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE_NET": sum(
                    time_compare_insert_remote_net
                )
                if time_compare_insert_remote_net
                else None,
                "MAX_TIME_TO_COMPARE_INSERT_REMOTE_NET": max(
                    time_compare_insert_remote_net
                )
                if time_compare_insert_remote_net
                else None,
                "MIN_TIME_TO_COMPARE_INSERT_REMOTE_NET": min(
                    time_compare_insert_remote_net
                )
                if time_compare_insert_remote_net
                else None,
                "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE_NET": (
                    sum(time_compare_insert_remote_net)
                    / (len(time_compare_insert_remote_net))
                )
                if time_compare_insert_remote_net
                else None,
                "MAX_TIME_TO_PAD_ORDERS_PRICE": max(
                    time_pad_orders_price_ask + time_pad_orders_price_bid
                )
                if time_pad_orders_price_bid or time_pad_orders_price_ask
                else None,
                "MIN_TIME_TO_PAD_ORDERS_PRICE": min(
                    time_pad_orders_price_ask + time_pad_orders_price_bid
                )
                if time_pad_orders_price_bid or time_pad_orders_price_ask
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_PRICE": (
                    sum(time_pad_orders_price_bid + time_pad_orders_price_ask)
                    / (len(time_pad_orders_price_bid) + len(time_pad_orders_price_ask))
                )
                if time_pad_orders_price_bid or time_pad_orders_price_ask
                else None,
                "MAX_TIME_TO_PAD_ORDERS_PRICE_BID": max(time_pad_orders_price_bid)
                if time_pad_orders_price_bid
                else None,
                "MIN_TIME_TO_PAD_ORDERS_PRICE_BID": min(time_pad_orders_price_bid)
                if time_pad_orders_price_bid
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_PRICE_BID": (
                    sum(time_pad_orders_price_bid) / (len(time_pad_orders_price_bid))
                )
                if time_pad_orders_price_bid
                else None,
                "MAX_TIME_TO_PAD_ORDERS_PRICE_ASK": max(time_pad_orders_price_ask)
                if time_pad_orders_price_ask
                else None,
                "MIN_TIME_TO_PAD_ORDERS_PRICE_ASK": min(time_pad_orders_price_ask)
                if time_pad_orders_price_ask
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_PRICE_ASK": (
                    sum(time_pad_orders_price_ask) / (len(time_pad_orders_price_ask))
                )
                if time_pad_orders_price_ask
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_PRICE": max(
                    time_unpad_orders_price_ask + time_unpad_orders_price_bid
                )
                if time_unpad_orders_price_bid or time_unpad_orders_price_ask
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_PRICE": min(
                    time_unpad_orders_price_ask + time_unpad_orders_price_bid
                )
                if time_unpad_orders_price_bid or time_unpad_orders_price_ask
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_PRICE": (
                    sum(time_unpad_orders_price_bid + time_unpad_orders_price_ask)
                    / (
                        len(time_unpad_orders_price_bid)
                        + len(time_unpad_orders_price_ask)
                    )
                )
                if time_unpad_orders_price_bid or time_unpad_orders_price_ask
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_PRICE_BID": max(time_unpad_orders_price_bid)
                if time_unpad_orders_price_bid
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_PRICE_BID": min(time_unpad_orders_price_bid)
                if time_unpad_orders_price_bid
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_PRICE_BID": (
                    sum(time_unpad_orders_price_bid)
                    / (len(time_unpad_orders_price_bid))
                )
                if time_unpad_orders_price_bid
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_PRICE_ASK": max(time_unpad_orders_price_ask)
                if time_unpad_orders_price_ask
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_PRICE_ASK": min(time_unpad_orders_price_ask)
                if time_unpad_orders_price_ask
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_PRICE_ASK": (
                    sum(time_unpad_orders_price_ask)
                    / (len(time_unpad_orders_price_ask))
                )
                if time_unpad_orders_price_ask
                else None,
                "MAX_TIME_TO_PAD_ORDERS_VOLUME": max(
                    time_pad_orders_volume_ask + time_pad_orders_volume_bid
                )
                if time_pad_orders_volume_bid or time_pad_orders_volume_ask
                else None,
                "MIN_TIME_TO_PAD_ORDERS_VOLUME": min(
                    time_pad_orders_volume_ask + time_pad_orders_volume_bid
                )
                if time_pad_orders_volume_bid or time_pad_orders_volume_ask
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_VOLUME": (
                    sum(time_pad_orders_volume_bid + time_pad_orders_volume_ask)
                    / (
                        len(time_pad_orders_volume_bid)
                        + len(time_pad_orders_volume_ask)
                    )
                )
                if time_pad_orders_volume_bid or time_pad_orders_volume_ask
                else None,
                "MAX_TIME_TO_PAD_ORDERS_VOLUME_BID": max(time_pad_orders_volume_bid)
                if time_pad_orders_volume_bid
                else None,
                "MIN_TIME_TO_PAD_ORDERS_VOLUME_BID": min(time_pad_orders_volume_bid)
                if time_pad_orders_volume_bid
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_VOLUME_BID": (
                    sum(time_pad_orders_volume_bid) / (len(time_pad_orders_volume_bid))
                )
                if time_pad_orders_volume_bid
                else None,
                "MAX_TIME_TO_PAD_ORDERS_VOLUME_ASK": max(time_pad_orders_volume_ask)
                if time_pad_orders_volume_ask
                else None,
                "MIN_TIME_TO_PAD_ORDERS_VOLUME_ASK": min(time_pad_orders_volume_ask)
                if time_pad_orders_volume_ask
                else None,
                "AVERAGE_TIME_TO_PAD_ORDERS_VOLUME_ASK": (
                    sum(time_pad_orders_volume_ask) / (len(time_pad_orders_volume_ask))
                )
                if time_pad_orders_volume_ask
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_VOLUME": max(
                    time_unpad_orders_volume_ask + time_unpad_orders_volume_bid
                )
                if time_unpad_orders_volume_bid or time_unpad_orders_volume_ask
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_VOLUME": min(
                    time_unpad_orders_volume_ask + time_unpad_orders_volume_bid
                )
                if time_unpad_orders_volume_bid or time_unpad_orders_volume_ask
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_VOLUME": (
                    sum(time_unpad_orders_volume_bid + time_unpad_orders_volume_ask)
                    / (
                        len(time_unpad_orders_volume_bid)
                        + len(time_unpad_orders_volume_ask)
                    )
                )
                if time_unpad_orders_volume_bid or time_unpad_orders_volume_ask
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_VOLUME_BID": max(time_unpad_orders_volume_bid)
                if time_unpad_orders_volume_bid
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_VOLUME_BID": min(time_unpad_orders_volume_bid)
                if time_unpad_orders_volume_bid
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_VOLUME_BID": (
                    sum(time_unpad_orders_volume_bid)
                    / (len(time_unpad_orders_volume_bid))
                )
                if time_unpad_orders_volume_bid
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_VOLUME_ASK": max(time_unpad_orders_volume_ask)
                if time_unpad_orders_volume_ask
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_VOLUME_ASK": min(time_unpad_orders_volume_ask)
                if time_unpad_orders_volume_ask
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_VOLUME_ASK": (
                    sum(time_unpad_orders_volume_ask)
                    / (len(time_unpad_orders_volume_ask))
                )
                if time_unpad_orders_volume_ask
                else None,
                "MAX_TIME_TO_UNPAD_ORDERS_VOLUME_MINIMUM": max(
                    time_unpad_orders_volume_minimum
                )
                if time_unpad_orders_volume_minimum
                else None,
                "MIN_TIME_TO_UNPAD_ORDERS_VOLUME_MINIMUM": min(
                    time_unpad_orders_volume_minimum
                )
                if time_unpad_orders_volume_minimum
                else None,
                "AVERAGE_TIME_TO_UNPAD_ORDERS_VOLUME_MINIMUM": (
                    sum(time_unpad_orders_volume_minimum)
                    / (len(time_unpad_orders_volume_minimum))
                )
                if time_unpad_orders_volume_minimum
                else None,
                "MAX_SIZE_ORDERS": max(size_orders) if size_orders else None,
                "MIN_SIZE_ORDERS": min(size_orders) if size_orders else None,
                "AVERAGE_SIZE_ORDERS": (sum(size_orders) / (len(size_orders)))
                if size_orders
                else None,
                "MAX_SIZE_CHALLENGES": max(size_challenges)
                if size_challenges
                else None,
                "MIN_SIZE_CHALLENGES": min(size_challenges)
                if size_challenges
                else None,
                "AVERAGE_SIZE_CHALLENGES": (
                    sum(size_challenges) / (len(size_challenges))
                )
                if size_challenges
                else None,
                "TIME_COMPARE_LOCAL_NET_COUNT": len(time_compare_local_net),
                "TIME_COMPARE_LOCAL_NET_SUM": sum(time_compare_local_net),
                "TIME_COMPARE_REMOTE_NET_COUNT": len(time_compare_remote_net),
                "TIME_COMPARE_REMOTE_NET_SUM": sum(time_compare_remote_net),
                "EFFECTIVE_RUN_TIME": max(time_iterations),
                "TOTAL_TIME_NET": sum(
                    time_compare_local_net
                    + time_compare_insert_local_net
                    + time_compare_remote_net
                    + time_compare_insert_remote_net
                    + [
                        sum(client_metrics[client]["TIME_TO_SEND_ORDER_NET"])
                        for client in client_metrics
                    ],
                ),
                "TOTAL_TIME_EXCHANGE_NET": sum(time_match_orders_net),
            }
        )

    with open(str(Path(path_output)), mode="w+") as report_file:
        report_file.write(json.dumps(struct))


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange Reports",
        description="Generate a summary from the outputted summaries from each component",
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
        "-p:o",
        "--path-output",
        help="Path to the output for the report",
        type=str,
        required=True,
    )
    args = parser.parse_args()

    create_report(
        path_exchange=args.path_exchange,
        path_clients=args.path_clients,
        path_output=args.path_output,
    )
