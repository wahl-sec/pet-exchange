#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import pprint
import json


def create_summary(path: str) -> None:
    summary = {"DEFAULT": {}, "NET": {}}
    with open(path, "r") as file_obj:
        report = json.load(file_obj)
        summary["DEFAULT"].update(
            {
                "EFFECTIVE_RUN_TIME": report["METRICS"]["EFFECTIVE_RUN_TIME"],
                "AVERAGE_TIME_TO_COMPLETION": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPLETION"
                ],
                "TOTAL_TIME_TO_COMPARE_LOCAL": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_LOCAL"
                ],
                "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL"
                ],
                "TOTAL_TIME_TO_COMPARE_REMOTE": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_REMOTE"
                ],
                "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE"
                ],
                "TOTAL_TIME_TO_MATCH_ORDER": report["METRICS"][
                    "TOTAL_TIME_TO_MATCH_ORDER"
                ],
                "TOTAL_TIME_TO_INSERT_ORDERS": report["METRICS"][
                    "TOTAL_TIME_TO_INSERT_ORDERS"
                ],
                "TOTAL_TIME_TO_GET_MINIMUM_VALUE": report["METRICS"][
                    "TOTAL_TIME_TO_GET_MINIMUM_VALUE"
                ],
                "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE": report["METRICS"][
                    "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE"
                ],
                "AVERAGE_TIME_TO_MATCH_ORDER": report["METRICS"][
                    "AVERAGE_TIME_TO_MATCH_ORDER"
                ],
                "AVERAGE_TIME_TO_INSERT_ORDERS": report["METRICS"][
                    "AVERAGE_TIME_TO_INSERT_ORDERS"
                ],
                "AVERAGE_TIME_TO_GET_MINIMUM_VALUE": report["METRICS"][
                    "AVERAGE_TIME_TO_GET_MINIMUM_VALUE"
                ],
                "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE": report["METRICS"][
                    "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE"
                ],
                "AVERAGE_TIME_TO_COMPARE_LOCAL": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_LOCAL"
                ],
                "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL"
                ],
                "AVERAGE_TIME_TO_COMPARE_REMOTE": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_REMOTE"
                ],
                "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE"
                ],
                "TOTAL_VOLUME_SUBMITTED": report["METRICS"]["TOTAL_VOLUME_SUBMITTED"],
                "TOTAL_VOLUME_MATCHED": sum(
                    [
                        report["CLIENTS"][client]["METRICS"]["TOTAL_VOLUME_MATCHED"]
                        for client in report["CLIENTS"]
                    ]
                ),
                "TOTAL_VOLUME_EXECUTED": report["METRICS"]["TOTAL_VOLUME_EXECUTED"],
                "TOTAL_TIME_TO_SEND_ORDER": report["METRICS"][
                    "TOTAL_TIME_TO_SEND_ORDER"
                ],
            }
        )

        summary["NET"].update(
            {
                "TOTAL_TIME_NET": report["METRICS"]["TOTAL_TIME_NET"],
                "TOTAL_TIME_EXCHANGE_NET": report["METRICS"]["TOTAL_TIME_EXCHANGE_NET"],
                "TOTAL_TIME_TO_COMPARE_LOCAL_NET": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_LOCAL_NET"
                ],
                "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL_NET": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_INSERT_LOCAL_NET"
                ],
                "TOTAL_TIME_TO_COMPARE_REMOTE_NET": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_REMOTE_NET"
                ],
                "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE_NET": report["METRICS"][
                    "TOTAL_TIME_TO_COMPARE_INSERT_REMOTE_NET"
                ],
                "TOTAL_TIME_TO_MATCH_ORDER_NET": report["METRICS"][
                    "TOTAL_TIME_TO_MATCH_ORDER_NET"
                ],
                "TOTAL_TIME_TO_INSERT_ORDERS_NET": report["METRICS"][
                    "TOTAL_TIME_TO_INSERT_ORDERS_NET"
                ],
                "TOTAL_TIME_TO_GET_MINIMUM_VALUE_NET": report["METRICS"][
                    "TOTAL_TIME_TO_GET_MINIMUM_VALUE_NET"
                ],
                "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": report["METRICS"][
                    "TOTAL_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET"
                ],
                "AVERAGE_TIME_TO_MATCH_ORDER_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_MATCH_ORDER_NET"
                ],
                "AVERAGE_TIME_TO_INSERT_ORDERS_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_INSERT_ORDERS_NET"
                ],
                "AVERAGE_TIME_TO_COMPARE_LOCAL_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_LOCAL_NET"
                ],
                "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_INSERT_LOCAL_NET"
                ],
                "AVERAGE_TIME_TO_COMPARE_REMOTE_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_REMOTE_NET"
                ],
                "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_COMPARE_INSERT_REMOTE_NET"
                ],
                "AVERAGE_TIME_TO_GET_MINIMUM_VALUE_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_GET_MINIMUM_VALUE_NET"
                ],
                "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET": report["METRICS"][
                    "AVERAGE_TIME_TO_GET_MINIMUM_INSERT_VALUE_NET"
                ],
                "TOTAL_TIME_TO_SEND_ORDER_NET": report["METRICS"][
                    "TOTAL_TIME_TO_SEND_ORDER_NET"
                ],
            }
        )

    return summary


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange Summary",
        description="Generate a summary from the outputted report",
    )
    parser.add_argument(
        "-p:r",
        "--path-report",
        help="Path to the report",
        type=str,
        required=True,
    )
    args = parser.parse_args()

    pprint.pp(create_summary(args.path_report))
