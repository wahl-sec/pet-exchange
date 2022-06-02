#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from statistics import mean, stdev
import pprint

from summary import create_summary


sections = ["CONTINOUS", "ONE-ITERATION"]
categories = {
    "REMOTE": "REM",
    "LOCAL": "LOC",
    "REMOTE-NET": "REM",
    "LOCAL-NET": "LOC",
}
containers = [
    "TTF",
    "TTM",
    "ATTM",
    "TTI",
    "ATTI",
    "TTSC",
    "ATTSC",
    "VSPTTF",
    "VMPTTF",
    "VEPTTF",
]
configurations = ["CKKS-11", "CKKS-12", "CKKS-14", "CKKS-15", "PLAIN"]
challenges = ["0", "1", "5", "10"]
summary_value_default = {
    "TTM": "TOTAL_TIME_TO_MATCH_ORDER",
    "TTI": "TOTAL_TIME_TO_INSERT_ORDERS",
    "TTSC": "TOTAL_TIME_TO_GET_MINIMUM_VALUE",
    "ATTM": "AVERAGE_TIME_TO_MATCH_ORDER",
    "ATTI": "AVERAGE_TIME_TO_INSERT_ORDERS",
    "ATTSC": "AVERAGE_TIME_TO_GET_MINIMUM_VALUE",
}
summary_value_net = {
    "TTM": "TOTAL_TIME_TO_MATCH_ORDER_NET",
    "TTI": "TOTAL_TIME_TO_INSERT_ORDERS_NET",
    "TTSC": "TOTAL_TIME_TO_GET_MINIMUM_VALUE_NET",
    "ATTM": "AVERAGE_TIME_TO_MATCH_ORDER_NET",
    "ATTI": "AVERAGE_TIME_TO_INSERT_ORDERS_NET",
    "ATTSC": "AVERAGE_TIME_TO_GET_MINIMUM_VALUE_NET",
}

data = {
    section: {
        category: {
            container: {
                configuration: {
                    challenge: {
                        "values": [],
                        "mean": None,
                        "dev": None,
                    }
                    for challenge in challenges
                }
                for configuration in configurations
            }
            for container in containers
        }
        for category, name in categories.items()
    }
    for section in sections
}

for folder in ["thesis-new-6"]:
    for section in sections:
        for category in ["remote", "local"]:
            for configuration in configurations:
                if configuration in ["CKKS-11", "CKKS-12"] and category == "local":
                    continue

                for challenge in challenges:
                    if (
                        configuration == "PLAIN"
                        and challenge != "0"
                        and category == "local"
                    ):
                        continue

                    summary = create_summary(
                        f"trades/{folder}/{section.lower()}/{configuration.lower()}/{category}/direct/{challenge}/report.json"
                    )

                    for container in containers:
                        if container in ["TTF", "VSPTTF", "VMPTTF", "VEPTTF"]:
                            continue

                        struct = data[section][category.upper()][container][
                            configuration
                        ][challenge]
                        if (
                            summary["DEFAULT"][summary_value_default[container]]
                            is not None
                        ):
                            struct["values"].append(
                                summary["DEFAULT"][summary_value_default[container]]
                            )

                        if len(struct["values"]) >= 1:
                            struct["mean"] = mean(struct["values"])

                        if len(struct["values"]) >= 2:
                            struct["dev"] = stdev(struct["values"])
                        else:
                            struct["dev"] = sum(struct["values"]) * 0.04

                        struct = data[section][f"{category.upper()}-NET"][container][
                            configuration
                        ][challenge]
                        struct["values"].append(
                            (
                                summary["DEFAULT"][summary_value_default[container]]
                                if summary["DEFAULT"][summary_value_default[container]]
                                is not None
                                else 0
                            )
                            - (
                                summary["NET"][summary_value_net[container]]
                                if summary["NET"][summary_value_net[container]]
                                is not None
                                else 0
                            )
                        )

                        if len(struct["values"]) >= 1:
                            struct["mean"] = mean(struct["values"])

                        if len(struct["values"]) >= 2:
                            struct["dev"] = stdev(struct["values"])
                        else:
                            struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][category.upper()]["TTF"][configuration][
                        challenge
                    ]
                    struct["values"].append(summary["DEFAULT"]["EFFECTIVE_RUN_TIME"])

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][f"{category.upper()}-NET"]["TTF"][
                        configuration
                    ][challenge]
                    struct["values"].append(
                        summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                        - (
                            summary["NET"]["TOTAL_TIME_NET"]
                            if section == "CONTINOUS"
                            else summary["NET"]["TOTAL_TIME_EXCHANGE_NET"]
                        )
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][category.upper()]["VSPTTF"][configuration][
                        challenge
                    ]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_SUBMITTED"]
                        / summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][f"{category.upper()}-NET"]["VSPTTF"][
                        configuration
                    ][challenge]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_SUBMITTED"]
                        / (
                            summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                            - (
                                summary["NET"]["TOTAL_TIME_NET"]
                                if section == "CONTINOUS"
                                else summary["NET"]["TOTAL_TIME_EXCHANGE_NET"]
                            )
                        )
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][category.upper()]["VMPTTF"][configuration][
                        challenge
                    ]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_MATCHED"]
                        / summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][f"{category.upper()}-NET"]["VMPTTF"][
                        configuration
                    ][challenge]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_MATCHED"]
                        / (
                            summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                            - (
                                summary["NET"]["TOTAL_TIME_NET"]
                                if section == "CONTINOUS"
                                else summary["NET"]["TOTAL_TIME_EXCHANGE_NET"]
                            )
                        )
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][category.upper()]["VEPTTF"][configuration][
                        challenge
                    ]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_EXECUTED"]
                        / summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

                    struct = data[section][f"{category.upper()}-NET"]["VEPTTF"][
                        configuration
                    ][challenge]
                    struct["values"].append(
                        summary["DEFAULT"]["TOTAL_VOLUME_EXECUTED"]
                        / (
                            summary["DEFAULT"]["EFFECTIVE_RUN_TIME"]
                            - (
                                summary["NET"]["TOTAL_TIME_NET"]
                                if section == "CONTINOUS"
                                else summary["NET"]["TOTAL_TIME_EXCHANGE_NET"]
                            )
                        )
                    )

                    if len(struct["values"]) >= 1:
                        struct["mean"] = mean(struct["values"])

                    if len(struct["values"]) >= 2:
                        struct["dev"] = stdev(struct["values"])
                    else:
                        struct["dev"] = sum(struct["values"]) * 0.04

import json

with open("trades/data4.json", "w+") as file_obj:
    file_obj.write(json.dumps(data))
