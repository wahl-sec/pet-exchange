#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from statistics import mean, stdev
import json


def fix_formatting(input_file):
    struct = {}
    with open(input_file, "r") as file_obj:
        data = json.load(file_obj)
        for chapter, chapter_data in data.items():
            struct[chapter] = {}
            for section, section_data in chapter_data.items():
                struct[chapter][section] = {}
                for category, category_data in section_data.items():
                    struct[chapter][section][category] = {}
                    for container, container_data in category_data.items():
                        struct[chapter][section][category][container] = {}
                        for configuration, configuration_data in container_data.items():
                            if category in ["VSPTTF", "VMPTFF", "VEPTTF"]:
                                configuration_data["values"] = [
                                    configuration_data["values"][index]
                                    / struct[chapter][section]["TTF"][container][
                                        configuration
                                    ]["values"][index]
                                    for index in range(
                                        len(configuration_data["values"])
                                    )
                                ]

                            struct[chapter][section][category][container][
                                configuration
                            ] = {
                                "values": configuration_data["values"],
                                "mean": mean(configuration_data["values"])
                                if configuration_data["values"]
                                else None,
                                "dev": stdev(configuration_data["values"])
                                if len(configuration_data["values"]) > 1
                                else None,
                            }

    return json.dumps(struct)


if __name__ == "__main__":
    parser = ArgumentParser("PET-Exchange: Create LaTeX tables from collected data")
    parser.add_argument("RESULTS_FILE")
    args = parser.parse_args()

    with open("trades/data3.json", "w+") as file_obj:
        file_obj.write(fix_formatting(args.RESULTS_FILE))
