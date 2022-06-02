#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from collections import OrderedDict
import json


def write_roman(num):

    roman = OrderedDict()
    roman[1000] = "M"
    roman[900] = "CM"
    roman[500] = "D"
    roman[400] = "CD"
    roman[100] = "C"
    roman[90] = "XC"
    roman[50] = "L"
    roman[40] = "XL"
    roman[10] = "X"
    roman[9] = "IX"
    roman[5] = "V"
    roman[4] = "IV"
    roman[1] = "I"

    def roman_num(num):
        for r in roman.keys():
            x, y = divmod(num, r)
            yield roman[r] * x
            num -= r * x
            if num <= 0:
                break

    return "".join([a for a in roman_num(num)])


def create_tables(input_file, categories):
    struct = {}
    with open(input_file, "r") as file_obj:
        data = json.load(file_obj)
        for section, section_data in data.items():
            for category in categories:
                for container, container_data in section_data[category].items():
                    for configuration, configuration_data in container_data.items():
                        if "-" in configuration:
                            configuration, roman = configuration.split("-")
                            roman = write_roman(int(roman))
                        else:
                            roman = ""

                        key = f"{configuration.lower()}{roman}{section[:3].lower()}{container.lower()}{category.lower().replace('-', '')}"
                        struct[key] = {"x": [], "y": [], "y-max": [], "y-min": []}
                        for challenges, challenges_data in configuration_data.items():
                            if challenges_data["mean"] is None:
                                continue

                            struct[key]["x"].append(challenges)
                            struct[key]["y"].append(challenges_data["mean"])
                            struct[key]["y-max"].append(challenges_data["dev"])
                            struct[key]["y-min"].append(challenges_data["dev"])

                        if not struct[key]["x"]:
                            del struct[key]

    tables = []
    table_header = "\pgfplotstableread{\n"
    table_footer = "\n}}{{\{table_name}}}\n"
    for table_name, table_data in struct.items():
        tables.append(
            table_header
            + " ".join([key for key in table_data.keys()])
            + "\n"
            + "\n".join(
                [
                    " ".join(
                        [
                            str(table_data["x"][index]),
                            str(table_data["y"][index]),
                            str(table_data["y-max"][index]),
                            str(table_data["y-min"][index]),
                        ]
                    )
                    for index in range(len(table_data["x"]))
                ]
            )
            + table_footer.format(table_name=table_name)
        )

    return "\n".join(tables)


if __name__ == "__main__":
    parser = ArgumentParser("PET-Exchange: Create LaTeX tables from collected data")
    parser.add_argument("RESULTS_FILE")
    args = parser.parse_args()

    with open("trades/remote4.txt", "w+") as file_obj:
        file_obj.write(create_tables(args.RESULTS_FILE, ["REMOTE", "REMOTE-NET"]))

    with open("trades/local4.txt", "w+") as file_obj:
        file_obj.write(create_tables(args.RESULTS_FILE, ["LOCAL", "LOCAL-NET"]))
