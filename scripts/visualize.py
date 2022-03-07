#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any, List
from argparse import ArgumentParser
from pathlib import Path
import json

import matplotlib.pyplot as plot


def visualize(data: List[Any], key: str, filename: str) -> None:
    plot.style.use("_mpl-gallery")
    x = list(range(len([1 for order in data if order["instrument"] == "BCA"])))
    y = [order[key] for order in data if order["instrument"] == "BCA"]

    fig, ax = plot.subplots(tight_layout=True, figsize=(15, 7))
    ax.plot(x, y)
    plot.savefig(f"images/{filename}.png", dpi=200)


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange Visualizer",
        description="Visualize market orders on a plot",
    )
    parser.add_argument(
        "-i:f",
        "--input-file",
        help="The market report file describing the orders to visualize",
    )
    args = parser.parse_args()

    _path = Path(args.input_file)
    if not _path.exists():
        raise FileNotFoundError

    with _path.open(mode="r") as file_obj:
        data = json.load(file_obj)

    for client, orders in data["CLIENTS"].items():
        visualize(orders, "price", client)
