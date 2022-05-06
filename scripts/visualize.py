#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any, List, Tuple
from argparse import ArgumentParser
from pathlib import Path
import json

import matplotlib.pyplot as plot


def visualize(
    data: List[Any],
    key_components: List[str],
    filename: str,
    size: Tuple[int, int],
    labels: Tuple[str, str],
) -> None:
    plot.style.use("_mpl-gallery")

    for key in key_components:
        data = data[key]

    if any(isinstance(value, dict) for value in data.values()):
        nrows = int(len(data.keys()) // 2)
        ncols = int(len(data.keys()) / nrows)
    else:
        nrows = 1
        ncols = 1

    fig, axs = plot.subplots(nrows=nrows, ncols=ncols, figsize=size, tight_layout=True)

    if (nrows, ncols) == (1, 1):
        axs.plot(data.keys(), data.values())
        axs.set_title(key_components[-1])
        fig.suptitle(key_components[-2])
        if labels[0]:
            axs.set_xlabel(labels[0])

        if labels[1]:
            axs.set_ylabel(labels[1])
    else:
        fig.suptitle(key_components[-1])
        if nrows == 1:
            axs = [axs]
        for y_index, row in enumerate(axs):
            for x_index, col in enumerate(row):
                results = list(data.values())[x_index + (y_index * 2)]
                col.plot(results.keys(), results.values())
                col.set_title(list(data.keys())[x_index + (y_index * 2)])
                if labels[0]:
                    col.set_xlabel(labels[0])

                if labels[1]:
                    col.set_ylabel(labels[1])

    if filename:
        plot.savefig(filename)


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
    parser.add_argument(
        "-o:f", "--output-file", help="The output file containing the plots"
    )
    parser.add_argument(
        "-f:k",
        "--file-key",
        help="Key to field to plot, if the value of the key contains multiple non-integers then they are treated as subplots",
        required=True,
    )
    parser.add_argument(
        "-f:x",
        "--figure-size-x",
        help="Figure size in number of rows",
        default=8,
        type=int,
    )
    parser.add_argument(
        "-f:y",
        "--figure-size-y",
        help="Figure size in number of columns",
        default=8,
        type=int,
    )
    parser.add_argument(
        "-y:l",
        "--y-label",
        help="Figure y label",
        default="",
        type=str,
    )
    parser.add_argument(
        "-x:l",
        "--x-label",
        help="Figure x label",
        default="",
        type=str,
    )
    parser.add_argument(
        "-t",
        "--type",
        help="Type of plot to visualize, defaults to line charts",
        default="line",
        choices=["line"],
    )
    args = parser.parse_args()

    _path = Path(args.input_file)
    if not _path.exists():
        raise FileNotFoundError

    if "." in args.file_key:
        key = args.file_key.split(".")
    else:
        key = [args.file_key]

    with _path.open(mode="r") as file_obj:
        data = json.load(file_obj)

    visualize(
        data,
        key_components=key,
        filename=args.output_file,
        size=(args.figure_size_x, args.figure_size_y),
        labels=(args.x_label, args.y_label),
    )
