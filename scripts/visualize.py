#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any, List, Tuple
from argparse import ArgumentParser
from pathlib import Path
from statistics import mean, stdev
import json

import matplotlib.pyplot as plot


def visualize(
    data: List[Any],
    key_components: List[str],
    filename: str,
    size: Tuple[int, int],
    labels: Tuple[str, str],
    ylim: int = None,
) -> None:
    plot.style.use("ggplot")

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
        values = []
        value_dev = []
        for value in data.values():
            values.append(mean(value) if value else 0)
            value_dev.append(stdev(value) if value else 0)

        axs.bar(
            data.keys(),
            values,
            yerr=value_dev,
            log=2,
            capsize=5,
        )
        _max_y = max(values)

        if labels[0]:
            axs.set_xlabel(labels[0])

        if labels[1]:
            axs.set_ylabel(labels[1])
    else:
        fig.suptitle(key_components[-1])
        _max_y = 0
        if nrows == 1:
            axs = [axs]
        for y_index, row in enumerate(axs):
            for x_index, col in enumerate(row):
                results = list(data.values())[x_index + (y_index * 2)]
                if max(map(max, results.values())) > _max_y:
                    _max_y = max(map(max, results.values()))

                col.bar(results.keys(), results.values(), log=2)
                col.set_title(list(data.keys())[x_index + (y_index * 2)])
                if labels[0]:
                    col.set_xlabel(labels[0])

                if labels[1]:
                    col.set_ylabel(labels[1])

    if ylim:
        plot.setp(axs, ylim=[0, ylim])
    else:
        plot.setp(axs, ylim=[0, _max_y])

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
    parser.add_argument(
        "-l", "--limit", help="The maximum limit of the y-axis", type=int
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
        ylim=args.limit,
    )
