#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import List, Dict, Any, Optional, Union, Literal, Tuple
from argparse import ArgumentParser, ArgumentTypeError
from dataclasses import dataclass, asdict
from string import ascii_uppercase
from random import randint, random, uniform, choice
from copy import deepcopy
from pathlib import Path
import json

from numpy.random import choice as np_choice

GENERAL_POINT = None
PEAK_PROBABILITY = 0.05
DIP_PROBABILITY = 0.05
MINIMUM_TRADES = 10
MAXIMUM_TRADES = 20
MINIMUM_ENTITIES = 2
MAXIMUM_ENTITIES = 5
MINIMUM_PRICE = 20.0
MAXIMUM_PRICE = None
MINIMUM_VOLUME = 50
MAXIMUM_VOLUME = 250
MINIMUM_OFFSET = None
MAXIMUM_OFFSET = None
INSTRUMENT_NAME = "COMP"
TYPE_RATIO = 0.5
VOLUME_RATIO = 0.5
NEXT_ORDER_PEAK_PROBABILITY = 0.4
NEXT_ORDER_DIP_PROBABILITY = 0.4
PEAK_OFFSET = 0.05
DIP_OFFSET = 0.05


@dataclass
class Order:
    offset: Optional[int]
    type: str
    instrument: str
    volume: int
    price: float
    is_peak: bool = False
    is_dip: bool = False


@dataclass
class Entity:
    entity: str
    orders: List[Order]
    instruments: List[Order]
    trade_count: int
    min_price: float
    max_price: Optional[float]
    min_volume: int
    max_volume: int
    min_offset: Optional[int]
    max_offset: Optional[int]
    market: List[Order]
    peak_probability: float
    dip_probability: float

    def add_order(self, order: Order) -> None:
        self.orders.append(order)
        self.market.append(order)

    def _generate_order_offset(self) -> Union[int, None]:
        if self.min_offset is not None and self.max_offset is not None:
            return (
                randint(self.min_offset, self.max_offset) + self.orders[-1].offset
                if self.orders
                else 0
            )
        elif self.min_offset is not None:
            return self.min_offset + self.orders[-1].offset if self.orders else 0
        elif self.max_offset is not None:
            return self.max_offset + self.orders[-1].offset if self.orders else 0
        else:
            return None

    def _determine_order_type(
        self, instrument: str
    ) -> Union[Literal["BID"], Literal["ASK"]]:
        bid_type: int = 0
        ask_type: int = 0

        for order in self.market:
            if order.instrument == instrument:
                bid_type += 1 if order.type == "BID" else 0
                ask_type += 1 if order.type == "ASK" else 0

        if bid_type == 0 and ask_type == 0:
            return choice(["ASK", "BID"])

        if (bid_type / (bid_type + ask_type)) < TYPE_RATIO:
            return "BID"

        return "ASK"

    def _determine_order_instrument(self) -> str:
        if not self.market:
            return choice(self.instruments)[0]

        latest_orders: Dict[str, Order] = {}
        for order in reversed(self.market):
            if order.instrument not in latest_orders:
                latest_orders[order.instrument] = order

        for instrument, _ in self.instruments:
            if instrument not in latest_orders:
                latest_orders[instrument] = None

        chances: Dict[str, float] = {}
        if not any(
            (order.is_peak or order.is_dip)
            for order in latest_orders.values()
            if order is not None
        ):
            for instrument, order in latest_orders.items():
                chances[instrument] = 1 / len(latest_orders)
        else:
            for instrument, order in latest_orders.items():
                if order is None:
                    chances[instrument] = 1 / len(latest_orders)
                elif order.is_peak:
                    chances[instrument] = (1 / len(latest_orders)) - (
                        (
                            NEXT_ORDER_PEAK_PROBABILITY
                            if (1 / len(latest_orders)) >= NEXT_ORDER_PEAK_PROBABILITY
                            else (1 / len(latest_orders))
                        )
                    )
                elif order.is_dip:
                    chances[instrument] = (1 / len(latest_orders)) - (
                        (
                            NEXT_ORDER_DIP_PROBABILITY
                            if (1 / len(latest_orders)) >= NEXT_ORDER_DIP_PROBABILITY
                            else (1 / len(latest_orders))
                        )
                    )

        remainder: float = 1 - sum(chances.values())
        for instrument in chances.keys():
            chances[instrument] += remainder / len(chances)

        return np_choice(list(chances.keys()), 1, p=list(chances.values()))[0]

    def _determine_order_volume(
        self, instrument: str, order_type: Literal["ASK", "BID"]
    ) -> int:
        bid_volume: int = 0
        ask_volume: int = 0

        for order in self.market:
            if order.instrument == instrument:
                bid_volume += order.volume if order.type == "BID" else 0
                ask_volume += order.volume if order.type == "ASK" else 0

        if ask_volume == 0 and bid_volume == 0:
            return randint(self.min_volume, self.max_volume)

        # TODO: Not really good at any ratio above > 0.6 but that could be because of the volumes used.

        if order_type == "BID":
            if (bid_volume / (bid_volume + ask_volume)) < VOLUME_RATIO:
                return randint(
                    self.max_volume * 0.8
                    if self.max_volume * 0.8 > self.min_volume
                    else self.min_volume,
                    self.max_volume,
                )
            else:
                return self.min_volume

        return randint(
            self.min_volume,
            self.max_volume,
        )

    def _determine_order_price(
        self, instrument: str, is_peak: bool, is_dip: bool
    ) -> float:
        latest_order: Union[Order, None] = None
        for order in reversed(self.market):
            if order.instrument == instrument:
                latest_order = order
                break

        if latest_order is None:
            return self.min_price

        if is_peak:
            return round(latest_order.price * (1 + uniform(0, PEAK_OFFSET)), 2)
        elif is_dip:
            return round(latest_order.price * (1 - uniform(0, DIP_OFFSET)), 2)
        else:
            if GENERAL_POINT is None:
                return latest_order.price
            elif latest_order.price < GENERAL_POINT:
                return round(latest_order.price + uniform(0, PEAK_OFFSET), 2)
            elif latest_order.price > GENERAL_POINT:
                return round(latest_order.price - uniform(0, DIP_OFFSET), 2)
            else:
                return latest_order.price

    def generate_order(self, is_peak: bool, is_dip: bool) -> Order:
        instrument = self._determine_order_instrument()
        order_type = self._determine_order_type(instrument)
        return Order(
            offset=self._generate_order_offset(),
            type=order_type,
            instrument=instrument,
            volume=self._determine_order_volume(instrument, order_type),
            price=self._determine_order_price(instrument, is_peak, is_dip),
        )


def generate_orders(entities: List[Entity]) -> Dict[str, Any]:
    """Generate a JSON structure describing the clients and their orders in the system."""
    max_trade_count = max(entity.trade_count for entity in entities)
    for trade_index in range(1, max_trade_count + 1):
        for entity in entities:
            if trade_index <= entity.trade_count:
                is_peak, is_dip = False, False
                if entity.market:
                    if entity.market[-1].is_peak and not entity.market[-1].is_dip:
                        is_peak = random() < NEXT_ORDER_PEAK_PROBABILITY
                    elif entity.market[-1].is_dip and not entity.market[-1].is_peak:
                        is_dip = random() < NEXT_ORDER_DIP_PROBABILITY
                    else:
                        peak_or_dip: List[bool] = [
                            random() < PEAK_PROBABILITY,
                            random() < DIP_PROBABILITY,
                        ]
                        if all(peak_or_dip):
                            if random() > 0.5:
                                is_peak = True
                            else:
                                is_dip = True
                        else:
                            is_peak = peak_or_dip[0]
                            is_dip = peak_or_dip[1]

                order = entity.generate_order(is_peak=is_peak, is_dip=is_dip)
                order.is_peak, order.is_dip = is_peak, is_dip
                entity.add_order(order)

    return entities


def instrument(argument) -> Tuple[str, int]:
    try:
        _instrument, _price = argument.split(",")
        return _instrument, float(_price)
    except Exception:
        raise ArgumentTypeError(
            "Instrument tuples must be provided as 'INSTRUMENT','PRICE'"
        )


if __name__ == "__main__":
    parser = ArgumentParser(
        prog="PET-Exchange Generator",
        description="Generate test data with certain attributes, i.e dips and peaks, general negativity etc",
    )

    # TODO: Make an argument group for limit and another for market orders, excluding the price arguments from market orders.
    parser.add_argument(
        "-g:p",
        "--general-point",
        help=f"General point, rises/decrases price to match the expected value, then let the market be free. Expects a floating point value for origin value, defaults to '{GENERAL_POINT}'",
        type=float,
        default=GENERAL_POINT,
    )
    parser.add_argument(
        "-p:p",
        "--peak-probability",
        help=f"The probability of a trade initiating a peak, a peak will alter the offset from the general point with an multiplier of 5. Expects a floating point value for peak probability, defaults to '{PEAK_PROBABILITY}"
        "If a peak occurs then the probability of the next order being a peak or dip is increased by 30%",
        type=float,
        default=PEAK_PROBABILITY,
    )
    parser.add_argument(
        "-d:p",
        "--dip-probability",
        help=f"The probability of a trade initiating a dip, a dip will alter the offset from the general point with an multiplier of -5. Expects a floating point value for dip probability, defaults to '{DIP_PROBABILITY}'"
        "If a dip occurs then the probability of the next order being a peak or dip is increased by 30%",
        type=float,
        default=DIP_PROBABILITY,
    )
    parser.add_argument(
        "-p:o",
        "--peak-offset",
        help=f"The peak offset is with how much an price can relatively increase at most from the previous order, defaults to '{PEAK_OFFSET}'",
        type=float,
        default=PEAK_OFFSET,
    )
    parser.add_argument(
        "-d:o",
        "--dip-offset",
        help=f"The dip offset is with how much an price can relatively decrease at most from the previous order, defaults to '{DIP_OFFSET}'",
        type=float,
        default=DIP_OFFSET,
    )
    parser.add_argument(
        "-t:r",
        "--type-ratio",
        help=f"The ratio to which how many orders are of type 'BID' and how many of type 'ASK', 0.5 is equal and less than 0.5 is more 'BID' orders and more than 0.5 is more 'ASK' orders, defaults to '{TYPE_RATIO}'",
        type=float,
        default=TYPE_RATIO,
    )
    parser.add_argument(
        "-v:r",
        "--volume-ratio",
        help=f"The ratio to how much of the total volume is controlled by 'BID' orders and how much by 'ASK' orders, 0.5 is equal and less than 0.5 is more 'BID' volume and more than 0.5 is more 'ASK' volume, defaults to '{VOLUME_RATIO}'",
        type=float,
        default=VOLUME_RATIO,
    )
    parser.add_argument(
        "-min:t",
        "--minimum-trades",
        help=f"Minimum amount of trades an entity can make. Expects an integer value, defaults to '{MINIMUM_TRADES}'",
        type=int,
        default=MINIMUM_TRADES,
    )
    parser.add_argument(
        "-max:t",
        "--maximum-trades",
        help=f"Maximum amount of trades an entity can make. Expects an integer value, defaults to '{MAXIMUM_TRADES}'",
        type=int,
        default=MAXIMUM_TRADES,
    )
    parser.add_argument(
        "-min:e",
        "--minimum-entities",
        help=f"Minimum amount of entities participating in the market. Expects an integer value, defaults to '{MINIMUM_ENTITIES}'",
        type=int,
        default=MINIMUM_ENTITIES,
    )
    parser.add_argument(
        "-max:e",
        "--maximum-entities",
        help=f"Maximum amount of entities participating in the market. Expects an integer value, defaults to '{MAXIMUM_ENTITIES}'",
        type=int,
        default=MAXIMUM_ENTITIES,
    )
    parser.add_argument(
        "-min:p",
        "--minimum-price",
        help=f"Minimum price of instruments in the market, any dip will not decline more than this value. Expects a floating point value, defaults to '{MINIMUM_PRICE}'",
        type=float,
        default=MINIMUM_PRICE,
    )
    parser.add_argument(
        "-max:p",
        "--maximum-price",
        help=f"Maximum price of instruments in the market, any peak will not rise higher than this value. Expects a floating point value, defaults to '{MAXIMUM_PRICE}'",
        type=float,
        default=MAXIMUM_PRICE,
    )
    parser.add_argument(
        "-min:v",
        "--minimum-volume",
        help=f"Minimum volume of trades in the market. Expects an integer value, defaults to '{MINIMUM_VOLUME}'",
        type=int,
        default=MINIMUM_VOLUME,
    )
    parser.add_argument(
        "-max:v",
        "--maximum-volume",
        help=f"Maximum volume of trades in the market. Expects an integer value, defaults to '{MAXIMUM_VOLUME}'",
        type=int,
        default=MAXIMUM_VOLUME,
    )
    parser.add_argument(
        "-min:o",
        "--minimum-offset",
        help=f"Minimum time offset value between a previous order and the next order. Expects an integer value, defaults to '{MINIMUM_OFFSET}'",
        type=int,
        default=MINIMUM_OFFSET,
    )
    parser.add_argument(
        "-max:o",
        "--maximum-offset",
        help=f"Maximum time offset value between a previous order and the next order. Expects an integer value, defaults to '{MAXIMUM_OFFSET}'",
        type=int,
        default=MAXIMUM_OFFSET,
    )
    parser.add_argument(
        "-i",
        "--instruments",
        help=f"Instrument pair(s) describing what instrument should be traded and their initial price. Expects an string and floating point value, defaults to '{INSTRUMENT_NAME}' and '{MINIMUM_PRICE}' or minimum price if provided",
        type=instrument,
        nargs="+",
        default=None,
    )
    parser.add_argument(
        "-o:f",
        "--output-file",
        help="Output file to write the orders to, if not provided the orders are output to stdout",
        type=str,
        default=None,
    )
    args = parser.parse_args()

    GENERAL_POINT = args.general_point
    PEAK_PROBABILITY = args.peak_probability
    DIP_PROBABILITY = args.dip_probability
    PEAK_OFFSET = args.peak_offset
    DIP_OFFSET = args.dip_offset
    TYPE_RATIO = args.type_ratio
    VOLUME_RATIO = args.volume_ratio

    entities: List[Entity] = []
    market: List[
        Order
    ] = (
        []
    )  # This is shared between all entities to keep track of latest price and trends

    for arg in vars(args):
        if arg.startswith("minimum") and hasattr(
            args, arg.replace("minimum", "maximum")
        ):
            max_arg = getattr(args, arg.replace("minimum", "maximum"))
            min_arg = getattr(args, arg)

            if max_arg is None:
                continue

            if min_arg > max_arg:
                factor = (
                    min_arg / globals().get(arg.upper())
                    if globals().get(arg.upper()) < min_arg
                    else globals().get(arg.upper()) / min_arg
                )
                if factor != 1.0:
                    print(
                        f"PET-Exchange Generator: '{arg}' was greater than its maximum counterpart, setting '{arg.replace('minimum', 'maximum')}': ('{max_arg}' -> '{max_arg * factor}')"
                    )
                    setattr(args, arg.replace("minimum", "maximum"), max_arg * factor)
                else:
                    factor = (
                        max_arg
                        / globals().get(arg.replace("minimum", "maximum").upper())
                        if globals().get(arg.replace("minimum", "maximum").upper())
                        < max_arg
                        else globals().get(arg.replace("minimum", "maximum").upper())
                        / max_arg
                    )
                    print(
                        f"PET-Exchange Generator: '{arg}' was less than its minimum counterpart, setting '{arg}': ('{min_arg}' -> '{min_arg / factor}')"
                    )
                    setattr(args, arg, min_arg / factor)

    if args.instruments is None:
        args.instruments = [(INSTRUMENT_NAME, args.minimum_price)]

    entity_names: List[str] = []
    entity_count = randint(round(args.minimum_entities), args.maximum_entities)
    for _ in range(entity_count):
        entity: str = "".join([choice(ascii_uppercase) for _ in range(3)])
        while entity in entity_names:
            entity = "".join([choice(ascii_uppercase) for _ in range(3)])

        entities.append(
            Entity(
                entity=entity,
                orders=[],
                instruments=args.instruments,
                trade_count=randint(args.minimum_trades, args.maximum_trades),
                min_price=args.minimum_price,
                max_price=args.maximum_price,
                min_volume=args.minimum_volume,
                max_volume=args.maximum_volume,
                min_offset=args.minimum_offset,
                max_offset=args.maximum_offset,
                market=market,
                peak_probability=args.peak_probability,
                dip_probability=args.dip_probability,
            )
        )

    output: Dict[str, Any] = {"INSTRUMENTS": [], "CLIENTS": {}}
    for entity in generate_orders(entities=entities):
        _entity = vars(entity)
        output["INSTRUMENTS"] = list(
            set(
                output["INSTRUMENTS"]
                + [order.instrument for order in _entity["orders"]]
            )
        )
        output["CLIENTS"][_entity["entity"]] = [
            {
                key: value
                for key, value in vars(order).items()
                if key not in ["is_peak", "is_dip"]
            }
            for order in _entity["orders"]
        ]

    output: str = json.dumps(output, default=vars)

    if args.output_file:
        _path = Path(args.output_file)
        with _path.open(mode="w+") as file_obj:
            file_obj.write(output)
    else:
        print(output)

    print(
        f"BID/ASK Spread: {sum([1 for order in market if order.type == 'BID']) / (sum([1 for order in market if order.type == 'ASK']) + sum([1 for order in market if order.type == 'BID']))}"
    )
    print(
        sum([1 for order in market if order.type == "BID"]),
        sum([1 for order in market if order.type == "ASK"]),
    )

    print(
        f"VOLUME Spread: {sum([order.volume for order in market if order.type == 'BID']) / (sum([order.volume for order in market if order.type == 'ASK']) + sum([order.volume for order in market if order.type == 'BID']))}"
    )

    instrument_spread: Dict[str, int] = {}
    for order in market:
        instrument_spread[order.instrument] = (
            instrument_spread.setdefault(order.instrument, 0) + 1
        )
