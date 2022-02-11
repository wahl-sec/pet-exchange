from dataclasses import dataclass
from enum import Enum


class OrderType(Enum):
    BID = 0
    ASK = 1


@dataclass
class PlaintextOrder:
    type: OrderType
    instrument: str
    volume: int
    price: float


@dataclass
class CiphertextOrder:
    type: OrderType
    instrument: str
    volume: bytes
    price: bytes
