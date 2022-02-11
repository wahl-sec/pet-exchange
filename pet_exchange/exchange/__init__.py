from enum import Enum


class ExchangeOrderType(Enum):
    LIMIT = 0
    MARKET = 1


class OrderType(Enum):
    BID = 0
    ASK = 1
