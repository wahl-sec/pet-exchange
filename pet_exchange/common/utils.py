#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Union, List, Dict, Tuple

import uuid
from random import randint, uniform

from pet_exchange.common.crypto import CKKS, BFV
from pet_exchange.proto.intermediate_pb2 import Challenge

# Remove secret key from message and also look into compression for bytes
MAX_GRPC_MESSAGE_LENGTH = 50000000


def generate_identifier() -> str:
    return str(uuid.uuid4())


def generate_random_int() -> int:
    return randint(100, 200)


def generate_random_float() -> float:
    return round(uniform(100, 200), 2)

def generate_challenges(engine: Union[CKKS, BFV], n: int) -> Tuple[List[int], List[Challenge]]:
    """Generate a list of challenges used to ensure the third party is correct when determining results."""
    _expected: List[int] = []
    _challenges: List[Challenge] = []
    for _ in range(n):
        a = uniform(0, 1)
        b = uniform(0, 1)
        _challenges.append(
            Challenge(first=engine.encrypt_float(a), second=engine.encrypt_float(b))
        )
        _expected.append(-1 if a < b else 1)

    return _expected, _challenges

