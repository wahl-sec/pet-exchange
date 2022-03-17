#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
from random import randint, uniform

# Remove secret key from message and also look into compression for bytes
MAX_GRPC_MESSAGE_LENGTH = 40000000


def generate_identifier() -> str:
    return str(uuid.uuid4())


def generate_random_int() -> int:
    return randint(100, 200)


def generate_random_float() -> float:
    return round(uniform(100, 200), 2)
