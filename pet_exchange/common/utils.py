#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid
from random import randint


def generate_identifier() -> str:
    return str(uuid.uuid4())


def generate_random_int() -> int:
    return randint(100, 100000)
