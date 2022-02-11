#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import uuid


def generate_identifier() -> str:
    return str(uuid.uuid4())
