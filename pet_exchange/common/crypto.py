#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from Pyfhel import Pyfhel


def encrypt_int(value: int, pyfhel: Pyfhel):
    return pyfhel.encryptInt(value).to_bytes()


def encrypt_frac(value: float, pyfhel: Pyfhel):
    return pyfhel.encryptFrac(value).to_bytes()
