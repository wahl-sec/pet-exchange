#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Union

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np


def encrypt_string(value: str, pyfhel: Pyfhel) -> bytes:
    return pyfhel.encryptBatch(np.array(list(map(lambda c: ord(c), value)))).to_bytes()


def encrypt_int(value: int, pyfhel: Pyfhel) -> bytes:
    return pyfhel.encryptInt(value).to_bytes()


def encrypt_frac(value: float, pyfhel: Pyfhel) -> bytes:
    return pyfhel.encryptFrac(value).to_bytes()


def encrypt_add_plain_int(ciphertext: bytes, value: int, pyfhel: Pyfhel) -> bytes:
    return pyfhel.add_plain(
        ctxt=PyCtxt(serialized=ciphertext, encoding="int"),
        ptxt=pyfhel.encodeInt(value),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_add_plain_float(ciphertext: bytes, value: float, pyfhel: Pyfhel) -> bytes:
    return pyfhel.add_plain(
        ctxt=PyCtxt(serialized=ciphertext, encoding="float"),
        ptxt=pyfhel.encodeFrac(value),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_add_ciphertext_int(
    ciphertext: bytes, value: bytes, pyfhel: Pyfhel
) -> bytes:
    return pyfhel.add(
        ctxt=PyCtxt(serialized=ciphertext, encoding="int"),
        ctxt_other=PyCtxt(serialized=value, encoding="int"),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_add_ciphertext_float(
    ciphertext: bytes, value: bytes, pyfhel: Pyfhel
) -> bytes:
    return pyfhel.add(
        ctxt=PyCtxt(serialized=ciphertext, encoding="float"),
        ctxt_other=PyCtxt(serialized=value, encoding="float"),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_sub_plain_int(ciphertext: bytes, value: int, pyfhel: Pyfhel) -> bytes:
    return pyfhel.sub_plain(
        ctxt=PyCtxt(serialized=ciphertext, encoding="int"),
        ptxt=pyfhel.encodeInt(value),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_sub_plain_float(ciphertext: bytes, value: float, pyfhel: Pyfhel) -> bytes:
    return pyfhel.sub_plain(
        ctxt=PyCtxt(serialized=ciphertext, encoding="float"),
        ptxt=pyfhel.encodeFrac(value),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_sub_ciphertext_int(
    ciphertext: bytes, value: bytes, pyfhel: Pyfhel
) -> bytes:
    return pyfhel.sub(
        ctxt=PyCtxt(serialized=ciphertext, encoding="int"),
        ctxt_other=PyCtxt(serialized=value, encoding="int"),
        in_new_ctxt=True,
    ).to_bytes()


def encrypt_sub_ciphertext_float(
    ciphertext: bytes, value: bytes, pyfhel: Pyfhel
) -> bytes:
    return pyfhel.sub(
        ctxt=PyCtxt(serialized=ciphertext, encoding="float"),
        ctxt_other=PyCtxt(serialized=value, encoding="float"),
        in_new_ctxt=True,
    ).to_bytes()
