#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Union, List, Optional
import zlib

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np


# n - polynomial modulus degree - size of ciphertext elements, number of coefficients in plaintext vector, bigger is better (security), bigger is worse (performance)
# qs - coefficient modulus sizes - used by SEAL to generate a list of primes of those binary sizes, size of ciphertext elements, length of list indicates the multiplicative depth (level) of the scheme, bigger is worse (security)
# scale - scaling factor - defines encoding precision for the binary representation of coefficients, bigger is better (precision), bigger is worse (performance?)`
CKKS_PARAMETERS = {
    "n": 2**14,
    # "qs": [52] + [45] * 0,  # CKKS-11, CKKS-12 (2**11 (128), 2**12 (256))
    "qs": [52] + [45] * 6,  # CKKS-14 (2**14 (128))
    # "qs": [45] + [32] * 6,  # CKKS-14 (2**14 (256))
    # "qs": [24] + [23] * 18,
    "scale": 2**45,
}

# Max qs for 2 ** 14 is 438
# n = 2**11 (128-bit)
# n = 2**14 (128-bit if 6 level)
# n = 2**12 (256 if 1 level)


class CKKS:
    def __init__(self, pyfhel: Pyfhel, compress: Optional[int] = None) -> "CKKS":
        self._pyfhel = pyfhel
        self._depth_map = {}
        self._compress = compress

    def to_bytes(self, ctxt: PyCtxt) -> bytes:
        return (
            zlib.compress(
                ctxt.to_bytes() if isinstance(ctxt, PyCtxt) else ctxt,
                level=self._compress,
            )
            if self._compress is not None
            else ctxt.to_bytes()
        )

    def from_bytes(self, ctxt: bytes) -> PyCtxt:
        if self._compress is not None:
            ctxt = zlib.decompress(ctxt)

        return PyCtxt(serialized=ctxt, pyfhel=self._pyfhel)

    def mod_switch_until(
        self, first: Union[PyPtxt, PyCtxt], second: Union[PyPtxt, PyCtxt]
    ) -> None:
        _first = self._depth_map[hash(first)]
        _second = self._depth_map[hash(second)]
        if _first < _second:
            for _ in range(_first, _second):
                self._pyfhel.mod_switch_to_next(first)

            self._depth_map[hash(first)] += _second - _first
        else:
            for _ in range(_second, _first):
                self._pyfhel.mod_switch_to_next(second)

            self._depth_map[hash(second)] += _first - _second

    def encode_float(self, values: List[float]) -> bytes:
        _ptxt = self._pyfhel.encodeFrac(np.array(values))
        self._depth_map[hash(_ptxt)] = 1
        return _ptxt

    def encrypt_string(self, value: str, to_bytes: bool = True) -> bytes:
        _ctxt = self._pyfhel.encrypt(
            self._pyfhel.encodeFrac(np.array(list(map(lambda c: float(ord(c)), value))))
        )
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_int(self, value: int, to_bytes: bool = True) -> bytes:
        return self.encrypt_float(float(value), to_bytes=to_bytes)

    def encrypt_float(
        self,
        value: float,
        scale: float = 0.0,
        scale_bits: int = 0,
        to_bytes: bool = True,
    ) -> bytes:
        _ctxt = self._pyfhel.encryptFrac(
            np.array([value]),
            scale=scale,
            scale_bits=scale_bits,
        )

        self._depth_map[hash(_ctxt)] = 1
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_add_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        return self.encrypt_add_plain_float(ciphertext=ciphertext, value=float(value))

    def encrypt_add_plain_float(
        self,
        ciphertext: bytes,
        value: float,
        to_bytes: bool = True,
        new_ctxt: bool = True,
        scale: float = 0.0,
        scale_bits: int = 0,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        ptxt = self.encode_float(values=[value]) if isinstance(value, float) else value

        self.mod_switch_until(first=ctxt, second=ptxt)
        _ctxt_r = self._pyfhel.add_plain(
            ctxt=ctxt,
            ptxt=ptxt,
            in_new_ctxt=new_ctxt,
        )
        _ctxt = _ctxt_r if new_ctxt else ciphertext

        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)]
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_add_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        raise NotImplementedError

    def encrypt_add_ciphertext_float(
        self,
        ciphertext: bytes,
        value: bytes,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        if isinstance(value, bytes):
            ctxt_other = PyCtxt(serialized=value, pyfhel=self._pyfhel)
            if hash(ctxt_other) not in self._depth_map:
                self._depth_map[hash(ctxt_other)] = self._depth_map[hash(value)]
        else:
            ctxt_other = value

        self.mod_switch_until(first=ctxt, second=ctxt_other)
        _ctxt_r = self._pyfhel.add(
            ctxt=ctxt,
            ctxt_other=ctxt_other,
            in_new_ctxt=new_ctxt,
        )
        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)]
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_sub_plain_int(
        self,
        ciphertext: bytes,
        value: int,
        to_bytes: bool = True,
    ) -> bytes:
        return self.encrypt_sub_plain_float(
            ciphertext=ciphertext,
            value=float(value),
            to_bytes=to_bytes,
            compress=compress,
        )

    def encrypt_sub_plain_float(
        self,
        ciphertext: bytes,
        value: float,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        ptxt = self.encode_float(values=[value]) if isinstance(value, float) else value

        self.mod_switch_until(first=ctxt, second=ptxt)
        _ctxt_r = self._pyfhel.sub_plain(
            ctxt=ctxt,
            ptxt=ptxt,
            in_new_ctxt=new_ctxt,
        )
        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)]
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_sub_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        raise NotImplementedError

    def encrypt_sub_ciphertext_float(
        self,
        ciphertext: bytes,
        value: bytes,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        if isinstance(value, bytes):
            ctxt_other = PyCtxt(serialized=value, pyfhel=self._pyfhel)
            if hash(ctxt_other) not in self._depth_map:
                self._depth_map[hash(ctxt_other)] = self._depth_map[hash(value)]
        else:
            ctxt_other = value

        self.mod_switch_until(first=ctxt, second=ctxt_other)
        _ctxt_r = self._pyfhel.sub(
            ctxt=ctxt,
            ctxt_other=ctxt_other,
            in_new_ctxt=new_ctxt,
        )
        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)]
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_mult_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        raise self.encrypt_mult_plain_float(ciphertext=ciphertext, value=float(value))

    def encrypt_mult_plain_float(
        self,
        ciphertext: bytes,
        value: float,
        to_bytes: bool = True,
        new_ctxt: bool = True,
        scale: float = 0.0,
        scale_bits: int = 0,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        ptxt = self.encode_float(values=[value]) if isinstance(value, float) else value

        self.mod_switch_until(first=ctxt, second=ptxt)
        _ctxt_r = self._pyfhel.multiply_plain(
            ctxt=ctxt,
            ptxt=ptxt,
            in_new_ctxt=new_ctxt,
        )

        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._pyfhel.rescale_to_next(_ctxt)
        _ctxt.round_scale()

        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)] + 1
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_mult_ciphertext_int(self, ciphertext: bytes, value: int) -> bytes:
        return NotImplementedError

    def encrypt_mult_ciphertext_float(
        self,
        ciphertext: bytes,
        value: bytes,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        if isinstance(value, bytes):
            ctxt_other = PyCtxt(serialized=value, pyfhel=self._pyfhel)
            if hash(ctxt_other) not in self._depth_map:
                self._depth_map[hash(ctxt_other)] = self._depth_map[hash(value)]
        else:
            ctxt_other = value

        self.mod_switch_until(first=ctxt, second=ctxt_other)
        _ctxt_r = self._pyfhel.multiply(
            ctxt=ctxt,
            ctxt_other=ctxt_other,
            in_new_ctxt=new_ctxt,
        )

        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._pyfhel.rescale_to_next(_ctxt)
        _ctxt.round_scale()

        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)] + 1
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_square(
        self,
        ciphertext: bytes,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        if isinstance(ciphertext, bytes):
            ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(ctxt) not in self._depth_map:
                self._depth_map[hash(ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            ctxt = ciphertext

        _ctxt_r = self._pyfhel.square(
            ctxt=ctxt,
            in_new_ctxt=new_ctxt,
        )

        _ctxt = _ctxt_r if new_ctxt else ciphertext
        self._pyfhel.rescale_to_next(_ctxt)
        _ctxt.round_scale()

        self._depth_map[hash(_ctxt)] = self._depth_map[hash(ctxt)] + 1
        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def encrypt_pow_plain_int(
        self,
        ciphertext: bytes,
        value: int,
        to_bytes: bool = True,
        new_ctxt: bool = True,
    ) -> bytes:
        # Pyfhel does not natively support power of operations on CKKS
        # This operation is probably very costly in terms of noise and performance
        if isinstance(ciphertext, bytes):
            _ctxt = PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
            if hash(_ctxt) not in self._depth_map:
                self._depth_map[hash(_ctxt)] = self._depth_map[hash(ciphertext)]
        else:
            _ctxt = ciphertext

        for _ in range(value):
            self._pyfhel.multiply(
                ctxt=_ctxt,
                ctxt_other=_ctxt,
                in_new_ctxt=False,
            )
            self._pyfhel.relinearize(_ctxt)
            self._pyfhel.rescale_to_next(_ctxt)
            _ctxt.round_scale()
            self._depth_map[hash(_ctxt)] += 1

        return self.to_bytes(ctxt=_ctxt) if to_bytes else _ctxt

    def decrypt_string(self, ciphertext: bytes) -> str:
        return "".join(
            [
                chr(round(value))
                for value in self._pyfhel.decryptFrac(self.from_bytes(ctxt=ciphertext))
                if round(value) > 0
            ]
        )

    def decrypt_int(self, ciphertext: bytes) -> int:
        raise NotImplementedError

    def decrypt_float(self, ciphertext: bytes) -> float:
        return max(
            self._pyfhel.decryptFrac(
                PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
                if isinstance(ciphertext, bytes)
                else ciphertext
            )
        )
