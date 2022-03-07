#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Union, List

from Pyfhel import Pyfhel, PyCtxt, PyPtxt
import numpy as np


# TODO: Look up parameters to understand what they mean
BFV_PARAMETERS = {"p": 65537, "n": 2048, "sec": 128}

CKKS_PARAMETERS = {
    "n": 8192,
    "qs": [60, 40, 40, 60],
    "scale": 2 ** 40,
    "sec": 128,
}


class BFV:
    def __init__(self, pyfhel: Pyfhel) -> "BFV":
        self._pyfhel = pyfhel

    def _float_to_array(self, value: float) -> List[int]:
        # Convert a float value to array of integers, so 24.25 => [24, 25]
        # If the floating point contains a zero as a leading value like 22.025
        # then the array becomes 22.02 => [22, 0, 25]
        integer, decimal = str(value).split(".")
        if decimal.startswith("0"):
            value = [int(integer), 0, int(decimal)]
        else:
            # The -1 is meant as an marker to allow us to treat the starts with 0-case and the not starts with 0-case in the same logic.
            value = [int(integer), -1, int(decimal)]

        return value

    def _array_to_float(self, value: List[int]) -> float:
        # Convert an integer array representation of a float to an actual float, [24, 25] => 24.25
        # In order to handle arrays with leading 0's of the decimal part we use the first item
        # as the integer part and the joined rest as the decimal part, [22, 0, 25] => 22.025
        if value[1] < 0 or value[1] > 0:
            return float(f"{value[0]}.{value[2]}")
        else:
            return float(f"{value[0]}.0{value[2]}")

        return float(
            f"{value[0]}.{''.join([str(val) for val in value[1:3] if val >= 0])}"
        )

    def encrypt_string(self, value: str) -> bytes:
        return self._pyfhel.encrypt(
            self._pyfhel.encodeInt(np.array(list(map(lambda c: ord(c), value))))
        ).to_bytes()

    def encrypt_int(self, value: int) -> bytes:
        return self._pyfhel.encrypt(
            self._pyfhel.encodeInt(np.array([value]))
        ).to_bytes()

    def encrypt_float(self, value: float) -> bytes:
        return self._pyfhel.encrypt(
            self._pyfhel.encodeInt(np.array(self._float_to_array(value)))
        ).to_bytes()

    def encrypt_add_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        return self._pyfhel.add_plain(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ptxt=self._pyfhel.encodeInt(np.array([value])),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_add_plain_float(self, ciphertext: bytes, value: float) -> bytes:
        return self.encrypt_add_ciphertext_float(
            ciphertext=ciphertext, value=self.encrypt_float(value)
        )

    def encrypt_add_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.add(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_add_ciphertext_float(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.add(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_sub_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        return self._pyfhel.sub_plain(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ptxt=self._pyfhel.encodeInt(np.array([value])),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_sub_plain_float(self, ciphertext: bytes, value: float) -> bytes:
        return self.encrypt_sub_ciphertext_float(
            ciphertext=ciphertext, value=self.encrypt_float(value)
        )

    def encrypt_sub_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.sub(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_sub_ciphertext_float(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.sub(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def decrypt_string(self, ciphertext: bytes) -> str:
        return "".join(
            [
                chr(value)
                for value in self._pyfhel.decodeInt(
                    self._pyfhel.decrypt(
                        PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel, scheme="bfv")
                    )
                )
                if value > 0
            ]
        )

    def decrypt_int(self, ciphertext: bytes) -> int:
        return max(
            self.decodeInt(
                self._pyfhel.decrypt(PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel))
            )
        )

    def decrypt_float(self, ciphertext: bytes) -> float:
        return self._array_to_float(
            list(
                self._pyfhel.decodeInt(
                    self._pyfhel.decrypt(
                        PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel)
                    )
                )
            )
        )


class CKKS:
    def __init__(self, pyfhel: Pyfhel) -> "CKKS":
        self._pyfhel = pyfhel

    def encrypt_string(self, value: str) -> bytes:
        return self._pyfhel.encryptFrac(
            np.array(list(map(lambda c: float(ord(c)), value)))
        ).to_bytes()

    def encrypt_int(self, value: int) -> bytes:
        return self._encrypt_float(float(value))

    def encrypt_float(self, value: float) -> bytes:
        return self._pyfhel.encryptFrac(np.array([value])).to_bytes()

    def encrypt_add_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        return self.encrypt_add_plain_float(ciphertext=ciphertext, value=float(value))

    def encrypt_add_plain_float(self, ciphertext: bytes, value: float) -> bytes:
        return self._pyfhel.add_plain(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ptxt=self._pyfhel.encodeFrac(np.array([value])),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_add_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        raise NotImplemented

    def encrypt_add_ciphertext_float(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.add(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_sub_plain_int(self, ciphertext: bytes, value: int) -> bytes:
        return self.encrypt_sub_plain_float(ciphertext=ciphertext, value=float(value))

    def encrypt_sub_plain_float(self, ciphertext: bytes, value: float) -> bytes:
        return self._pyfhel.sub_plain(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ptxt=self._pyfhel.encodeFrac(np.array([value])),
            in_new_ctxt=True,
        ).to_bytes()

    def encrypt_sub_ciphertext_int(self, ciphertext: bytes, value: bytes) -> bytes:
        raise NotImplemented

    def encrypt_sub_ciphertext_float(self, ciphertext: bytes, value: bytes) -> bytes:
        return self._pyfhel.sub(
            ctxt=PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel),
            ctxt_other=PyCtxt(serialized=value, pyfhel=self._pyfhel),
            in_new_ctxt=True,
        ).to_bytes()

    def decrypt_string(self, ciphertext: bytes) -> str:
        return "".join(
            [
                chr(round(value))
                for value in self._pyfhel.decryptFrac(
                    PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel, scheme="ckks")
                )
                if round(value) > 0
            ]
        )

    def decrypt_int(self, ciphertext: bytes) -> int:
        raise NotImplemented

    def decrypt_float(self, ciphertext: bytes) -> float:
        return max(
            self._pyfhel.decryptFrac(PyCtxt(serialized=ciphertext, pyfhel=self._pyfhel))
        )
