#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from typing import Union, Dict, List, Optional
from dataclasses import dataclass
import logging
import time

from Pyfhel import Pyfhel, PyCtxt

from pet_exchange.proto.intermediate_pb2 import PlaintextOrder, CiphertextOrder
from pet_exchange.common.crypto import CKKS, CKKS_PARAMETERS

logger = logging.getLogger("__main__")


@dataclass
class KeyPair:
    context: bytes
    public: bytes
    secret: bytes
    relin: bytes


class KeyHandler:
    def __init__(
        self,
        instrument: str,
        compress: Optional[int] = None,
        precision: Optional[int] = None,
    ):
        self.instrument: str = instrument
        self.precision: Optional[int] = precision
        self.timings = {
            "TIME_TO_GENERATE_KEYS": None,
            "TIME_TO_GENERATE_RELIN_KEYS": None,
        }
        # Microsoft has some explanation on the parameters for CKKS
        # https://github.com/microsoft/SEAL/blob/main/native/examples/4_ckks_basics.cpp#L78
        self.pyfhel: Pyfhel = Pyfhel()
        try:
            self.pyfhel.contextGen(scheme="CKKS", **CKKS_PARAMETERS)
        except Exception as e:
            print("Failed to generate context", e)
            raise e from None

        self._key_pair: KeyPair = None
        self._context = self.pyfhel.to_bytes_context()
        self.crypto = CKKS(self.pyfhel, compress=compress)

    @property
    def key_pair(self) -> KeyPair:
        """Returns the key-pair for the object, if it doesn't exist then generate one"""
        if self._key_pair is None:
            return self._generate_key_pair()

        return self._key_pair

    @property
    def context(self):
        """Returns the context to use for the Pyfhel object"""
        return self._context

    def load_key_pair(self) -> Union[KeyPair, None]:
        """Load an existing key-pair from disk based storage if exists"""
        pass

    def _generate_key_pair(self) -> KeyPair:
        """Generate a public/secret/relinearization key for the given instrument"""
        start_time = time.time()
        self.pyfhel.keyGen()
        end_time = time.time()
        self.timings["TIME_TO_GENERATE_KEYS"] = end_time - start_time

        start_time = time.time()
        self.pyfhel.relinKeyGen()
        end_time = time.time()
        self.timings["TIME_TO_GENERATE_RELIN_KEYS"] = end_time - start_time

        self._key_pair = KeyPair(
            context=self.pyfhel.to_bytes_context(),
            public=self.pyfhel.to_bytes_public_key(),
            secret=self.pyfhel.to_bytes_secret_key(),
            relin=self.pyfhel.to_bytes_relin_key(),
        )
        return self._key_pair

    def encrypt(self, plaintext: PlaintextOrder) -> CiphertextOrder:
        """Encrypt a single plaintext order using the initialized key-pair's public key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        return CiphertextOrder(
            type=plaintext.type,
            instrument=plaintext.instrument,
            volume=self.crypto.encrypt_int(plaintext.volume),
            price=self.crypto.encrypt_float(plaintext.price),
        )

    def decrypt(self, ciphertext: CiphertextOrder) -> PlaintextOrder:
        """Decrypt a single ciphertext order using the initialized key-pair's secret key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        _price = self.crypto.decrypt_float(ciphertext.price)
        _volume = self.crypto.decrypt_float(ciphertext.volume)

        return PlaintextOrder(
            type=ciphertext.type,
            instrument=ciphertext.instrument,
            volume=_volume
            if self.precision is None
            else round(_volume, self.precision),
            price=_price if self.precision is None else round(_price, self.precision),
        )


class KeyEngine:
    def __init__(self):
        self._key_storage_cache: Dict[str, KeyHandler] = {}

    def _initialize_key_storage(self):
        """Initialize disk-based storage of key-pairs"""
        pass

    def _lookup_key_storage_disk(self, instrument: str) -> KeyHandler:
        """Look up the disk-based storage of key-pairs for a certain instrument"""
        return None

    def _save_key_handler(
        self,
        handler: KeyHandler,
        overwrite: bool = False,
        _skip_pre_check: bool = False,
    ) -> KeyHandler:
        """Save a key-pair to the local key-storage and to the disk-based storage of key-pairs

        Raises ValueError if the key-handler already exists for a certain instrument
        """
        if not _skip_pre_check and (
            self.key_handler(handler.instrument) and not overwrite
        ):
            raise ValueError(
                f"Key handler for instrument: {handler.instrument} already exists and overwrite was set to: {overwrite}"
            )

        # TODO: Save on disk aswell
        self._key_storage_cache[handler.instrument] = handler
        return handler

    def key_handler(self, instrument: str) -> KeyHandler:
        """Lookup the key-handler assigned to a specific instrument in both the cache and then in the disk-based storage

        Raises `KeyError` if the instrument does not have a key-handler assigned to it
        """
        if instrument in self._key_storage_cache:
            handler = self._key_storage_cache[instrument]
        else:
            handler = self._lookup_key_storage_disk(instrument)

        if handler is None:
            raise KeyError(
                f"Intermediate-Keys: Key handler for instrument: '{instrument}' does not exist"
            )

        return handler

    def generate_key_handler(
        self,
        instrument: str,
        compress: Optional[int] = None,
        precision: Optional[int] = None,
    ) -> KeyHandler:
        try:
            return self.key_handler(instrument=instrument)
        except KeyError:
            logger.info(
                f"Intermediate-Keys: Generating new key-pair for instrument: '{instrument}'"
            )
            handler = KeyHandler(
                instrument=instrument, compress=compress, precision=precision
            )
            self._save_key_handler(handler=handler, _skip_pre_check=True)
            return handler
