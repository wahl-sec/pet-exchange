#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from typing import Union, Dict
from dataclasses import dataclass
import logging

from Pyfhel import Pyfhel, PyCtxt

from pet_exchange.proto.intermediate_pb2 import PlaintextOrder, CiphertextOrder

logger = logging.getLogger("__main__")


@dataclass
class KeyPair:
    public: bytes
    secret: bytes


class KeyHandler:
    def __init__(self, instrument: str):
        self.instrument: str = instrument
        self.pyfhel: Pyfhel = Pyfhel()
        self.pyfhel.contextGen(p=655357)

        self._key_pair: KeyPair = None
        self._context = self.pyfhel.to_bytes_context()

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
        """Load an existing key-pair from disk based storage if exists."""
        pass

    def _generate_key_pair(self) -> KeyPair:
        self.pyfhel.keyGen()
        self._key_pair = KeyPair(
            public=self.pyfhel.to_bytes_publicKey(),
            secret=self.pyfhel.to_bytes_secretKey(),
        )
        return self._key_pair

    def encrypt(self, plaintext: PlaintextOrder) -> CiphertextOrder:
        """Encrypt a single plaintext order using the initialized key-pair's public key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        return CiphertextOrder(
            type=plaintext.type,
            instrument=plaintext.instrument,
            volume=self.pyfhel.encryptInt(plaintext.volume).to_bytes(),
            price=self.pyfhel.encryptFrac(plaintext.price).to_bytes(),
        )

    def decrypt(self, ciphertext: CiphertextOrder) -> PlaintextOrder:
        """Decrypt a single ciphertext order using the initialized key-pair's secret key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        _ctx_price: PyCtxt = PyCtxt(
            serialized=ciphertext.price, encoding="float", pyfhel=self.pyfhel
        )
        _ctx_volume: PyCtxt = PyCtxt(
            serialized=ciphertext.volume, encoding="int", pyfhel=self.pyfhel
        )
        return PlaintextOrder(
            type=ciphertext.type,
            instrument=ciphertext.instrument,
            volume=self.pyfhel.decryptInt(_ctx_volume),
            price=self.pyfhel.decryptFrac(_ctx_price),
        )

    def decrypt_int(self, ciphertext: bytes) -> int:
        """Decrypt a single ciphertext integer value using the initialized key-pair's secret key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        _ctx: PyCtxt = PyCtxt(serialized=ciphertext, encoding="int", pyfhel=self.pyfhel)

        return self.pyfhel.decryptInt(_ctx)

    def decrypt_float(self, ciphertext: bytes) -> float:
        """Decrypt a single ciphertext floating point value using the initialized key-pair's secret key

        Raises `ValueError` if the key-pair is not initialized yet
        """
        _ctx: PyCtxt = PyCtxt(
            serialized=ciphertext, encoding="float", pyfhel=self.pyfhel
        )

        return self.pyfhel.decryptFrac(_ctx)


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

    def generate_key_handler(self, instrument: str) -> KeyHandler:
        try:
            return self.key_handler(instrument=instrument)
        except KeyError:
            logger.info(
                f"Intermediate-Keys: Generating new key-pair for instrument: '{instrument}'"
            )
            handler = KeyHandler(instrument=instrument)
            self._save_key_handler(handler=handler, _skip_pre_check=True)
            return handler
