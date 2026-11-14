"""Cifrado AES-256-GCM con clave derivada del PSK via HKDF.
AAD = ruta del objeto. Nonce = 4B aleatorios + 8B seq.
"""
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

MAGIC = b"TR1"
_HDR = len(MAGIC) + 12
_TAG = 16

class CryptoError(Exception):
    pass

class Crypto:
    def __init__(self, psk: str, chan: str):
        salt = b"todus-relay|" + chan.encode("ascii")
        self.key = HKDF(algorithm=hashes.SHA256(), length=32, salt=salt,
                        info=b"todus-relay/v1/aesgcm").derive(psk.encode("utf-8"))
        self._aes = AESGCM(self.key)

    def seal(self, seq: int, path: str, mtype: int, body: bytes) -> bytes:
        nonce = os.urandom(4) + seq.to_bytes(8, "big")
        plaintext = bytes([mtype & 0xFF]) + body
        ct = self._aes.encrypt(nonce, plaintext, path.encode("ascii"))
        return MAGIC + nonce + ct

    def unseal(self, blob: bytes, path: str):
        if not blob.startswith(MAGIC) or len(blob) < _HDR + _TAG + 1:
            raise CryptoError("blob con formato invalido")
        nonce = blob[len(MAGIC):_HDR]
        ct = blob[_HDR:]
        try:
            pt = self._aes.decrypt(nonce, ct, path.encode("ascii"))
        except Exception as e:
            raise CryptoError(f"autenticacion fallida en {path}: {e}") from e
        return pt[0], pt[1:]
