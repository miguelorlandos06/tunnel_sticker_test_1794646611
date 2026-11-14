"""Compresion opcional zstd con fallback a zlib y a sin compresion.
Flag en primer byte: 0x00=sin, 0x01=zlib, 0x02=zstd.
"""
import zlib
try:
    import zstandard as _zstd
    _HAS_ZSTD = True
except ImportError:
    _HAS_ZSTD = False

MIN_SIZE = 256
LEVEL_ZSTD = 3
LEVEL_ZLIB = 6

def compress(body: bytes) -> bytes:
    if len(body) < MIN_SIZE:
        return b"\x00" + body
    if _HAS_ZSTD:
        c = _zstd.ZstdCompressor(level=LEVEL_ZSTD).compress(body)
        if len(c) < len(body):
            return b"\x02" + c
    c = zlib.compress(body, LEVEL_ZLIB)
    if len(c) < len(body):
        return b"\x01" + c
    return b"\x00" + body

def decompress(blob: bytes) -> bytes:
    if not blob:
        return blob
    flag, payload = blob[0], blob[1:]
    if flag == 0x00:
        return payload
    if flag == 0x01:
        return zlib.decompress(payload)
    if flag == 0x02:
        if not _HAS_ZSTD:
            raise ValueError("objeto zstd pero zstandard no instalado")
        return _zstd.ZstdDecompressor().decompress(payload)
    raise ValueError(f"flag de compresion desconocido: {flag}")
