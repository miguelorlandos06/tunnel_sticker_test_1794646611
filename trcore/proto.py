"""Protocolo de mensajes sobre las claves S3.
c/<conn>/<seq>  cliente->servidor: seq 0=OPEN, luego DATA y FIN
s/<conn>/<seq>  servidor->cliente: DATA y FIN
conn=8 hex != 00000000. seq=12 decimales con ceros.
"""
import json

T_OPEN = 1
T_DATA = 2
T_FIN = 3
CTL_CONN = "00000000"

def conn_key(side: str, conn: str, seq: int) -> str:
    return f"{side}/{conn}/{seq:012d}"

def pack_open(conn: str, host: str, port: int) -> bytes:
    return json.dumps({"id": conn, "host": host, "port": int(port)},
                      separators=(",", ":")).encode()

def pack_fin(conn: str, err: str = "") -> bytes:
    return json.dumps({"id": conn, "err": err}, separators=(",", ":")).encode()

def unpack_json(body: bytes) -> dict:
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return {}
