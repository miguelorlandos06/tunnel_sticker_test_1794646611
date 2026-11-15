"""tunnel_sticker :: cliente (Termux, síncrono)."""
import argparse, logging, os, sys, threading, itertools
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.config import Config
from trcore.core import Relay, Conn, setup_logging, stats_loop
from trcore.crypto import Crypto
from trcore.proto import T_OPEN, T_DATA, T_FIN, pack_open, pack_fin, unpack_json
from trcore.transport import S3

log = logging.getLogger("tr.client")
_conn_counter = itertools.count(1)
def new_conn_id() -> str: return f"{next(_conn_counter) & 0xffff:04x}{os.urandom(2).hex()}"

def _recvn(sock, n):
    buf = b""
    while len(buf) < n:
        d = sock.recv(n - len(buf))
        if not d: raise ConnectionError("socket cerrado")
        buf += d
    return buf

def socks5_handshake(cs):
    ver, nmethods = _recvn(cs, 2)
    if ver != 5: raise ConnectionError(f"no es SOCKS5 (ver={ver})")
    _recvn(cs, nmethods)
    cs.sendall(b"\x05\x00")
    ver, cmd, _rsv, atyp = _recvn(cs, 4)
    if cmd != 1: 
        cs.sendall(b"\x05\x07\x00\x01" + b"\x00" * 6)
        raise ConnectionError(f"comando no soportado: {cmd}")
    if atyp == 1: host = ".".join(map(str, _recvn(cs, 4)))
    elif atyp == 3: host = _recvn(cs, _recvn(cs, 1)[0]).decode("idna")
    elif atyp == 4: 
        import socket
        host = socket.inet_ntop(socket.AF_INET6, _recvn(cs, 16))
    else: raise ConnectionError(f"ATYP desconocido: {atyp}")
    port = int.from_bytes(_recvn(cs, 2), "big")
    return host, port

def local_reader(relay, cfg, conn):
    try:
        while True:
            conn.inflight.acquire()
            data = conn.sock.recv(cfg.chunk_kb * 1024)
            if not data: break
            relay.send_conn(conn, T_DATA, data)
    except Exception: pass
    finally:
        relay.send_conn(conn, T_FIN, pack_fin(conn.id))
        relay.teardown(conn.id, "fin local")

def session(relay, cfg, cs):
    try: host, port = socks5_handshake(cs)
    except Exception as e:
        log.debug("handshake invalido: %s", e)
        try: cs.close()
        except Exception: pass
        return
    conn = Conn(new_conn_id(), cfg.inflight)
    conn.sock = cs
    relay.register_conn(conn)
    relay.send_conn(conn, T_OPEN, pack_open(conn.id, host, port))
    cs.sendall(b"\x05\x00\x00\x01" + b"\x00" * 6)
    log.info("conn %s -> %s:%d abierta", conn.id, host, port)
    local_reader(relay, cfg, conn)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose)
    cfg = Config.load(args.config)
    crypto = Crypto(cfg.psk, cfg.chan)
    s3 = S3(cfg.s3_base, cfg.bucket, cfg.user_agent, cfg.timeout_s, cfg.retries)
    relay = Relay(cfg, s3, crypto, "c", dispatch=None)

    def dispatch(mtype, body, key):
        conn_id = relay.conn_id_of(key)
        if mtype == T_DATA:
            conn = relay.conns.get(conn_id)
            if conn and conn.sock:
                try: conn.sock.sendall(body)
                except Exception: relay.teardown(conn_id, "send fallo")
        elif mtype == T_FIN:
            d = unpack_json(body)
            relay.teardown(conn_id, d.get("err", "fin remoto"))
    relay.dispatch = dispatch

    stats_loop(relay)
    threading.Thread(target=relay.poll_loop, args=(cfg.poll_ms,), daemon=True).start()

    lh, lp = cfg.listen_addr()
    import socket
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((lh, lp))
    lsock.listen(64)
    log.info("SOCKS5 Sync en %s:%d | canal=%s | workers=%d", lh, lp, cfg.chan, cfg.workers)
    
    try:
        while True:
            cs, addr = lsock.accept()
            threading.Thread(target=session, args=(relay, cfg, cs), daemon=True).start()
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    sys.exit(main())
