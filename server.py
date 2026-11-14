"""tunnel_sticker :: servidor (VPS). Relay TCP a internet libre."""
import argparse, logging, os, socket, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.config import Config
from trcore.core import Relay, Conn, setup_logging, stats_loop
from trcore.crypto import Crypto
from trcore.proto import T_OPEN, T_DATA, T_FIN, pack_fin, unpack_json
from trcore.transport import S3

log = logging.getLogger("tr.server")
_DEFAULT_BLOCKED = ("127.", "10.", "169.254.", "192.168.", "172.16.", "172.17.",
                    "172.18.", "172.19.", "172.20.", "172.21.", "172.22.", "172.23.",
                    "172.24.", "172.25.", "172.26.", "172.27.", "172.28.", "172.29.",
                    "172.30.", "172.31.", "0.0.0.0", "[::1]", "localhost")

def dest_reader(relay, cfg, conn):
    try:
        while True:
            conn.inflight.acquire()
            data = conn.sock.recv(cfg.chunk_kb * 1024)
            if not data: break
            relay.send_conn(conn, T_DATA, data)
    except Exception: pass
    finally:
        relay.send_conn(conn, T_FIN, pack_fin(conn.id))
        relay.teardown(conn.id, "fin remoto")

def dial(relay, cfg, conn_id, host, port):
    blocked = tuple(cfg.blocked_hosts) or _DEFAULT_BLOCKED
    h = (host or "").lower()
    if any(h.startswith(p) for p in blocked):
        log.warning("conn %s: destino bloqueado %s:%d", conn_id, host, port)
        tmp = Conn(conn_id, 1)
        relay.send_conn(tmp, T_FIN, pack_fin(conn_id, "destino bloqueado"))
        return
    conn = Conn(conn_id, cfg.inflight)
    try:
        sock = socket.create_connection((host, port), timeout=cfg.dial_timeout_s)
        sock.settimeout(cfg.idle_timeout_s)
    except Exception as e:
        log.info("conn %s: dial %s:%d fallo: %s", conn_id, host, port, e)
        tmp = Conn(conn_id, 1)
        relay.send_conn(tmp, T_FIN, pack_fin(conn_id, f"dial fallo: {e}"))
        return
    conn.sock = sock
    relay.register_conn(conn)
    log.info("conn %s conectada a %s:%d", conn_id, host, port)
    threading.Thread(target=dest_reader, args=(relay, cfg, conn), daemon=True).start()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    setup_logging(args.verbose)
    cfg = Config.load(args.config)
    crypto = Crypto(cfg.psk, cfg.chan)
    s3 = S3(cfg.s3_base, cfg.bucket, cfg.user_agent, cfg.timeout_s, cfg.retries)
    relay = Relay(cfg, s3, crypto, "s", dispatch=None)

    def dispatch(mtype, body, key):
        conn_id = relay.conn_id_of(key)
        if mtype == T_OPEN:
            d = unpack_json(body)
            dial(relay, cfg, d.get("id") or conn_id, d.get("host",""), int(d.get("port") or 0))
        elif mtype == T_DATA:
            conn = relay.conns.get(conn_id)
            if conn is not None and conn.sock is not None:
                try: conn.sock.sendall(body)
                except Exception:
                    relay.send_conn(conn, T_FIN, pack_fin(conn_id, "envio fallo"))
                    relay.teardown(conn_id, "send a destino fallo")
            else:
                log.debug("datos de conn %s ya cerrada: %d B", conn_id, len(body))
        elif mtype == T_FIN:
            d = unpack_json(body)
            relay.teardown(d.get("id") or conn_id, "fin cliente")
    relay.dispatch = dispatch

    relay.gc_old(cfg.gc_hours)
    stats_loop(relay)
    threading.Thread(target=relay.poll_loop, args=(cfg.poll_ms,), daemon=True).start()
    threading.Thread(target=relay.heartbeat_loop, args=(30,), daemon=True).start()
    log.info("relay activo | canal=%s | bucket=%s/%s | chunk=%dKB poll=%dms workers=%d inflight=%d compress=%s",
             cfg.chan, cfg.s3_base, cfg.bucket, cfg.chunk_kb, cfg.poll_ms,
             cfg.workers, cfg.inflight, cfg.compress)
    try:
        while True: time.sleep(3600)
    except KeyboardInterrupt:
        log.info("saliendo...")
        relay.running = False
    return 0

if __name__ == "__main__":
    sys.exit(main())
