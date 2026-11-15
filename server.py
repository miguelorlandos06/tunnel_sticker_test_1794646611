"""tunnel_sticker :: servidor (VPS, síncrono)."""
import argparse, logging, os, socket, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.config import Config
from trcore.core import Relay, Conn, setup_logging, stats_loop
from trcore.crypto import Crypto
from trcore.proto import T_OPEN, T_DATA, T_FIN, pack_fin, unpack_json
from trcore.transport import S3

log = logging.getLogger("tr.server")
_BLOCKED = ("127.", "10.", "169.254.", "192.168.", "172.16.", "localhost")

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
    if any(host.lower().startswith(p) for p in _BLOCKED):
        tmp = Conn(conn_id, 1)
        relay.send_conn(tmp, T_FIN, pack_fin(conn_id, "bloqueado"))
        return
    try:
        sock = socket.create_connection((host, port), timeout=cfg.dial_timeout_s)
        sock.settimeout(cfg.idle_timeout_s)
    except Exception as e:
        tmp = Conn(conn_id, 1)
        relay.send_conn(tmp, T_FIN, pack_fin(conn_id, f"fallo: {e}"))
        return
    conn = Conn(conn_id, cfg.inflight)
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
            if conn and conn.sock:
                try: conn.sock.sendall(body)
                except Exception: relay.teardown(conn_id, "send fallo")
        elif mtype == T_FIN:
            d = unpack_json(body)
            relay.teardown(d.get("id") or conn_id, "fin cliente")
    relay.dispatch = dispatch

    stats_loop(relay)
    threading.Thread(target=relay.poll_loop, args=(cfg.poll_ms,), daemon=True).start()
    log.info("Servidor Sync activo | canal=%s | workers=%d", cfg.chan, cfg.workers)
    
    try:
        while True: time.sleep(3600)
    except KeyboardInterrupt: pass

if __name__ == "__main__":
    sys.exit(main())
