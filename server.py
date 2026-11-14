"""tunnel_sticker :: servidor (VPS, async)."""
import argparse, logging, os, sys, asyncio, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.config import Config
from trcore.core import Relay, Conn, stats_loop
from trcore.crypto import Crypto
from trcore.proto import T_OPEN, T_DATA, T_FIN, pack_fin, unpack_json
from trcore.transport import S3

log = logging.getLogger("tr.server")
_BLOCKED = ("127.", "10.", "169.254.", "192.168.", "172.16.", "localhost")

async def dest_reader(conn, relay, cfg):
    try:
        while True:
            await conn.inflight.acquire()
            data = await conn.sock.read(cfg.chunk_kb * 1024)
            if not data: break
            await relay.send_conn(conn, T_DATA, data)
    except Exception: pass
    finally:
        await relay.send_conn(conn, T_FIN, pack_fin(conn.id))
        relay.teardown(conn.id, "fin remoto")

async def dial(relay, cfg, conn_id, host, port):
    if any(host.lower().startswith(p) for p in _BLOCKED):
        tmp = Conn(conn_id, 1)
        await relay.send_conn(tmp, T_FIN, pack_fin(conn_id, "bloqueado"))
        return
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=cfg.dial_timeout_s)
    except Exception as e:
        tmp = Conn(conn_id, 1)
        await relay.send_conn(tmp, T_FIN, pack_fin(conn_id, f"fallo: {e}"))
        return
    conn = Conn(conn_id, cfg.inflight)
    conn.sock = writer
    relay.register_conn(conn)
    log.info("conn %s conectada a %s:%d", conn_id, host, port)
    asyncio.create_task(dest_reader(conn, relay, cfg))

async def main_async(args):
    cfg = Config.load(args.config)
    crypto = Crypto(cfg.psk, cfg.chan)
    s3 = S3(cfg.s3_base, cfg.bucket, cfg.user_agent, cfg.timeout_s, cfg.retries)
    relay = Relay(cfg, s3, crypto, "s", dispatch=None)
    
    async def dispatch(mtype, body, key):
        conn_id = relay.conn_id_of(key)
        if mtype == T_OPEN:
            d = unpack_json(body)
            await dial(relay, cfg, d.get("id") or conn_id, d.get("host",""), int(d.get("port") or 0))
        elif mtype == T_DATA:
            conn = relay.conns.get(conn_id)
            if conn and conn.sock:
                try: conn.sock.write(body); await conn.sock.drain()
                except Exception: relay.teardown(conn_id, "send fallo")
        elif mtype == T_FIN:
            d = unpack_json(body)
            relay.teardown(d.get("id") or conn_id, "fin cliente")
            
    relay.dispatch = dispatch
    relay.start_workers()
    asyncio.create_task(relay.poll_loop(cfg.poll_ms))
    asyncio.create_task(stats_loop(relay))
    
    log.info("Servidor Async activo | canal=%s | workers=%d", cfg.chan, cfg.workers)
    while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")
    try: asyncio.run(main_async(args))
    except KeyboardInterrupt: pass
