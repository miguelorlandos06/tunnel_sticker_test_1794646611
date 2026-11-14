"""tunnel_sticker :: cliente (Termux, async)."""
import argparse, logging, os, sys, asyncio, itertools
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.config import Config
from trcore.core import Relay, Conn, stats_loop
from trcore.crypto import Crypto
from trcore.proto import T_OPEN, T_DATA, T_FIN, pack_open, pack_fin, unpack_json
from trcore.transport import S3

log = logging.getLogger("tr.client")
_counter = itertools.count(1)

def new_conn_id(): return f"{next(_counter) & 0xffff:04x}{os.urandom(2).hex()}"

async def handle_client(reader, writer, relay, cfg):
    try:
        ver, nmethods = await reader.readexactly(2)
        if ver != 5: raise ConnectionError("no SOCKS5")
        await reader.readexactly(nmethods)
        writer.write(b"\x05\x00"); await writer.drain()
        
        ver, cmd, _rsv, atyp = await reader.readexactly(4)
        if cmd != 1: 
            writer.write(b"\x05\x07\x00\x01" + b"\x00" * 6); await writer.drain()
            raise ConnectionError("comando no soportado")
            
        if atyp == 1: host = ".".join(map(str, await reader.readexactly(4)))
        elif atyp == 3: host = (await reader.readexactly((await reader.readexactly(1))[0])).decode("idna")
        else: raise ConnectionError("ATYP no soportado")
        
        port = int.from_bytes(await reader.readexactly(2), "big")
        conn = Conn(new_conn_id(), cfg.inflight)
        relay.register_conn(conn)
        
        await relay.send_conn(conn, T_OPEN, pack_open(conn.id, host, port))
        writer.write(b"\x05\x00\x00\x01" + b"\x00" * 6); await writer.drain()
        log.info("conn %s -> %s:%d abierta", conn.id, host, port)
        
        while True:
            await conn.inflight.acquire()
            data = await reader.read(cfg.chunk_kb * 1024)
            if not data: break
            await relay.send_conn(conn, T_DATA, data)
    except Exception as e:
        log.debug("Conexion cerrada: %s", e)
    finally:
        if "conn" in locals():
            await relay.send_conn(conn, T_FIN, pack_fin(conn.id))
            relay.teardown(conn.id, "fin local")
        writer.close()

async def main_async(args):
    cfg = Config.load(args.config)
    crypto = Crypto(cfg.psk, cfg.chan)
    s3 = S3(cfg.s3_base, cfg.bucket, cfg.user_agent, cfg.timeout_s, cfg.retries)
    relay = Relay(cfg, s3, crypto, "c", dispatch=None)
    
    async def dispatch(mtype, body, key):
        conn_id = relay.conn_id_of(key)
        if mtype == T_DATA:
            conn = relay.conns.get(conn_id)
            if conn and conn.sock:
                try: conn.sock.write(body); await conn.sock.drain()
                except Exception: relay.teardown(conn_id, "send fallo")
        elif mtype == T_FIN:
            d = unpack_json(body)
            relay.teardown(conn_id, d.get("err", "fin remoto"))
            
    relay.dispatch = dispatch
    relay.start_workers()
    asyncio.create_task(relay.poll_loop(cfg.poll_ms))
    asyncio.create_task(stats_loop(relay))
    
    lh, lp = cfg.listen_addr()
    server = await asyncio.start_server(lambda r, w: handle_client(r, w, relay, cfg), lh, lp)
    log.info("SOCKS5 Async en %s:%d | workers=%d", lh, lp, cfg.workers)
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")
    try: asyncio.run(main_async(args))
    except KeyboardInterrupt: pass
