"""Motor del tunel asincrono (aiohttp)."""
import datetime, logging, asyncio, time
from .crypto import CryptoError
from .compress import compress, decompress
from .proto import CTL_CONN, conn_key, T_FIN

log = logging.getLogger("tr.core")

class Conn:
    def __init__(self, conn_id: str, inflight: int):
        self.id, self.sock, self.seq, self.inflight = conn_id, None, 0, asyncio.Semaphore(inflight)
        self.closed = False

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq - 1

class Relay:
    def __init__(self, cfg, s3, crypto, side: str, dispatch):
        self.cfg, self.s3, self.crypto, self.side = cfg, s3, crypto, side
        self.chan_root = cfg.chan
        self.out_side = f"{cfg.chan}/{side}"
        otro_lado = "s" if side == "c" else "c"
        self.in_side = f"{cfg.chan}/{otro_lado}"
        
        self.dispatch = dispatch
        self.conns = {}
        self.q = asyncio.Queue(maxsize=10000)
        self.running = True
        self.seq_ctl = 0
        self.wm = None
        self.stats = {"puts": 0, "gets": 0, "up_bytes": 0, "down_bytes": 0, "start": time.time()}
        self.worker_tasks = []

    async def send_conn(self, conn: Conn, mtype: int, body: bytes):
        if self.cfg.compress: body = compress(body)
        seq = conn.next_seq()
        await self.q.put((conn_key(self.out_side, conn.id, seq), seq, mtype, body, conn))

    async def _upload_worker(self):
        while self.running:
            try: key, seq, mtype, body, conn = await asyncio.wait_for(self.q.get(), timeout=0.5)
            except asyncio.TimeoutError: continue
            try:
                await self.s3.put(key, self.crypto.seal(seq, key, mtype, body))
                self.stats["puts"] += 1
                self.stats["up_bytes"] += len(body)
            except Exception as e:
                log.error("PUT %s fallo: %s", key, e)
                if conn: self.teardown(conn.id, "put-fail")
            finally:
                if conn: conn.inflight.release()

    async def poll(self) -> int:
        keys = await self.s3.list(self.in_side, start_after=self.wm)
        n = 0
        for it in keys:
            k = it["key"]
            blob = await self.s3.get(k)
            self.stats["gets"] += 1
            if blob:
                self.stats["down_bytes"] += max(0, len(blob) - 31)
                try: mtype, body = self.crypto.unseal(blob, k)
                except CryptoError: 
                    await self.s3.delete(k); self.wm = k; continue
                if self.cfg.compress:
                    try: body = decompress(body)
                    except Exception: 
                        await self.s3.delete(k); self.wm = k; continue
                if mtype == T_FIN and b"\"hb\"" in body: 
                    pass
                else:
                    try: await self.dispatch(mtype, body, k)
                    except Exception: pass
            await self.s3.delete(k)
            self.wm = k
            n += 1
        return n

    async def poll_loop(self, poll_ms: int):
        base_wait = max(0.001, poll_ms / 1000.0)
        current_wait = base_wait
        while self.running:
            try:
                if await self.poll() > 0:
                    current_wait = base_wait
                    await asyncio.sleep(0.001)
                else:
                    await asyncio.sleep(current_wait)
                    current_wait = min(current_wait * 1.5, 0.5)
            except Exception:
                await asyncio.sleep(0.1)

    def register_conn(self, conn: Conn):
        self.conns[conn.id] = conn

    def teardown(self, conn_id: str, err: str = ""):
        conn = self.conns.pop(conn_id, None)
        if conn is None: return
        conn.closed = True
        try:
            if conn.sock: conn.sock.close()
        except Exception: pass

    def conn_id_of(self, key: str) -> str:
        parts = key.split("/")
        return parts[-2] if len(parts) >= 2 else ""

    def start_workers(self):
        for _ in range(max(1, self.cfg.workers)):
            self.worker_tasks.append(asyncio.create_task(self._upload_worker()))

async def stats_loop(relay, every_s: int = 30):
    while relay.running:
        await asyncio.sleep(every_s)
        s = relay.stats; up = (time.time() - s["start"]) or 1
        log.info("stats: up=%d obj down=%d obj puts/s=%.1f gets/s=%.1f conns=%d", s["puts"], s["gets"], s["puts"]/up, s["gets"]/up, len(relay.conns))
