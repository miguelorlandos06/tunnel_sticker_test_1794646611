"""Motor del tunel (Modo Kamikaze)."""
import datetime, logging, queue, threading, time
from .crypto import CryptoError
from .compress import compress, decompress
from .proto import CTL_CONN, conn_key, T_FIN

log = logging.getLogger("tr.core")

class Conn:
    def __init__(self, conn_id: str, inflight: int):
        self.id, self.sock, self.seq, self.lock, self.inflight, self.closed = conn_id, None, 0, threading.Lock(), threading.Semaphore(inflight), False
    def next_seq(self) -> int:
        with self.lock: 
            s = self.seq
            self.seq += 1
            return s

class Relay:
    def __init__(self, cfg, s3, crypto, side: str, dispatch):
        self.cfg, self.s3, self.crypto, self.side = cfg, s3, crypto, side
        self.chan_root = cfg.chan
        self.out_side = f"{cfg.chan}/{side}"
        otro_lado = "s" if side == "c" else "c"
        self.in_side = f"{cfg.chan}/{otro_lado}"
        
        self.dispatch, self.conns, self.conns_lock = dispatch, {}, threading.Lock()
        self.q = queue.Queue(maxsize=2000) # Cola masiva para 32 workers
        self.running, self.seq_ctl, self.wm = True, 0, None
        self.stats = {"puts": 0, "gets": 0, "up_bytes": 0, "down_bytes": 0, "start": time.time()}
        for _ in range(max(1, cfg.workers)): threading.Thread(target=self._upload_worker, daemon=True).start()

    def send_ctl(self, mtype: int, body: bytes) -> None:
        self.seq_ctl += 1
        self.q.put((conn_key(self.out_side, CTL_CONN, self.seq_ctl), self.seq_ctl, mtype, body, None))

    def send_conn(self, conn: Conn, mtype: int, body: bytes) -> None:
        if self.cfg.compress: 
            body = compress(body)
        seq = conn.next_seq()
        self.q.put((conn_key(self.out_side, conn.id, seq), seq, mtype, body, conn))

    def _upload_worker(self) -> None:
        while self.running:
            try: key, seq, mtype, body, conn = self.q.get(timeout=0.5)
            except queue.Empty: continue
            try:
                self.s3.put(key, self.crypto.seal(seq, key, mtype, body))
                self.stats["puts"] += 1
                self.stats["up_bytes"] += len(body)
            except Exception as e:
                log.error("PUT %s fallo: %s", key, e)
                if conn: self.teardown(conn.id, "put-fail")
            finally:
                if conn: conn.inflight.release()

    def poll(self) -> int:
        keys = self.s3.list(self.in_side, start_after=self.wm)
        n = 0
        for it in keys:
            k = it["key"]
            blob = self.s3.get(k)
            self.stats["gets"] += 1
            if blob is None: 
                self.wm = k
                continue
            self.stats["down_bytes"] += max(0, len(blob) - 31)
            try: mtype, body = self.crypto.unseal(blob, k)
            except CryptoError: 
                self.s3.delete(k)
                self.wm = k
                continue
            if self.cfg.compress:
                try: body = decompress(body)
                except Exception: 
                    self.s3.delete(k)
                    self.wm = k
                    continue
            if mtype == T_FIN and b'"hb"' in body: 
                self.s3.delete(k)
                self.wm = k
                n += 1
                continue
            try: self.dispatch(mtype, body, k)
            except Exception: pass
            self.s3.delete(k)
            self.wm = k
            n += 1
        return n

    def poll_loop(self, poll_ms: int) -> None:
        while self.running:
            try:
                if self.poll() == 0: time.sleep(max(0.01, poll_ms / 1000.0))
            except Exception: time.sleep(0.1)

    def heartbeat_loop(self, every_s: int = 30) -> None:
        while self.running:
            time.sleep(every_s)
            try: self.send_ctl(T_FIN, b'{"hb":1}')
            except Exception: pass

    def register_conn(self, conn: Conn) -> None:
        with self.conns_lock: self.conns[conn.id] = conn

    def teardown(self, conn_id: str, err: str = "") -> None:
        with self.conns_lock: conn = self.conns.pop(conn_id, None)
        if conn is None: return
        conn.closed = True
        try:
            if conn.sock: conn.sock.close()
        except Exception: pass

    def conn_id_of(self, key: str) -> str:
        parts = key.split("/")
        return parts[-2] if len(parts) >= 2 else ""

    def gc_old(self, max_age_hours: int = 24) -> int:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=max_age_hours)
        n = 0
        try: items = self.s3.list(self.chan_root)
        except Exception: return 0
        for it in items:
            try: t = datetime.datetime.fromisoformat(it["mtime"].replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError: continue
            if t < cutoff: 
                self.s3.delete(it["key"])
                n += 1
        return n

def setup_logging(verbose: bool) -> None:
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s", datefmt="%H:%M:%S")

def stats_loop(relay, every_s: int = 30) -> threading.Thread:
    def run():
        while relay.running:
            time.sleep(every_s)
            s = relay.stats; up = (time.time() - s["start"]) or 1
            log.info("stats: up=%d obj down=%d obj puts/s=%.1f gets/s=%.1f conns=%d", s["puts"], s["gets"], s["puts"]/up, s["gets"]/up, len(relay.conns))
    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t
