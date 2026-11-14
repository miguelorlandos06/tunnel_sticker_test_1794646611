"""speedtest.py :: mide throughput del canal."""
import argparse, concurrent.futures as cf, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trcore.transport import S3

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://s3.todus.cu")
    ap.add_argument("--bucket", default="stickers")
    ap.add_argument("--chunks", type=int, default=8)
    ap.add_argument("--kb", type=int, default=256)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--small", action="store_true")
    args = ap.parse_args()
    if args.small: args.chunks, args.kb = 4, 32
    s3 = S3(args.base, args.bucket)
    prefix = f"speed-{int(time.time())}/"
    data = os.urandom(args.kb * 1024)
    keys = [f"{prefix}{i:04d}.bin" for i in range(args.chunks)]
    t0 = time.time()
    with cf.ThreadPoolExecutor(args.workers) as ex:
        list(ex.map(lambda k: s3.put(k, data), keys))
    t1 = time.time()
    with cf.ThreadPoolExecutor(args.workers) as ex:
        got = list(ex.map(s3.get, keys))
    t2 = time.time()
    listed = s3.list(prefix)
    with cf.ThreadPoolExecutor(min(8, len(keys))) as ex:
        list(ex.map(s3.delete, keys))
    assert all(g == data for g in got), "corrupcion"
    up = args.chunks * args.kb / max(t1-t0, 1e-6) / 1024
    down = args.chunks * args.kb / max(t2-t1, 1e-6) / 1024
    print(f"objetos: {args.chunks} x {args.kb} KB | workers={args.workers}")
    print(f"subida:  {up:7.1f} KB/s")
    print(f"bajada:  {down:7.1f} KB/s")
    print(f"LIST:    {len(listed)} claves | limpieza: {len(keys)} borradas")
    print("integridad: OK")
    return 0

if __name__ == "__main__":
    sys.exit(main())
