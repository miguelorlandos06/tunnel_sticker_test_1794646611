"""probe.py :: verifica el transporte y calibra."""
import argparse, os, re, sys, time, urllib.request
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
OK, WARN, FAIL = "[OK]  ", "[AVISO]", "[FALLO]"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://s3.todus.cu")
    ap.add_argument("--bucket", default="stickers")
    ap.add_argument("--size-kb", type=int, default=256)
    args = ap.parse_args()
    base = args.base.rstrip("/")
    fails = 0
    def req(method, url, data=None, timeout=30):
        r = urllib.request.Request(url, data=data, method=method)
        return urllib.request.urlopen(r, timeout=timeout)
    try:
        try: req("GET", base + "/")
        except urllib.error.HTTPError: pass
    except Exception as e:
        print(f"{FAIL} {base} no responde: {e}"); return 1
    print(f"{OK} endpoint alcanzable: {base}")
    try:
        r = req("GET", f"{base}/{args.bucket}/?list-type=2&max-keys=5")
        body = r.read().decode()
        if "<ListBucketResult" in body:
            n = len(re.findall(r"<Key>", body))
            print(f"{OK} bucket {args.bucket} publico y listable ({n} claves)")
        else:
            print(f"{WARN} bucket responde sin ListBucketResult"); fails += 1
    except Exception as e:
        print(f"{FAIL} bucket no listable: {e}"); return 1
    key = f"probe-{int(time.time())}-{os.urandom(4).hex()}.bin"
    url = f"{base}/{args.bucket}/{key}"
    data = os.urandom(args.size_kb * 1024)
    t0 = time.time(); req("PUT", url, data=data); t1 = time.time()
    got = req("GET", url).read(); t2 = time.time()
    match = got == data
    req("DELETE", url); t3 = time.time()
    if match:
        print(f"{OK} roundtrip {args.size_kb}KB: PUT {(t1-t0)*1000:.0f} ms | "
              f"GET {(t2-t1)*1000:.0f} ms | DELETE {(t3-t2)*1000:.0f} ms")
    else:
        print(f"{FAIL} el GET no devolvio los mismos bytes"); fails += 1
    rtt = (t1 - t0 + t2 - t1) / 2
    poll = max(150, min(600, int(rtt * 1000 / 1.5)))
    chunk = 128 if rtt > 1.5 else (256 if rtt > 0.8 else 384)
    print(f"{OK} calibracion sugerida: poll_ms={poll} chunk_kb={chunk}")
    return 1 if fails else 0

if __name__ == "__main__":
    sys.exit(main())
