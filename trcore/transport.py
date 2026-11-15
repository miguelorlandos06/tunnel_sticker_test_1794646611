"""Cliente S3 minimo (Síncrono)."""
import logging, time, random
import xml.etree.ElementTree as ET
from urllib.parse import quote
import requests
from requests.adapters import HTTPAdapter

log = logging.getLogger("tr.transport")
_RETRYABLE = {500, 502, 503, 504, 429}

class TransportError(Exception): pass

class S3:
    def __init__(self, base: str, bucket: str, user_agent: str = "ToDus Relay/1",
                 timeout: int = 40, retries: int = 5, pool_maxsize: int = 100):
        self.base = base.rstrip("/")
        self.bucket = bucket
        self.timeout = timeout
        self.retries = retries
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "identity"})
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=pool_maxsize)
        self.s.mount("https://", adapter)
        self.s.mount("http://", adapter)

    def _url(self, key: str) -> str:
        return f"{self.base}/{self.bucket}/{quote(key)}"

    def put(self, key: str, data: bytes) -> None:
        err = ""
        for i in range(self.retries):
            try:
                r = self.s.put(self._url(key), data=data, timeout=self.timeout)
                if r.status_code in (200, 201): return
                err = f"HTTP {r.status_code}"
                if r.status_code not in _RETRYABLE: raise TransportError(f"PUT {key} -> {err}")
            except requests.RequestException as e: err = repr(e)
            time.sleep(random.uniform(0.01, 0.05))
        raise TransportError(f"PUT {key} agoto reintentos: {err}")

    def get(self, key: str):
        err = ""
        for i in range(self.retries):
            try:
                r = self.s.get(self._url(key), timeout=self.timeout)
                if r.status_code == 200: return r.content
                if r.status_code == 404: return None
                err = f"HTTP {r.status_code}"
                if r.status_code not in _RETRYABLE: raise TransportError(f"GET {key} -> {err}")
            except requests.RequestException as e: err = repr(e)
            time.sleep(random.uniform(0.01, 0.05))
        raise TransportError(f"GET {key} agoto reintentos: {err}")

    def list(self, prefix: str, start_after: str = None, max_keys: int = 1000):
        out, token = [], None
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": str(max_keys)}
            if start_after: params["start-after"] = start_after
            if token: params["continuation-token"] = token
            err = ""
            for i in range(self.retries):
                try:
                    r = self.s.get(self.base + "/" + self.bucket + "/", params=params, timeout=self.timeout)
                    if r.status_code == 200:
                        keys, trunc, token = self._parse_list(r.content)
                        out.extend(keys)
                        start_after = None
                        break
                    err = f"HTTP {r.status_code}"
                    if r.status_code not in _RETRYABLE: raise TransportError(f"LIST {prefix} -> {err}")
                except requests.RequestException as e: err = repr(e)
                time.sleep(random.uniform(0.01, 0.05))
            else: raise TransportError(f"LIST {prefix} agoto reintentos: {err}")
            if not trunc: return out

    @staticmethod
    def _parse_list(xml_bytes: bytes):
        root = ET.fromstring(xml_bytes)
        ns = {"a": "http://s3.amazonaws.com/doc/2006-03-01/"}
        keys = []
        for c in root.findall("a:Contents", ns):
            keys.append({"key": c.findtext("a:Key", "", ns), "size": int(c.findtext("a:Size", "0", ns) or 0), "mtime": c.findtext("a:LastModified", "", ns)})
        trunc = root.findtext("a:IsTruncated", "false", ns).lower() == "true"
        token = root.findtext("a:NextContinuationToken", None, ns)
        return keys, trunc, token

    def delete(self, key: str) -> None:
        try:
            r = self.s.delete(self._url(key), timeout=self.timeout)
        except requests.RequestException: pass
