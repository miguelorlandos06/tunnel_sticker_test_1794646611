"""Cliente S3 asíncrono con aiohttp."""
import logging
import xml.etree.ElementTree as ET
from urllib.parse import quote
import aiohttp

log = logging.getLogger("tr.transport")
_RETRYABLE = {500, 502, 503, 504, 429}

class TransportError(Exception):
    pass

class S3:
    def __init__(self, base: str, bucket: str, user_agent: str = "ToDus Relay/1",
                 timeout: int = 40, retries: int = 5, **kwargs):
        self.base = base.rstrip("/")
        self.bucket = bucket
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retries = retries
        self.headers = {"User-Agent": user_agent, "Accept-Encoding": "identity"}
        self.session = None

    async def _get_session(self):
        if self.session is None or self.session.closed:
            # Límite de conexiones concurrentes
            conn = aiohttp.TCPConnector(limit=100, force_close=False)
            self.session = aiohttp.ClientSession(headers=self.headers, connector=conn, timeout=self.timeout)
        return self.session

    def _url(self, key: str) -> str:
        return f"{self.base}/{self.bucket}/{quote(key)}"

    async def put(self, key: str, data: bytes) -> None:
        err = ""
        session = await self._get_session()
        for i in range(self.retries):
            try:
                async with session.put(self._url(key), data=data) as r:
                    if r.status in (200, 201): return
                    err = f"HTTP {r.status}"
                    if r.status not in _RETRYABLE: raise TransportError(f"PUT {key} -> {err}")
            except Exception as e:
                err = repr(e)
            await aiohttp.helpers.asyncio.sleep(0.01)
        raise TransportError(f"PUT {key} agoto reintentos: {err}")

    async def get(self, key: str):
        err = ""
        session = await self._get_session()
        for i in range(self.retries):
            try:
                async with session.get(self._url(key)) as r:
                    if r.status == 200: return await r.read()
                    if r.status == 404: return None
                    err = f"HTTP {r.status}"
                    if r.status not in _RETRYABLE: raise TransportError(f"GET {key} -> {err}")
            except Exception as e:
                err = repr(e)
            await aiohttp.helpers.asyncio.sleep(0.01)
        raise TransportError(f"GET {key} agoto reintentos: {err}")

    async def list(self, prefix: str, start_after: str = None, max_keys: int = 1000):
        out, token = [], None
        session = await self._get_session()
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": str(max_keys)}
            if start_after: params["start-after"] = start_after
            if token: params["continuation-token"] = token
            err = ""
            for i in range(self.retries):
                try:
                    async with session.get(self.base + "/" + self.bucket + "/", params=params) as r:
                        if r.status == 200:
                            keys, trunc, token = self._parse_list(await r.read())
                            out.extend(keys)
                            start_after = None
                            break
                        err = f"HTTP {r.status}"
                        if r.status not in _RETRYABLE: raise TransportError(f"LIST {prefix} -> {err}")
                except Exception as e:
                    err = repr(e)
                await aiohttp.helpers.asyncio.sleep(0.01)
            else:
                raise TransportError(f"LIST {prefix} agoto reintentos: {err}")
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

    async def delete(self, key: str) -> None:
        session = await self._get_session()
        try:
            async with session.delete(self._url(key)) as r:
                pass
        except Exception:
            pass
            
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
