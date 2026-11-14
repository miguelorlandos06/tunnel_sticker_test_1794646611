"""Configuracion compartida cliente/servidor."""
import json
import re
from dataclasses import dataclass, field

def _parse_listen(s: str):
    m = re.match(r"^([\w.\-]+):(\d+)$", s.strip())
    if not m:
        raise ValueError(f"listen invalido: {s!r} (formato host:puerto)")
    return m.group(1), int(m.group(2))

@dataclass
class Config:
    s3_base: str = "https://s3.todus.cu"
    bucket: str = "stickers"
    chan: str = ""
    psk: str = ""
    user_agent: str = "ToDus 0.40.29 HTTP-Upload"
    timeout_s: int = 40
    retries: int = 5
    chunk_kb: int = 256
    poll_ms: int = 300
    workers: int = 5
    inflight: int = 10
    gc_hours: int = 24
    compress: bool = True
    listen: str = "127.0.0.1:1080"
    dial_timeout_s: int = 12
    idle_timeout_s: int = 600
    blocked_hosts: list = field(default_factory=list)

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        cfg = Config()
        for k, v in (raw.get("todus_relay") or raw).items():
            if not hasattr(cfg, k):
                raise ValueError(f"clave de config desconocida: {k}")
            setattr(cfg, k, v)
        if not re.fullmatch(r"[0-9a-f]{16}", cfg.chan):
            raise ValueError("chan debe ser 16 caracteres hex")
        if len(cfg.psk) < 16:
            raise ValueError("psk demasiado corta: usa 32+ caracteres")
        if not (8 <= cfg.chunk_kb <= 4096):
            raise ValueError("chunk_kb fuera de rango (8..4096)")
        if not (1 <= cfg.workers <= 16):
            raise ValueError("workers fuera de rango (1..16)")
        if not (1 <= cfg.inflight <= 32):
            raise ValueError("inflight fuera de rango (1..32)")
        if not (128 <= cfg.poll_ms <= 5000):
            raise ValueError("poll_ms fuera de rango (128..5000)")
        return cfg

    def listen_addr(self):
        return _parse_listen(self.listen)
