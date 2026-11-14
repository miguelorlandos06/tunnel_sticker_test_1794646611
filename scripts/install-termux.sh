#!/data/data/com.termux/files/usr/bin/bash
set -e
echo "== 1/3 Dependencias =="
pkg update -y
pkg install -y python python-cryptography zstd
pip install -q requests zstandard
echo "== 2/3 Verificando S3 =="
cd "$(dirname "$0")/.."
python3 probe.py
echo "== 3/3 Listo. Siguiente:"
echo "  cp config.client.json config.json   # edita chan/psk"
echo "  python3 client.py --config config.json"
