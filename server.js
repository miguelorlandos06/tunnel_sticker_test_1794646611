const crypto = require("crypto");
const zlib = require("zlib");
const net = require("net");
const fs = require("fs");
const https = require("https");

// --- Configuración ---
const config = JSON.parse(fs.readFileSync("config.server.json", "utf8")).todus_relay;
const bucket = config.bucket;
const psk = config.psk;
const poll_ms = config.poll_ms || 50;
const chunkKb = config.chunk_kb || 256;
const maxInflight = config.inflight || 64;

function deriveKey(chan) {
    const salt = Buffer.from(`todus-relay|${chan}`);
    return crypto.hkdfSync("sha256", Buffer.from(psk), salt, Buffer.from("todus-relay/v1/aesgcm"), 32);
}

const activeChannels = new Map(); // chan -> { key, wm, conns: Map }
const stats = { puts: 0, gets: 0, start: Date.now() };

// --- S3 Helpers ---
function s3Request(method, keyPath, body = null, query = "") {
    return new Promise((resolve, reject) => {
        const path = keyPath ? `/${bucket}/${keyPath}${query}` : `/${bucket}/${query}`;
        const options = {
            hostname: "s3.todus.cu", port: 443, path: path, method: method,
            headers: { "User-Agent": "NuvolaVPN/1.0", "Accept-Encoding": "identity" }
        };
        if (body) options.headers["Content-Length"] = body.length;
        const req = https.request(options, (res) => {
            let data = [];
            res.on("data", chunk => data.push(chunk));
            res.on("end", () => resolve({ status: res.statusCode, body: Buffer.concat(data) }));
        });
        req.on("error", reject);
        if (body) req.write(body);
        req.end();
    });
}

async function s3Put(keyPath, data) { await s3Request("PUT", keyPath, data); stats.puts++; }
async function s3Get(keyPath) {
    const res = await s3Request("GET", keyPath); stats.gets++;
    if (res.status === 200) return res.body;
    if (res.status === 404) return null;
    throw new Error(`GET ${keyPath} -> ${res.status}`);
}
async function s3List(prefix, startAfter = null) {
    let query = `?list-type=2&prefix=${prefix}`;
    if (startAfter) query += `&start-after=${startAfter}`;
    const res = await s3Request("GET", null, null, query);
    if (res.status !== 200) return [];
    const xml = res.body.toString();
    const keys = [];
    const regex = /<Key>(.*?)<\/Key>/g;
    let match;
    while ((match = regex.exec(xml)) !== null) keys.push(match[1]);
    return keys;
}
async function s3Delete(keyPath) { await s3Request("DELETE", keyPath); }

// --- Cifrado y Compresión ---
const MAGIC = Buffer.from("TR1");
function seal(key, seq, path, mtype, body) {
    const nonce = Buffer.alloc(12);
    crypto.randomFillSync(nonce, 0, 4);
    nonce.writeBigUInt64BE(BigInt(seq), 4);
    const cipher = crypto.createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(Buffer.from(path));
    const pt = Buffer.concat([Buffer.from([mtype]), body]);
    const ct = Buffer.concat([cipher.update(pt), cipher.final(), cipher.getAuthTag()]);
    return Buffer.concat([MAGIC, nonce, ct]);
}
function unseal(key, blob, path) {
    if (!blob.slice(0, 3).equals(MAGIC)) throw new Error("bad magic");
    const nonce = blob.slice(3, 15);
    const ct = blob.slice(15);
    const decipher = crypto.createDecipheriv("aes-256-gcm", key, nonce);
    decipher.setAAD(Buffer.from(path));
    decipher.setAuthTag(ct.slice(ct.length - 16));
    const pt = Buffer.concat([decipher.update(ct.slice(0, ct.length - 16)), decipher.final()]);
    return { mtype: pt[0], body: pt.slice(1) };
}

function compress(buf) {
    if (buf.length < 256) return Buffer.concat([Buffer.from([0]), buf]);
    const c = zlib.deflateSync(buf);
    return c.length < buf.length ? Buffer.concat([Buffer.from([1]), c]) : Buffer.concat([Buffer.from([0]), buf]);
}
function decompress(buf) {
    const flag = buf[0];
    const data = buf.slice(1);
    if (flag === 1) return zlib.inflateSync(data, { maxOutputLength: 16 * 1024 * 1024 });
    return data;
}

// --- Lógica de Conexión (por canal) ---
async function sendConn(chanCtx, conn, mtype, body) {
    const compressed = compress(body);
    const seq = conn.seq++;
    const keyPath = `${chanCtx.chan}/s/${conn.id}/${seq.toString().padStart(12, "0")}`;
    const blob = seal(chanCtx.key, seq, keyPath, mtype, compressed);
    await s3Put(keyPath, blob).catch(console.error);
}

async function dial(chanCtx, connId, host, port) {
    return new Promise((resolve) => {
        const sock = net.connect(port, host, () => {
            const conn = { id: connId, sock, seq: 1 };
            chanCtx.conns.set(connId, conn);
            console.log(`[${chanCtx.chan}] conn ${connId} conectada a ${host}:${port}`);
            
            sock.on("data", async (data) => {
                // Partir los datos en chunks del tamaño configurado (256 KB)
                for (let i = 0; i < data.length; i += chunkKb * 1024) {
                    const chunk = data.slice(i, i + chunkKb * 1024);
                    await sendConn(chanCtx, conn, 2, chunk);
                }
            });
            sock.on("close", async () => {
                await sendConn(chanCtx, conn, 3, Buffer.from(JSON.stringify({ id: connId, err: "fin remoto" })));
                chanCtx.conns.delete(connId);
            });
            sock.on("error", () => {});
            resolve();
        });
        sock.on("error", async (e) => {
            const tmp = { id: connId, seq: 1 };
            await sendConn(chanCtx, tmp, 3, Buffer.from(JSON.stringify({ id: connId, err: `fallo: ${e.message}` })));
            resolve();
        });
    });
}

// --- Polling por Canal ---
async function pollChannel(chanCtx) {
    try {
        const keys = await s3List(`${chanCtx.chan}/c/`, chanCtx.wm);
        let found = false;
        
        for (const k of keys) {
            found = true;
            let nextWm = null;
            try {
                const blob = await s3Get(k);
                if (blob) {
                    const { mtype, body } = unseal(chanCtx.key, blob, k);
                    const raw = decompress(body);
                    const connId = k.split("/")[2];
                    
                    if (mtype === 1) {
                        const d = JSON.parse(raw.toString());
                        await dial(chanCtx, connId, d.host, d.port);
                    } else if (mtype === 2) {
                        const conn = chanCtx.conns.get(connId);
                        if (conn) conn.sock.write(raw);
                    } else if (mtype === 3) {
                        const conn = chanCtx.conns.get(connId);
                        if (conn) { conn.sock.end(); chanCtx.conns.delete(connId); }
                    }
                }
                nextWm = k;
            } catch (e) {
                console.error(`[${chanCtx.chan}] Error en ${k}:`, e.message);
                nextWm = k;
            } finally {
                await s3Delete(k).catch(() => {});
                if (nextWm) chanCtx.wm = nextWm;
            }
        }
        setTimeout(() => pollChannel(chanCtx), found ? 10 : poll_ms);
    } catch (e) {
        setTimeout(() => pollChannel(chanCtx), 1000);
    }
}

// --- Escáner de Nuevos Canales ---
async function scanChannels() {
    try {
        const res = await s3Request("GET", "registro/activos.json");
        if (res.status === 200) {
            const data = JSON.parse(res.body.toString());
            const canales = data.canales || [];
            
            for (const chan of canales) {
                if (!activeChannels.has(chan)) {
                    console.log(`[+] Nuevo canal detectado: ${chan}`);
                    const chanCtx = { chan, key: deriveKey(chan), wm: null, conns: new Map() };
                    activeChannels.set(chan, chanCtx);
                    pollChannel(chanCtx);
                }
            }
        }
    } catch (e) {}
    setTimeout(scanChannels, 30000);
}

// --- Iniciar ---
console.log("Servidor Node.js Multi-Canal activo (alineado con APK)");
scanChannels();

setInterval(() => {
    console.log(`stats: puts=${stats.puts} gets=${stats.gets} canales_activos=${activeChannels.size}`);
}, 30000);
