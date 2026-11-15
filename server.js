const crypto = require("crypto");
const zlib = require("zlib");
const net = require("net");
const fs = require("fs");
const https = require("https");
const { URL } = require("url");

// --- Configuración ---
const config = JSON.parse(fs.readFileSync("config.server.json", "utf8")).todus_relay;
const s3Base = new URL(config.s3_base);
const bucket = config.bucket;
const chan = config.chan;
const psk = config.psk;

// Derivar clave con HKDF (igual que Python)
const salt = Buffer.from(`todus-relay|${chan}`);
const key = crypto.hkdfSync("sha256", Buffer.from(psk), salt, Buffer.from("todus-relay/v1/aesgcm"), 32);

const conns = new Map();
let wm = null;
let running = true;

const stats = { puts: 0, gets: 0, start: Date.now() };

// --- Utilidades S3 (HTTP nativo sin dependencias) ---
function s3Request(method, key, body = null, query = "") {
    return new Promise((resolve, reject) => {
        const path = `/${bucket}/${key}${query}`;
        const options = {
            hostname: s3Base.hostname,
            port: 443,
            path: encodeURI(path),
            method: method,
            headers: { "User-Agent": "ToDus-Node/1.0", "Accept-Encoding": "identity" }
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

async function s3Put(key, data) {
    await s3Request("PUT", key, data);
    stats.puts++;
}

async function s3Get(key) {
    const res = await s3Request("GET", key);
    stats.gets++;
    if (res.status === 200) return res.body;
    if (res.status === 404) return null;
    throw new Error(`GET ${key} -> ${res.status}`);
}

async function s3List(prefix, startAfter = null) {
    let query = `?list-type=2&prefix=${encodeURIComponent(prefix)}&max-keys=1000`;
    if (startAfter) query += `&start-after=${encodeURIComponent(startAfter)}`;
    const res = await s3Request("GET", "", null, query);
    if (res.status !== 200) return [];
    
    const xml = res.body.toString();
    const keys = [];
    const regex = /<Key>(.*?)<\/Key>/g;
    let match;
    while ((match = regex.exec(xml)) !== null) keys.push(match[1]);
    return keys;
}

async function s3Delete(key) {
    await s3Request("DELETE", key);
}

// --- Cifrado (AES-256-GCM) ---
const MAGIC = Buffer.from("TR1");
const HDR = MAGIC.length + 12;

function unseal(blob, path) {
    if (!blob.slice(0, 3).equals(MAGIC)) throw new Error("bad magic");
    const nonce = blob.slice(3, 15);
    const ct = blob.slice(15);
    const decipher = crypto.createDecipheriv("aes-256-gcm", key, nonce);
    decipher.setAAD(Buffer.from(path));
    decipher.setAuthTag(ct.slice(ct.length - 16));
    const pt = Buffer.concat([decipher.update(ct.slice(0, ct.length - 16)), decipher.final()]);
    return { mtype: pt[0], body: pt.slice(1) };
}

function seal(seq, path, mtype, body) {
    const nonce = Buffer.concat([crypto.randomBytes(4), Buffer.from(seq.toString().padStart(8, "0"))]);
    const cipher = crypto.createCipheriv("aes-256-gcm", key, nonce);
    cipher.setAAD(Buffer.from(path));
    const pt = Buffer.concat([Buffer.from([mtype]), body]);
    const ct = Buffer.concat([cipher.update(pt), cipher.final(), cipher.getAuthTag()]);
    return Buffer.concat([MAGIC, nonce, ct]);
}

// --- Descompresión ---
function decompress(buf) {
    const flag = buf[0];
    const data = buf.slice(1);
    if (flag === 0) return data;
    if (flag === 1) return zlib.inflateSync(data); // zlib
    if (flag === 2) return zlib.brotliDecompressSync(data); // zstd fallback (no soportado nativo, usamos brotli)
    return data;
}

function compress(buf) {
    if (buf.length < 256) return Buffer.concat([Buffer.from([0]), buf]);
    const c = zlib.deflateSync(buf);
    return c.length < buf.length ? Buffer.concat([Buffer.from([1]), c]) : Buffer.concat([Buffer.from([0]), buf]);
}

// --- Lógica del Servidor ---
async function sendConn(conn, mtype, body) {
    const compressed = compress(body);
    const seq = conn.seq++;
    const keyPath = `${chan}/s/${conn.id}/${seq.toString().padStart(12, "0")}`;
    const blob = seal(seq, keyPath, mtype, compressed);
    await s3Put(keyPath, blob);
}

async function dial(connId, host, port) {
    return new Promise((resolve) => {
        const sock = net.connect(port, host, () => {
            const conn = { id: connId, sock, seq: 1 };
            conns.set(connId, conn);
            console.log(`conn ${connId} conectada a ${host}:${port}`);
            
            sock.on("data", async (data) => {
                await sendConn(conn, 2, data); // T_DATA
            });
            sock.on("close", async () => {
                await sendConn(conn, 3, Buffer.from(JSON.stringify({ id: connId, err: "fin remoto" }))); // T_FIN
                conns.delete(connId);
            });
            sock.on("error", () => {});
            resolve();
        });
        sock.on("error", async (e) => {
            const tmp = { id: connId, seq: 1 };
            await sendConn(tmp, 3, Buffer.from(JSON.stringify({ id: connId, err: `fallo: ${e.message}` })));
            resolve();
        });
    });
}

async function poll() {
    if (!running) return;
    try {
        const keys = await s3List(`${chan}/c/`, wm);
        for (const k of keys) {
            const blob = await s3Get(k);
            if (blob) {
                try {
                    const { mtype, body } = unseal(blob, k);
                    const raw = decompress(body);
                    const connId = k.split("/")[2];
                    
                    if (mtype === 1) { // T_OPEN
                        const d = JSON.parse(raw.toString());
                        await dial(connId, d.host, d.port);
                    } else if (mtype === 2) { // T_DATA
                        const conn = conns.get(connId);
                        if (conn) conn.sock.write(raw);
                    } else if (mtype === 3) { // T_FIN
                        const conn = conns.get(connId);
                        if (conn) conn.sock.end();
                    }
                } catch (e) {}
            }
            await s3Delete(k);
            wm = k;
        }
    } catch (e) {}
    setTimeout(poll, 40);
}

// --- Iniciar ---
console.log("Servidor Node.js activo | canal=" + chan);
poll();

// Estadísticas
setInterval(() => {
    console.log(`stats: puts=${stats.puts} gets=${stats.gets} conns=${conns.size}`);
}, 30000);
