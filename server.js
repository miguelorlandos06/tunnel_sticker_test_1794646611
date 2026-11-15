const crypto = require("crypto");
const zlib = require("zlib");
const net = require("net");
const fs = require("fs");
const https = require("https");

// --- Configuración ---
const config = JSON.parse(fs.readFileSync("config.server.json", "utf8")).todus_relay;
const bucket = config.bucket;
const chan = config.chan;
const psk = config.psk;
const poll_ms = config.poll_ms || 40;

// Derivar clave con HKDF
const salt = Buffer.from(`todus-relay|${chan}`);
const key = crypto.hkdfSync("sha256", Buffer.from(psk), salt, Buffer.from("todus-relay/v1/aesgcm"), 32);

const conns = new Map();
let wm = null;
let running = true;
const stats = { puts: 0, gets: 0, start: Date.now() };

// --- Peticiones HTTP a S3 (Sin codificar las barras) ---
function s3Request(method, keyPath, body = null, query = "") {
    return new Promise((resolve, reject) => {
        // MinIO en ToDus: la ruta se pasa tal cual, solo codificamos caracteres especiales
        const path = keyPath ? `/${bucket}/${keyPath}${query}` : `/${bucket}/${query}`;
        
        const options = {
            hostname: "s3.todus.cu",
            port: 443,
            path: path,
            method: method,
            headers: { 
                "User-Agent": "ToDus-Node/2.0", 
                "Accept-Encoding": "identity"
            }
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

async function s3Put(keyPath, data) {
    await s3Request("PUT", keyPath, data);
    stats.puts++;
}

async function s3Get(keyPath) {
    const res = await s3Request("GET", keyPath);
    stats.gets++;
    if (res.status === 200) return res.body;
    if (res.status === 404) return null;
    throw new Error(`GET ${keyPath} -> ${res.status}`);
}

async function s3List(prefix, startAfter = null) {
    // Construimos la query manualmente para no codificar las barras (/) del prefix
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

async function s3Delete(keyPath) {
    await s3Request("DELETE", keyPath);
}

// --- Cifrado (AES-256-GCM) ---
const MAGIC = Buffer.from("TR1");

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
    if (flag === 1) return zlib.inflateSync(data); // zlib
    return data; // flag === 0
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
        let found = false;
        
        for (const k of keys) {
            found = true;
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
                } catch (e) {
                    console.error("Error al procesar objeto:", e.message);
                }
            }
            await s3Delete(k);
            wm = k;
        }
        
        // Si no hay objetos, dormimos. Si los hay, volvemos a preguntar casi inmediatamente.
        setTimeout(poll, found ? 10 : 40);
        
    } catch (e) {
        console.error("Poll error:", e.message);
        setTimeout(poll, 500); // En caso de error grave, esperar medio segundo
    }
}

// --- Iniciar ---
console.log("Servidor Node.js v2 activo | canal=" + chan);
poll();

setInterval(() => {
    console.log(`stats: puts=${stats.puts} gets=${stats.gets} conns=${conns.size}`);
}, 30000);
