"use strict";
/* =========================================================================
   UConnect API for Vercel (one serverless function, Neon Postgres).
   Same security model as the standalone server: server-side access code and
   PIN checks, hashed secrets, sessions in the database, lockouts, audit log
   with IP addresses, role checks, Claude API proxy, private chunked files.
   ========================================================================= */
const crypto = require("crypto");
const zlib = require("zlib");
const pg = require("pg");

pg.types.setTypeParser(20, (v) => Number(v)); // bigint → number (all our values fit)
/* Neon and other Postgres add-ons expose the connection string under a few
   different names depending on how the integration was connected. Take the
   first one that looks like a Postgres URL so a working database is never
   missed because of the variable's name. */
const DB_URL = (function () {
  const named = ["DATABASE_URL", "POSTGRES_URL", "POSTGRES_PRISMA_URL", "NEON_DATABASE_URL", "POSTGRES_URL_NON_POOLING", "DATABASE_URL_UNPOOLED"];
  for (const k of named) { const v = process.env[k]; if (v && /^postgres(ql)?:\/\//i.test(v)) return v; }
  for (const [k, v] of Object.entries(process.env)) if (/^postgres(ql)?:\/\//i.test(String(v || "")) && !/UNPOOLED|NON_POOLING/i.test(k)) return v;
  for (const v of Object.values(process.env)) if (/^postgres(ql)?:\/\//i.test(String(v || ""))) return v;
  return "";
})();
const pool = DB_URL ? new pg.Pool({ connectionString: DB_URL, max: 4, idleTimeoutMillis: 10000 }) : null;
const q = async (text, params) => (await pool.query(text, params)).rows;
const q1 = async (text, params) => (await q(text, params))[0] || null;

const TZ = process.env.APP_TIMEZONE || "Europe/London";
const PBKDF2_ITER = 210000;
const MAX_JSON = 4 * 1024 * 1024;
const CHUNK = 3 * 1024 * 1024;
const MAX_FILE = 25 * 1024 * 1024;
const GATE_MINUTES = 15;
const AI_MODEL = process.env.ANTHROPIC_MODEL || "claude-sonnet-5";
const AI_MODEL_QUICK = process.env.ANTHROPIC_MODEL_QUICK || "claude-haiku-4-5-20251001";
const OVERLAP = 5000; // poll window overlap (ms) so no change is ever missed

/* ---------------- schema (created on first request) ---------------- */
const SCHEMA_VERSION = 1;
const SCHEMA = `
BEGIN;
SELECT pg_advisory_xact_lock(774411);
CREATE OR REPLACE FUNCTION uc_now() RETURNS bigint LANGUAGE sql VOLATILE AS $f$ SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint $f$;
CREATE OR REPLACE FUNCTION uc_merge(a jsonb, b jsonb) RETURNS jsonb LANGUAGE plpgsql IMMUTABLE AS $f$
DECLARE k text; v jsonb; r jsonb;
BEGIN
  IF a IS NULL OR jsonb_typeof(a) <> 'object' OR jsonb_typeof(b) <> 'object' THEN RETURN b; END IF;
  r := a;
  FOR k, v IN SELECT * FROM jsonb_each(b) LOOP
    IF jsonb_typeof(v) = 'object' AND jsonb_typeof(r->k) = 'object' THEN r := jsonb_set(r, ARRAY[k], uc_merge(r->k, v));
    ELSE r := r || jsonb_build_object(k, v); END IF;
  END LOOP;
  RETURN r;
END $f$;
CREATE TABLE IF NOT EXISTS docs(path text PRIMARY KEY, coll text NOT NULL, data jsonb, updated_at bigint NOT NULL);
CREATE INDEX IF NOT EXISTS docs_updated ON docs(updated_at);
CREATE INDEX IF NOT EXISTS docs_coll ON docs(coll);
CREATE TABLE IF NOT EXISTS users(id text PRIMARY KEY, name text NOT NULL, role text NOT NULL, pin_salt text NOT NULL, pin_hash text NOT NULL, iter int NOT NULL,
  active boolean NOT NULL DEFAULT true, created_at bigint, created_by text, last_login_at bigint, last_login_device text, last_login_ip text, updated_at bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS sessions(token_hash text PRIMARY KEY, user_id text NOT NULL, created_at bigint NOT NULL, last_seen bigint NOT NULL, last_poll bigint NOT NULL DEFAULT 0, view text, locked boolean NOT NULL DEFAULT false, ip text, device text);
CREATE TABLE IF NOT EXISTS gates(token_hash text PRIMARY KEY, created_at bigint NOT NULL, ip text);
CREATE TABLE IF NOT EXISTS kv(k text PRIMARY KEY, v jsonb, updated_at bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS files(id text PRIMARY KEY, name text, type text, size bigint, chunks int, complete boolean NOT NULL DEFAULT false, created_at bigint, created_by text);
CREATE TABLE IF NOT EXISTS file_chunks(file_id text NOT NULL, n int NOT NULL, data bytea NOT NULL, PRIMARY KEY(file_id, n));
CREATE TABLE IF NOT EXISTS fails(key text PRIMARY KEY, n int NOT NULL, first bigint NOT NULL, until bigint NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS audit(id text PRIMARY KEY, day text NOT NULL, t bigint NOT NULL, u text, n text, a text, e text, i text, s text, d text);
CREATE INDEX IF NOT EXISTS audit_day ON audit(day);
CREATE INDEX IF NOT EXISTS audit_t ON audit(t);
COMMIT;`;
let ready = null;
function init() {
  if (!ready) ready = (async () => {
    const v = await q1("SELECT to_regclass('public.kv') AS t").catch(() => null);
    const cur = v && v.t ? await q1("SELECT v FROM kv WHERE k='schema'") : null;
    if (!cur || Number(cur.v) < SCHEMA_VERSION) {
      await pool.query(SCHEMA);
      await q("INSERT INTO kv(k,v,updated_at) VALUES('schema',$1::jsonb,uc_now()) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v", [JSON.stringify(SCHEMA_VERSION)]);
    }
    await firstStart();
  })().catch((e) => { ready = null; throw e; });
  return ready;
}

/* ---------------- helpers ---------------- */
const now = () => Date.now();
const rid = (p = "") => p + Date.now().toString(36) + crypto.randomBytes(5).toString("hex");
const sha = (s) => crypto.createHash("sha256").update(s).digest("hex");
const kvGet = async (k, d = null) => { const r = await q1("SELECT v FROM kv WHERE k=$1", [k]); return r ? r.v : d; };
const kvSet = (k, v) => q("INSERT INTO kv(k,v,updated_at) VALUES($1,$2::jsonb,uc_now()) ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v, updated_at=EXCLUDED.updated_at", [k, JSON.stringify(v)]);
const dayKey = (t = now()) => new Date(t).toLocaleDateString("en-CA", { timeZone: TZ });
const dbNow = async () => (await q1("SELECT uc_now() AS t")).t;
function pbkdf2(secret, salt, iter) { return new Promise((res, rej) => crypto.pbkdf2(String(secret), Buffer.from(salt, "base64"), iter, 32, "sha256", (e, k) => (e ? rej(e) : res(k.toString("base64"))))); }
async function hashSecret(secret) { const salt = crypto.randomBytes(16).toString("base64"); return { salt, hash: await pbkdf2(secret, salt, PBKDF2_ITER), iter: PBKDF2_ITER }; }
async function verifySecret(secret, salt, hash, iter) { if (!salt || !hash) return false; const h = Buffer.from(await pbkdf2(secret, salt, iter || PBKDF2_ITER)); const H = Buffer.from(hash); return h.length === H.length && crypto.timingSafeEqual(h, H); }
const policy = async () => ({ idleMinutes: 15, maxAttempts: 5, lockMinutes: 15, maxSessionHours: 12, ...(await kvGet("policy", {})) });
const PIN_OK = (p) => /^\d{6,12}$/.test(p) && !/^(\d)\1+$/.test(p) && !"01234567890123".includes(p) && !"98765432109876".includes(p);
const CODE_OK = (c) => typeof c === "string" && c.length >= 8 && /[A-Za-z]/.test(c) && /\d/.test(c);
const SEG = /^[A-Za-z0-9_\-.~:@+]{1,200}$/;
function validPath(p) { if (typeof p !== "string" || p.length > 1000) return false; const s = p.split("/"); return s.length >= 2 && s.length <= 16 && s.length % 2 === 0 && s.every((x) => SEG.test(x) && x !== "." && x !== ".."); }
const collOf = (p) => p.split("/").slice(0, -1).join("/");
const isObj = (v) => v && typeof v === "object" && !Array.isArray(v);

/* ---------------- documents ---------------- */
async function getDoc(p) { const r = await q1("SELECT data FROM docs WHERE path=$1", [p]); return r ? r.data : null; }
const setDoc = (p, data) => q("INSERT INTO docs(path,coll,data,updated_at) VALUES($1,$2,$3::jsonb,uc_now()) ON CONFLICT(path) DO UPDATE SET data=EXCLUDED.data, updated_at=EXCLUDED.updated_at", [p, collOf(p), data === null ? null : JSON.stringify(data)]);
async function mergeDoc(p, patch) { const r = await q("UPDATE docs SET data=uc_merge(data,$2::jsonb), updated_at=uc_now() WHERE path=$1 AND data IS NOT NULL RETURNING path", [p, JSON.stringify(patch)]); return r.length > 0; }
function userDoc(u) { return { name: u.name, role: u.role, active: !!u.active, createdAt: u.created_at, createdBy: u.created_by, lastLoginAt: u.last_login_at, lastLoginDevice: [u.last_login_device, u.last_login_ip].filter(Boolean).join(" · ") }; }
async function securityDoc() { const code = (await kvGet("code", {})) || {}; return { ...(await policy()), codeRotatedAt: code.rotatedAt || null, codeRotatedBy: code.rotatedBy || null }; }
async function auditDayDocs(days) {
  if (!days.length) return [];
  const rows = await q("SELECT * FROM audit WHERE day = ANY($1::text[]) ORDER BY t", [days]);
  const byDay = new Map(days.map((d) => [d, {}]));
  for (const r of rows) byDay.get(r.day)[r.id] = { t: r.t, u: r.u, n: r.n, a: r.a, e: r.e, i: r.i, s: r.s, d: r.d };
  return days.map((day) => ({ p: `audit/${day}`, d: { day, ev: byDay.get(day) } }));
}
async function audit(a, e, i, s, { user, ip, device } = {}) {
  await q("INSERT INTO audit(id,day,t,u,n,a,e,i,s,d) VALUES($1,$2,uc_now(),$3,$4,$5,$6,$7,$8,$9)", [rid("e"), dayKey(), user ? user.id : null, user ? user.name : "Unknown", a, e || "", i || "", String(s || "").slice(0, 400), [device, ip].filter(Boolean).join(" · ").slice(0, 200)]);
}

/* ---------------- lockouts ---------------- */
async function lockedFor(key) { const r = await q1("SELECT until FROM fails WHERE key=$1", [key]); return r && r.until > now() ? r.until - now() : 0; }
async function fail(key) {
  const pol = await policy(); const r = await q1("SELECT * FROM fails WHERE key=$1", [key]);
  let n = 1, first = now(); if (r && now() - r.first < 30 * 60000) { n = r.n + 1; first = r.first; }
  const until = n >= pol.maxAttempts ? now() + pol.lockMinutes * 60000 : 0;
  await q("INSERT INTO fails(key,n,first,until) VALUES($1,$2,$3,$4) ON CONFLICT(key) DO UPDATE SET n=EXCLUDED.n, first=EXCLUDED.first, until=EXCLUDED.until", [key, until ? 0 : n, first, until]);
  return { n, max: pol.maxAttempts, locked: !!until, minutes: pol.lockMinutes };
}
const clearFails = (key) => q("DELETE FROM fails WHERE key=$1", [key]);

/* ---------------- HTTP helpers ---------------- */
function clientIp(req) { return String(req.headers["x-real-ip"] || String(req.headers["x-forwarded-for"] || "").split(",")[0] || req.socket?.remoteAddress || "").trim(); }
function cookies(req) { const o = {}; String(req.headers.cookie || "").split(";").forEach((p) => { const i = p.indexOf("="); if (i > 0) o[p.slice(0, i).trim()] = decodeURIComponent(p.slice(i + 1).trim()); }); return o; }
function setCookie(res, name, val, maxAgeSec) {
  const c = `${name}=${encodeURIComponent(val)}; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=${maxAgeSec}`;
  const prev = res.getHeader("Set-Cookie"); res.setHeader("Set-Cookie", [...(prev ? [].concat(prev) : []), c]);
}
function send(res, req, status, body, headers = {}) {
  const json = !Buffer.isBuffer(body) && typeof body !== "string";
  let buf = Buffer.isBuffer(body) ? body : Buffer.from(json ? JSON.stringify(body) : body);
  const h = { "Content-Type": json ? "application/json; charset=utf-8" : "text/plain; charset=utf-8", "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", ...headers };
  if (json && buf.length > 2048 && /\bgzip\b/.test(req.headers["accept-encoding"] || "")) { buf = zlib.gzipSync(buf); h["Content-Encoding"] = "gzip"; h.Vary = "Accept-Encoding"; }
  h["Content-Length"] = buf.length; res.writeHead(status, h); res.end(buf);
}
const err = (res, req, status, code, error, extra = {}) => send(res, req, status, { code, error, ...extra });
/* Read the raw request body.
   Normal path: read the stream ourselves, so the size limit is enforced.
   Vercel's Node bridge buffers the body before the handler runs and exposes it
   as rawBody, or as a lazily parsed `body` getter. Only fall back to those when
   the stream has already finished, so the limit still applies whenever it can,
   and never let the getter's own parse error escape as a 500. */
function fromParsed(req, limit) {
  if (Buffer.isBuffer(req.rawBody)) return cap(req.rawBody, limit);
  if (typeof req.rawBody === "string") return cap(Buffer.from(req.rawBody), limit);
  let b;
  try { b = req.body; } catch { throw Object.assign(new Error("bad json"), { status: 400 }); }
  if (b === undefined || b === null) return Buffer.alloc(0);
  if (Buffer.isBuffer(b)) return cap(b, limit);
  if (typeof b === "string") return cap(Buffer.from(b), limit);
  let s; try { s = JSON.stringify(b); } catch { return Buffer.alloc(0); } // unserialisable: treat as no body
  return cap(Buffer.from(s === undefined ? "" : s), limit); // the size limit must not be swallowed here
}
function cap(buf, limit) { if (buf.length > limit) throw Object.assign(new Error("too large"), { status: 413 }); return buf; }
function readBody(req, limit) {
  if (Buffer.isBuffer(req.rawBody) || typeof req.rawBody === "string" || req.readableEnded || req.complete) {
    try { return Promise.resolve(fromParsed(req, limit)); } catch (e) { return Promise.reject(e); }
  }
  return new Promise((resolve, reject) => {
    const chunks = []; let size = 0;
    req.on("data", (c) => { size += c.length; if (size > limit) { reject(Object.assign(new Error("too large"), { status: 413 })); req.destroy(); } else chunks.push(c); });
    req.on("end", () => resolve(Buffer.concat(chunks))); req.on("error", reject);
  });
}
async function readJSON(req) { const b = await readBody(req, MAX_JSON); if (!b.length) return {}; try { return JSON.parse(b.toString("utf8")); } catch { throw Object.assign(new Error("bad json"), { status: 400 }); } }

/* ---------------- sessions ---------------- */
async function sessionFrom(req) {
  const tok = cookies(req).uc_session; if (!tok) return null;
  const s = await q1("SELECT s.*, u.name, u.role, u.active, u.pin_salt, u.pin_hash, u.iter FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=$1", [sha(tok)]);
  if (!s) return null;
  const user = { id: s.user_id, name: s.name, role: s.role, pin_salt: s.pin_salt, pin_hash: s.pin_hash, iter: s.iter };
  if (!s.active) { await q("DELETE FROM sessions WHERE token_hash=$1", [s.token_hash]); return null; }
  const pol = await policy();
  if (now() - s.created_at > pol.maxSessionHours * 3600000) { await q("DELETE FROM sessions WHERE token_hash=$1", [s.token_hash]); await audit("logout", "users", user.id, "Signed out (maximum session length reached)", { user, ip: s.ip, device: s.device }); return null; }
  if (!s.locked && now() - s.last_seen > pol.idleMinutes * 60000) { await q("UPDATE sessions SET locked=true WHERE token_hash=$1", [s.token_hash]); s.locked = true; await audit("lock", "users", user.id, "Screen locked after inactivity", { user, ip: s.ip, device: s.device }); }
  return { ...s, user };
}
const touch = (s) => q("UPDATE sessions SET last_seen=$2 WHERE token_hash=$1", [s.token_hash, now()]);
async function createSession(res, req, u, device) {
  const tok = crypto.randomBytes(32).toString("base64url");
  await q("INSERT INTO sessions(token_hash,user_id,created_at,last_seen,locked,ip,device) VALUES($1,$2,$3,$3,false,$4,$5)", [sha(tok), u.id, now(), clientIp(req), String(device || "").slice(0, 160)]);
  setCookie(res, "uc_session", tok, (await policy()).maxSessionHours * 3600); setCookie(res, "uc_gate", "", 0);
}
async function gateOk(req) { const g = cookies(req).uc_gate; if (!g) return false; const r = await q1("SELECT created_at FROM gates WHERE token_hash=$1", [sha(g)]); return !!r && now() - r.created_at < GATE_MINUTES * 60000; }
const kickUser = (uid) => q("DELETE FROM sessions WHERE user_id=$1", [uid]);
const touchUser = (id) => q("UPDATE users SET updated_at=uc_now() WHERE id=$1", [id]);
const publicUsers = () => q("SELECT id,name,role FROM users WHERE active ORDER BY name");
const meOf = (u) => ({ id: u.id, name: u.name, role: u.role });
const hasUsers = async () => !!(await q1("SELECT 1 AS x FROM users LIMIT 1"));

/* ---------------- permissions ---------------- */
const WRITABLE = new Set(["contacts", "deals", "intros", "tasks", "docs", "matches"]);
const MEMBER_DELETE = new Set(["tasks", "matches"]);
function canWrite(s, p, del) {
  const c = collOf(p);
  if (p === "settings/general") return s.user.role === "superadmin";
  if (!WRITABLE.has(c)) return false;
  if (del && !MEMBER_DELETE.has(c)) return s.user.role === "superadmin";
  return true;
}

/* ---------------- Claude ---------------- */
async function callClaude({ input, tier, json, images }, signal) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) return { status: 503, body: { code: "not_configured", error: "AI isn't set up yet. Add ANTHROPIC_API_KEY in Vercel." } };
  let messages = typeof input === "string" ? [{ role: "user", content: input }] : Array.isArray(input) ? input.filter((m) => m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string").map((m) => ({ role: m.role, content: m.content })) : [];
  if (!messages.length || messages[messages.length - 1].role !== "user") return { status: 400, body: { code: "invalid_request", error: "Nothing to ask." } };
  if (messages.reduce((n, m) => n + m.content.length, 0) > 200000) return { status: 413, body: { code: "prompt_too_large", error: "Too much text for one request." } };
  const imgs = (Array.isArray(images) ? images : []).slice(0, 8).filter((im) => im && /^image\/(jpeg|png|webp|gif)$/.test(im.type) && typeof im.data === "string");
  if (imgs.length) { const last = messages[messages.length - 1]; last.content = [...imgs.map((im) => ({ type: "image", source: { type: "base64", media_type: im.type, data: im.data } })), { type: "text", text: last.content }]; }
  const system = "You are the analysis engine inside UConnect, a private relationship platform for a principal who allocates, co-invests and places private capital. Follow the requested output format exactly and use UK English." + (json ? " Reply with only valid JSON: no prose, no code fences." : "");
  const r = await fetch((process.env.ANTHROPIC_BASE_URL || "https://api.anthropic.com") + "/v1/messages", { method: "POST", signal, headers: { "x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json" }, body: JSON.stringify({ model: tier === "quick" ? AI_MODEL_QUICK : AI_MODEL, max_tokens: 8000, system, messages }) });
  const j = await r.json().catch(() => ({}));
  if (!r.ok) { console.error("Claude API", r.status, j && j.error); return { status: 502, body: { code: r.status === 429 || r.status === 529 ? "rate_limited" : r.status === 400 ? "invalid_request" : "upstream_error", error: (j && j.error && j.error.message) || "Claude API error" } }; }
  return { status: 200, body: { text: (j.content || []).filter((b) => b.type === "text").map((b) => b.text).join(""), truncated: j.stop_reason === "max_tokens" } };
}

/* ---------------- routes: auth ---------------- */
async function handleAuth(req, res, p, url, ip) {
  const m = req.method;
  if (p === "/api/auth/status" && m === "GET") {
    const s = await sessionFrom(req); const code = await kvGet("code");
    if (s && !s.locked) await touch(s);
    return send(res, req, 200, { initialised: !!code, hasUsers: await hasUsers(), session: s ? { user: meOf(s.user), locked: !!s.locked, start: s.created_at } : null, policy: await policy(), lockedOut: await lockedFor("ip:" + ip), gate: (await gateOk(req)) ? await publicUsers() : null, ai: !!process.env.ANTHROPIC_API_KEY, transport: "poll" });
  }
  if (m !== "POST") return err(res, req, 405, "invalid_request", "Method not allowed");
  const body = await readJSON(req); const device = String(body.device || "").slice(0, 160);
  if (p === "/api/auth/code") {
    const lk = await lockedFor("ip:" + ip); if (lk) return err(res, req, 429, "locked_out", `Too many attempts. Try again in ${Math.ceil(lk / 60000)} min.`);
    const code = await kvGet("code"); if (!code) return err(res, req, 503, "not_initialised", "No access code has been set.");
    if (!(await verifySecret(String(body.code || ""), code.salt, code.hash, code.iter))) {
      const f = await fail("ip:" + ip); await audit("security", "", "", `Failed access code (attempt ${f.n} of ${f.max})${f.locked ? `. Address locked out for ${f.minutes} min` : ""}`, { ip, device });
      return err(res, req, f.locked ? 429 : 401, f.locked ? "locked_out" : "bad_code", f.locked ? `Too many attempts. Try again in ${f.minutes} min.` : "That code isn't recognised.");
    }
    await clearFails("ip:" + ip);
    const g = crypto.randomBytes(24).toString("base64url"); await q("INSERT INTO gates(token_hash,created_at,ip) VALUES($1,$2,$3)", [sha(g), now(), ip]);
    setCookie(res, "uc_gate", g, GATE_MINUTES * 60);
    return send(res, req, 200, { ok: true, needsSetup: !(await hasUsers()), users: await publicUsers() });
  }
  if (p === "/api/auth/setup") {
    if (!(await gateOk(req))) return err(res, req, 401, "gate", "Enter the access code first.");
    if (await hasUsers()) return err(res, req, 409, "exists", "Accounts already exist. Sign in instead.");
    const admins = (Array.isArray(body.admins) ? body.admins : []).filter((a) => a && String(a.name || "").trim()).slice(0, 3);
    if (!admins.length) return err(res, req, 400, "invalid_request", "Add at least one super admin.");
    for (const a of admins) if (!PIN_OK(String(a.pin || ""))) return err(res, req, 400, "weak_pin", "PINs need 6 to 12 digits and can't be a sequence or one repeated digit.");
    let first = null;
    for (const a of admins) { const h = await hashSecret(a.pin); const id = rid("u_"); await q("INSERT INTO users(id,name,role,pin_salt,pin_hash,iter,active,created_at,created_by,updated_at) VALUES($1,$2,'superadmin',$3,$4,$5,true,$6,'setup',uc_now())", [id, String(a.name).trim().slice(0, 80), h.salt, h.hash, h.iter, now()]); first = first || id; }
    const u = await q1("SELECT * FROM users WHERE id=$1", [first]);
    await audit("security", "users", first, `Workspace initialised with ${admins.length} super admin account${admins.length > 1 ? "s" : ""}`, { user: u, ip, device });
    await q("UPDATE users SET last_login_at=$2, last_login_device=$3, last_login_ip=$4, updated_at=uc_now() WHERE id=$1", [first, now(), device, ip]);
    await createSession(res, req, u, device); await audit("login", "users", first, "Signed in (first entry)", { user: u, ip, device });
    return send(res, req, 200, { user: meOf(u), prevLogin: null, start: now() });
  }
  if (p === "/api/auth/pin") {
    if (!(await gateOk(req))) return err(res, req, 401, "gate", "Enter the access code first.");
    const u = await q1("SELECT * FROM users WHERE id=$1 AND active", [String(body.userId || "")]);
    if (!u) return err(res, req, 404, "no_user", "That account isn't available.");
    const lk = Math.max(await lockedFor("ip:" + ip), await lockedFor("user:" + u.id)); if (lk) return err(res, req, 429, "locked_out", `Too many attempts. Try again in ${Math.ceil(lk / 60000)} min.`);
    if (!(await verifySecret(String(body.pin || ""), u.pin_salt, u.pin_hash, u.iter))) {
      const f = await fail("user:" + u.id); await fail("ip:" + ip);
      await audit("security", "users", u.id, `Failed PIN for ${u.name} (attempt ${f.n} of ${f.max})${f.locked ? `. Account locked for ${f.minutes} min` : ""}`, { ip, device });
      return err(res, req, f.locked ? 429 : 401, f.locked ? "locked_out" : "bad_pin", f.locked ? `Too many attempts. Try again in ${f.minutes} min.` : "Incorrect PIN.");
    }
    await clearFails("user:" + u.id); await clearFails("ip:" + ip);
    const prevLogin = u.last_login_at ? { at: u.last_login_at, dev: [u.last_login_device, u.last_login_ip].filter(Boolean).join(" · ") } : null;
    await q("UPDATE users SET last_login_at=$2, last_login_device=$3, last_login_ip=$4, updated_at=uc_now() WHERE id=$1", [u.id, now(), device, ip]);
    await createSession(res, req, u, device); await audit("login", "users", u.id, "Signed in", { user: u, ip, device });
    return send(res, req, 200, { user: meOf(u), prevLogin, start: now() });
  }
  const s = await sessionFrom(req); if (!s) return err(res, req, 401, "signed_out", "Please sign in.");
  if (p === "/api/auth/unlock") {
    if (await lockedFor("user:" + s.user.id)) return err(res, req, 429, "locked_out", "Too many attempts on this account. Try again later.");
    if (!(await verifySecret(String(body.pin || ""), s.user.pin_salt, s.user.pin_hash, s.user.iter))) {
      const f = await fail("user:" + s.user.id); await audit("security", "users", s.user.id, `Failed PIN at lock screen (attempt ${f.n} of ${f.max})`, { user: s.user, ip, device });
      if (f.locked) { await q("DELETE FROM sessions WHERE token_hash=$1", [s.token_hash]); await audit("logout", "users", s.user.id, "Signed out after repeated failed unlocks", { user: s.user, ip, device }); return err(res, req, 401, "signed_out", "Too many attempts. You've been signed out."); }
      return err(res, req, 401, "bad_pin", "Incorrect PIN.");
    }
    await clearFails("user:" + s.user.id); await q("UPDATE sessions SET locked=false, last_seen=$2 WHERE token_hash=$1", [s.token_hash, now()]);
    await audit("unlock", "users", s.user.id, "Unlocked the screen", { user: s.user, ip, device });
    return send(res, req, 200, { ok: true });
  }
  if (p === "/api/auth/lock") { if (!s.locked) { await q("UPDATE sessions SET locked=true WHERE token_hash=$1", [s.token_hash]); await audit("lock", "users", s.user.id, body.why === "manual" ? "Locked the screen" : "Screen locked after inactivity", { user: s.user, ip, device }); } return send(res, req, 200, { ok: true }); }
  if (p === "/api/auth/logout") { await q("DELETE FROM sessions WHERE token_hash=$1", [s.token_hash]); setCookie(res, "uc_session", "", 0); await audit("logout", "users", s.user.id, String(body.reason || "Signed out").slice(0, 120), { user: s.user, ip, device }); return send(res, req, 200, { ok: true }); }
  if (s.locked) return err(res, req, 423, "locked", "Unlock to continue.");
  if (p === "/api/auth/verify-pin") {
    if (!(await verifySecret(String(body.pin || ""), s.user.pin_salt, s.user.pin_hash, s.user.iter))) { const f = await fail("user:" + s.user.id); await audit("security", "users", s.user.id, `Failed PIN re-confirmation${body.purpose ? " (" + String(body.purpose).slice(0, 60) + ")" : ""}`, { user: s.user, ip, device }); if (f.locked) { await kickUser(s.user.id); return err(res, req, 401, "signed_out", "Too many attempts. You've been signed out."); } return err(res, req, 401, "bad_pin", "That PIN doesn't match."); }
    await clearFails("user:" + s.user.id); return send(res, req, 200, { ok: true });
  }
  if (p === "/api/auth/change-pin") {
    if (!(await verifySecret(String(body.current || ""), s.user.pin_salt, s.user.pin_hash, s.user.iter))) { await fail("user:" + s.user.id); await audit("security", "users", s.user.id, "Failed PIN while changing own PIN", { user: s.user, ip, device }); return err(res, req, 401, "bad_pin", "That PIN doesn't match."); }
    if (!PIN_OK(String(body.next || ""))) return err(res, req, 400, "weak_pin", "Use 6 to 12 digits, not a sequence or one repeated digit.");
    const h = await hashSecret(body.next); await q("UPDATE users SET pin_salt=$2, pin_hash=$3, iter=$4 WHERE id=$1", [s.user.id, h.salt, h.hash, h.iter]);
    await audit("security", "users", s.user.id, "Changed own PIN", { user: s.user, ip, device }); return send(res, req, 200, { ok: true });
  }
  return err(res, req, 404, "not_found", "Not found");
}

/* ---------------- routes: admin ---------------- */
async function handleAdmin(req, res, p, url, ip, s) {
  if (s.user.role !== "superadmin") return err(res, req, 403, "permission_denied", "Super admins only.");
  const body = req.method === "GET" ? {} : await readJSON(req); const device = String(body.device || req.headers["x-device"] || "").slice(0, 160);
  const needPin = async () => { if (await verifySecret(String(body.authPin || ""), s.user.pin_salt, s.user.pin_hash, s.user.iter)) return true; const f = await fail("user:" + s.user.id); await audit("security", "users", s.user.id, "Failed PIN on an admin action", { user: s.user, ip, device }); if (f.locked) await kickUser(s.user.id); return false; };
  if (p === "/api/admin/users" && req.method === "POST") {
    const name = String(body.name || "").trim().slice(0, 80); const role = body.role === "superadmin" ? "superadmin" : "member";
    if (!name) return err(res, req, 400, "invalid_request", "Add a name.");
    if (!PIN_OK(String(body.pin || ""))) return err(res, req, 400, "weak_pin", "PINs need 6 to 12 digits and can't be a sequence or one repeated digit.");
    if (!(await needPin())) return err(res, req, 401, "bad_pin", "Your PIN doesn't match.");
    const h = await hashSecret(body.pin); const id = rid("u_");
    await q("INSERT INTO users(id,name,role,pin_salt,pin_hash,iter,active,created_at,created_by,updated_at) VALUES($1,$2,$3,$4,$5,$6,true,$7,$8,uc_now())", [id, name, role, h.salt, h.hash, h.iter, now(), s.user.id]);
    await audit("security", "users", id, `Added user ${name} (${role === "superadmin" ? "super admin" : "member"})`, { user: s.user, ip, device });
    return send(res, req, 200, { id });
  }
  const um = p.match(/^\/api\/admin\/users\/([A-Za-z0-9_]+)$/);
  if (um && req.method === "PATCH") {
    const u = await q1("SELECT * FROM users WHERE id=$1", [um[1]]); if (!u) return err(res, req, 404, "not_found", "No such user.");
    const admins = (await q1("SELECT COUNT(*)::int AS n FROM users WHERE role='superadmin' AND active")).n;
    if ((body.role !== undefined || body.active !== undefined || body.pin !== undefined) && !(await needPin())) return err(res, req, 401, "bad_pin", "Your PIN doesn't match.");
    if (body.name !== undefined) { const n = String(body.name).trim().slice(0, 80); if (n) { await q("UPDATE users SET name=$2 WHERE id=$1", [u.id, n]); await audit("security", "users", u.id, `Renamed ${u.name} to ${n}`, { user: s.user, ip, device }); } }
    if (body.role !== undefined) {
      const role = body.role === "superadmin" ? "superadmin" : "member";
      if (u.id === s.user.id) return err(res, req, 400, "invalid_request", "You can't change your own role.");
      if (u.role === "superadmin" && role !== "superadmin" && admins <= 1) return err(res, req, 400, "invalid_request", "Keep at least one super admin.");
      await q("UPDATE users SET role=$2 WHERE id=$1", [u.id, role]); await audit("security", "users", u.id, `Changed ${u.name}'s role to ${role === "superadmin" ? "super admin" : "member"}`, { user: s.user, ip, device });
    }
    if (body.active !== undefined) {
      if (u.id === s.user.id) return err(res, req, 400, "invalid_request", "You can't deactivate yourself.");
      if (!body.active && u.role === "superadmin" && admins <= 1) return err(res, req, 400, "invalid_request", "Keep at least one super admin.");
      await q("UPDATE users SET active=$2 WHERE id=$1", [u.id, !!body.active]); await audit("security", "users", u.id, `${body.active ? "Reactivated" : "Deactivated"} ${u.name}`, { user: s.user, ip, device });
      if (!body.active) await kickUser(u.id);
    }
    if (body.pin !== undefined) {
      if (!PIN_OK(String(body.pin))) return err(res, req, 400, "weak_pin", "PINs need 6 to 12 digits and can't be a sequence or one repeated digit.");
      const h = await hashSecret(body.pin); await q("UPDATE users SET pin_salt=$2, pin_hash=$3, iter=$4 WHERE id=$1", [u.id, h.salt, h.hash, h.iter]); await clearFails("user:" + u.id);
      await audit("security", "users", u.id, `Reset PIN for ${u.name}`, { user: s.user, ip, device }); if (u.id !== s.user.id) await kickUser(u.id);
    }
    await touchUser(u.id); return send(res, req, 200, { ok: true });
  }
  if (p === "/api/admin/code" && req.method === "POST") {
    if (!CODE_OK(body.code)) return err(res, req, 400, "weak_code", "Use at least 8 characters, mixing letters and numbers.");
    if (!(await needPin())) return err(res, req, 401, "bad_pin", "Your PIN doesn't match.");
    const h = await hashSecret(body.code); await kvSet("code", { ...h, rotatedAt: now(), rotatedBy: s.user.id }); await q("DELETE FROM gates");
    await audit("security", "settings", "security", "Changed the access code", { user: s.user, ip, device });
    return send(res, req, 200, { ok: true });
  }
  if (p === "/api/admin/policy" && req.method === "POST") {
    const c = (v, a, b, d) => Math.max(a, Math.min(b, Number(v) || d));
    const pol = { idleMinutes: c(body.idleMinutes, 2, 240, 15), maxSessionHours: c(body.maxSessionHours, 1, 72, 12), maxAttempts: c(body.maxAttempts, 3, 20, 5), lockMinutes: c(body.lockMinutes, 1, 1440, 15) };
    await kvSet("policy", pol); await audit("security", "settings", "security", `Updated session policies: lock after ${pol.idleMinutes} min, max session ${pol.maxSessionHours} h, lockout after ${pol.maxAttempts} attempts for ${pol.lockMinutes} min`, { user: s.user, ip, device });
    return send(res, req, 200, { ok: true });
  }
  if (p === "/api/admin/audit" && req.method === "GET") {
    const events = (await q("SELECT t,u,n,a,e,i,s,d FROM audit ORDER BY t DESC LIMIT 100000"));
    await audit("export", "", "", `Exported the full access log (${events.length} events)`, { user: s.user, ip, device });
    return send(res, req, 200, { events });
  }
  if (p === "/api/admin/sessions" && req.method === "GET") return send(res, req, 200, { sessions: await q("SELECT s.user_id, u.name, s.created_at, s.last_seen, s.locked, s.ip, s.device FROM sessions s JOIN users u ON u.id=s.user_id ORDER BY s.last_seen DESC") });
  if (p === "/api/admin/sessions/revoke" && req.method === "POST") {
    if (!(await needPin())) return err(res, req, 401, "bad_pin", "Your PIN doesn't match.");
    const u = await q1("SELECT * FROM users WHERE id=$1", [String(body.userId || "")]); if (!u) return err(res, req, 404, "not_found", "No such user.");
    await kickUser(u.id); await audit("security", "users", u.id, `Signed ${u.name} out of every device`, { user: s.user, ip, device });
    return send(res, req, 200, { ok: true });
  }
  return err(res, req, 404, "not_found", "Not found");
}

/* ---------------- routes: data ---------------- */
const MIME = { ".pdf": "application/pdf", ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp", ".gif": "image/gif", ".csv": "text/csv", ".txt": "text/plain", ".md": "text/markdown", ".json": "application/json", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation", ".doc": "application/msword", ".xls": "application/vnd.ms-excel", ".ppt": "application/vnd.ms-powerpoint", ".zip": "application/zip" };
const extOf = (n) => { const m = /\.[A-Za-z0-9]+$/.exec(n || ""); return m ? m[0].toLowerCase() : ""; };
const aiUse = new Map();

async function changesSince(since, s) {
  const docs = await q("SELECT path, data FROM docs WHERE updated_at > $1 AND coll <> 'audit'", [since]);
  const out = docs.map((r) => ({ p: r.path, d: r.data }));
  for (const u of await q("SELECT * FROM users WHERE updated_at > $1", [since])) out.push({ p: "users/" + u.id, d: userDoc(u) });
  const sec = await q("SELECT 1 AS x FROM kv WHERE k IN ('policy','code') AND updated_at > $1", [since]);
  if (sec.length) out.push({ p: "settings/security", d: await securityDoc() });
  const days = (await q("SELECT DISTINCT day FROM audit WHERE t > $1", [since])).map((r) => r.day);
  out.push(...(await auditDayDocs(days)));
  return out;
}
async function handleData(req, res, p, url, ip, s) {
  const m = req.method;
  if (p === "/api/sync" && m === "GET") {
    const cursor = (await dbNow()) - OVERLAP;
    const docs = (await q("SELECT path, data FROM docs WHERE data IS NOT NULL AND coll <> 'audit'")).map((r) => ({ p: r.path, d: r.data }));
    for (const u of await q("SELECT * FROM users")) docs.push({ p: "users/" + u.id, d: userDoc(u) });
    docs.push({ p: "settings/security", d: await securityDoc() });
    const days = (await q("SELECT DISTINCT day FROM audit WHERE day >= $1", [dayKey(now() - 90 * 86400000)])).map((r) => r.day);
    docs.push(...(await auditDayDocs(days)));
    return send(res, req, 200, { cursor, docs, me: meOf(s.user) });
  }
  if (p === "/api/poll" && m === "GET") {
    const since = Number(url.searchParams.get("since")) || 0; const view = String(url.searchParams.get("view") || "").slice(0, 40);
    const t = await dbNow();
    await q("UPDATE sessions SET last_poll=$2, view=$3 WHERE token_hash=$1", [s.token_hash, t, view]);
    const peers = await q("SELECT s.user_id AS uid, u.name, s.view, s.created_at AS since FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.last_poll > $1 AND NOT s.locked", [t - 15000]);
    return send(res, req, 200, { cursor: t - OVERLAP, docs: since ? await changesSince(since, s) : [], peers });
  }
  if (p === "/api/ping" && m === "POST") return send(res, req, 200, { ok: true });
  if (p === "/api/presence" && m === "POST") { const b = await readJSON(req); await q("UPDATE sessions SET view=$2 WHERE token_hash=$1", [s.token_hash, String(b.view || "").slice(0, 40)]); return send(res, req, 200, { ok: true }); }
  if (p === "/api/audit" && m === "POST") {
    const b = await readJSON(req); const list = Array.isArray(b.events) ? b.events.slice(0, 50) : [b];
    const ALLOWED = new Set(["create", "update", "delete", "erase", "import", "export", "view", "ai"]);
    for (const e of list) if (e && ALLOWED.has(e.a)) await audit(e.a, String(e.e || "").slice(0, 40), String(e.i || "").slice(0, 80), e.s, { user: s.user, ip, device: String(e.d || "").slice(0, 160) });
    return send(res, req, 200, { ok: true });
  }
  if (p === "/api/doc") {
    if (m === "GET") { const dp = url.searchParams.get("path"); if (!validPath(dp)) return err(res, req, 400, "invalid_argument", "Bad path"); const d = dp === "settings/security" ? await securityDoc() : await getDoc(dp); return send(res, req, 200, { exists: !!d, data: d }); }
    if (m === "DELETE") {
      const dp = url.searchParams.get("path"); if (!validPath(dp)) return err(res, req, 400, "invalid_argument", "Bad path");
      if (!canWrite(s, dp, true)) return err(res, req, 403, "permission_denied", "You don't have permission to delete that.");
      await setDoc(dp, null); return send(res, req, 200, { ok: true });
    }
    const b = await readJSON(req);
    if (!validPath(b.path)) return err(res, req, 400, "invalid_argument", "Bad path");
    if (!isObj(b.data)) return err(res, req, 400, "invalid_argument", "Body must be an object");
    if (!canWrite(s, b.path, false)) return err(res, req, 403, "permission_denied", "You don't have permission to change that.");
    if (m === "PUT") { await setDoc(b.path, b.data); return send(res, req, 200, { ok: true }); }
    if (m === "PATCH") { if (!(await mergeDoc(b.path, b.data))) return err(res, req, 400, "invalid_argument", "Document does not exist"); return send(res, req, 200, { ok: true }); }
  }
  if (p === "/api/ai" && m === "POST") {
    const b = await readJSON(req);
    const hist = (aiUse.get(s.user.id) || []).filter((t) => now() - t < 10 * 60000); if (hist.length >= 40) return err(res, req, 429, "rate_limited", "Too many AI requests. Wait a few minutes.");
    hist.push(now()); aiUse.set(s.user.id, hist);
    const ctl = new AbortController(); req.on("close", () => { if (!res.writableEnded) ctl.abort(); });
    try { const r = await callClaude({ input: b.input, tier: b.tier, json: !!b.json, images: b.images }, ctl.signal); return send(res, req, r.status, r.body); }
    catch (e) { if (ctl.signal.aborted) return; console.error(e); return err(res, req, 502, "upstream_error", "Couldn't reach Claude."); }
  }
  /* Files are stored privately in Postgres in 3 MB chunks (Vercel limits each request to 4.5 MB). */
  if (p === "/api/files" && m === "POST") {
    const b = await readJSON(req); const name = String(b.name || "file").replace(/[\\/\r\n"]/g, "_").slice(0, 180); const type = MIME[extOf(name)]; const size = Number(b.size) || 0;
    if (!type) return err(res, req, 415, "unsupported_type", "That file type isn't accepted.");
    if (!size || size > MAX_FILE) return err(res, req, 413, "too_large", "Files must be 25 MB or smaller.");
    const id = rid("f_"); await q("INSERT INTO files(id,name,type,size,chunks,complete,created_at,created_by) VALUES($1,$2,$3,$4,$5,false,$6,$7)", [id, name, type, size, Math.ceil(size / CHUNK), now(), s.user.id]);
    return send(res, req, 200, { id, chunkSize: CHUNK, chunks: Math.ceil(size / CHUNK), contentType: type });
  }
  const fm = p.match(/^\/api\/files\/(f_[a-z0-9]+)(?:\/(\d+|complete))?$/);
  if (fm) {
    const f = await q1("SELECT * FROM files WHERE id=$1", [fm[1]]); if (!f) return err(res, req, 404, "not_found", "File not found.");
    if (fm[2] === "complete" && m === "POST") {
      const n = (await q1("SELECT COUNT(*)::int AS n FROM file_chunks WHERE file_id=$1", [f.id])).n;
      if (n !== f.chunks) return err(res, req, 400, "invalid_argument", "Upload incomplete.");
      await q("UPDATE files SET complete=true WHERE id=$1", [f.id]); return send(res, req, 200, { id: f.id, url: "/_blob/" + f.id, sizeBytes: f.size, contentType: f.type });
    }
    if (fm[2] !== undefined && m === "PUT") {
      if (f.complete || f.created_by !== s.user.id) return err(res, req, 403, "permission_denied", "Upload not allowed.");
      const n = Number(fm[2]); if (!(n >= 0 && n < f.chunks)) return err(res, req, 400, "invalid_argument", "Bad chunk.");
      const buf = await readBody(req, CHUNK + 1024);
      await q("INSERT INTO file_chunks(file_id,n,data) VALUES($1,$2,$3) ON CONFLICT(file_id,n) DO UPDATE SET data=EXCLUDED.data", [f.id, n, buf]);
      return send(res, req, 200, { ok: true });
    }
    if (fm[2] !== undefined && m === "GET") {
      if (!f.complete) return err(res, req, 404, "not_found", "File not ready.");
      const r = await q1("SELECT data FROM file_chunks WHERE file_id=$1 AND n=$2", [f.id, Number(fm[2])]); if (!r) return err(res, req, 404, "not_found", "Chunk missing.");
      return send(res, req, 200, Buffer.from(r.data), { "Content-Type": "application/octet-stream", "Cache-Control": "private, no-store" });
    }
    if (fm[2] === undefined && m === "GET") return send(res, req, 200, { id: f.id, name: f.name, type: f.type, size: f.size, chunks: f.chunks, complete: f.complete });
    if (fm[2] === undefined && m === "DELETE") {
      if (s.user.role !== "superadmin") return err(res, req, 403, "permission_denied", "Super admins only.");
      await q("DELETE FROM file_chunks WHERE file_id=$1", [f.id]); await q("DELETE FROM files WHERE id=$1", [f.id]); return send(res, req, 200, { deleted: true });
    }
  }
  return err(res, req, 404, "not_found", "Not found");
}

/* ---------------- first start ---------------- */
async function firstStart() {
  if (!(await kvGet("code")) && process.env.ACCESS_CODE) {
    const h = await hashSecret(process.env.ACCESS_CODE); await kvSet("code", { ...h, rotatedAt: now(), rotatedBy: "setup" });
    await audit("security", "settings", "security", "Access code set from server configuration");
  }
  if (!(await kvGet("seeded")) && process.env.SEED_EXAMPLES !== "false") {
    await kvSet("seeded", true);
    const has = await q1("SELECT 1 AS x FROM docs WHERE coll IN ('contacts','deals') LIMIT 1");
    if (!has) {
      let seed = {}; try { seed = require("../seed/examples.json"); } catch {}
      const delta = seed._generatedAt ? now() - seed._generatedAt : 0;
      const shift = (v) => { if (typeof v === "number" && v > 1.6e12 && v < 2.5e12) return v + delta; if (typeof v === "string" && /^\d{4}-\d{2}-\d{2}$/.test(v)) return new Date(Date.parse(v + "T12:00:00Z") + delta).toISOString().slice(0, 10); if (Array.isArray(v)) return v.map(shift); if (isObj(v)) return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, shift(x)])); return v; };
      for (const [p, d] of Object.entries(seed)) if (p !== "_generatedAt" && validPath(p) && p !== "settings/security") await setDoc(p, shift(d));
    }
  }
  await q("DELETE FROM gates WHERE created_at < $1", [now() - GATE_MINUTES * 60000]);
  await q("DELETE FROM sessions WHERE created_at < $1", [now() - 72 * 3600000]);
  await q("DELETE FROM files WHERE NOT complete AND created_at < $1", [now() - 86400000]);
}

/* ---------------- entry point ---------------- */
module.exports = async (req, res) => {
  const url = new URL(req.url, "http://x");
  let p = url.pathname;
  if (p === "/api/index" || p === "/api/index.js") p = "/api/" + (url.searchParams.get("__p") || "");
  const ip = clientIp(req);
  res.setHeader("X-Robots-Tag", "noindex, nofollow");
  try {
    if (p === "/api/healthz") {
      const out = { ok: !!pool, database: pool ? "connected" : "missing", ai: !!(process.env.ANTHROPIC_API_KEY || "").trim(), region: process.env.VERCEL_REGION || null };
      if (!pool) out.fix = "Open this project in Vercel, go to Storage, create or connect a Neon Postgres database, then redeploy the latest deployment.";
      else { try { await init(); out.initialised = !!(await kvGet("code")); if (!out.initialised) out.fix = "Add ACCESS_CODE in Settings, Environment Variables, then redeploy."; } catch (e) { out.ok = false; out.database = "error"; out.detail = String(e && e.message || e).slice(0, 200); } }
      return send(res, req, 200, out);
    }
    if (!pool) return err(res, req, 503, "not_configured", "No database is connected. In Vercel open this project, go to Storage, create a Neon Postgres database and connect it, then redeploy.");
    await init();
    if (req.method !== "GET" && req.headers["x-uc"] !== "1") return err(res, req, 403, "csrf", "Request blocked.");
    if (p.startsWith("/api/auth/")) return await handleAuth(req, res, p, url, ip);
    const s = await sessionFrom(req);
    if (!s) return err(res, req, 401, "signed_out", "Please sign in.");
    if (s.locked) return err(res, req, 423, "locked", "Unlock to continue.");
    if (p !== "/api/poll" && p !== "/api/sync") await touch(s);
    if (p.startsWith("/api/admin/")) return await handleAdmin(req, res, p, url, ip, s);
    return await handleData(req, res, p, url, ip, s);
  } catch (e) {
    if (e && e.status) return err(res, req, e.status, e.status === 413 ? "too_large" : "invalid_request", e.message);
    console.error(e);
    const msg = String((e && e.message) || "");
    if (/ENOTFOUND|ECONNREFUSED|ETIMEDOUT|connection refused|could not connect|connection to server|password authentication|role .* does not exist|database .* does not exist|SSL|self.signed|terminating connection|Connection terminated/i.test(msg))
      return err(res, req, 503, "db_unreachable", "The database can't be reached. Check the Neon database is connected and running in Vercel, then redeploy.");
    if (!res.headersSent) err(res, req, 500, "server_error", "Something went wrong.");
  }
};
module.exports.config = { api: { bodyParser: false } };
