/* =========================================================
   Standalone adapter: gives the app the same db / room / sample /
   assets / downloads interfaces it uses inside Claude, backed by the
   UConnect server (REST + server-sent events).
   ========================================================= */
(function () {
  window.UC_STANDALONE = true;
  const DEVICE = () => (typeof deviceLabel === "function" ? deviceLabel() : "");
  const codeFor = (status) => ({ 400: "invalid_argument", 401: "signed_out", 403: "permission_denied", 404: "not_found", 409: "conflict", 413: "too_large", 415: "unsupported_type", 423: "locked", 429: "rate_limited", 503: "unavailable" }[status] || "unavailable");
  async function api(method, url, body, opts = {}) {
    const isBlob = typeof Blob !== "undefined" && body instanceof Blob;
    const headers = { "X-UC": "1", ...(body !== undefined && !isBlob ? { "Content-Type": "application/json" } : {}), ...(opts.headers || {}) };
    let r;
    try { r = await fetch(url, { method, headers, credentials: "same-origin", signal: opts.signal, body: body === undefined ? undefined : isBlob ? body : JSON.stringify(body) }); }
    catch (e) { if (e && e.name === "AbortError") throw { code: "cancelled", message: "Stopped." }; throw { code: "unavailable", message: "Can't reach the UConnect server." }; }
    let j = null; try { j = await r.json(); } catch {}
    if (!r.ok) {
      const e = { code: (j && j.code) || codeFor(r.status), message: (j && j.error) || r.statusText, status: r.status, data: j };
      if ((r.status === 401 && e.code === "signed_out") || r.status === 423) window.dispatchEvent(new CustomEvent("uc-auth", { detail: { state: r.status === 423 ? "locked" : "out" } }));
      throw e;
    }
    return j;
  }
  window.UCAPI = api;

  /* ---------- local mirror of the server's documents ---------- */
  const store = new Map();
  const listeners = new Set();
  let ready = false, seq = 0, es = null, peers = [], stopped = true;
  const peerHandlers = new Set();
  const clone = (o) => (o === undefined ? undefined : JSON.parse(JSON.stringify(o)));
  const isObj = (v) => v && typeof v === "object" && !Array.isArray(v);
  const merge = (a, b) => { const o = { ...a }; for (const [k, v] of Object.entries(b)) o[k] = isObj(v) && isObj(o[k]) ? merge(o[k], v) : v; return o; };
  let notifyQueued = false;
  const notify = () => { if (notifyQueued) return; notifyQueued = true; setTimeout(() => { notifyQueued = false; listeners.forEach((l) => { try { l(); } catch (e) { console.warn(e); } }); }, 0); };
  const snapDoc = (p) => { const d = store.get(p); return { id: p.split("/").pop(), exists: !!d, data: () => (d ? clone(d) : undefined), metadata: { fromCache: false, hasPendingWrites: false } }; };
  function apply(p, d) { if (d === null || d === undefined) store.delete(p); else store.set(p, d); }

  function docRef(p) {
    return {
      id: p.split("/").pop(), path: p,
      get: async () => { if (ready && store.has(p)) return snapDoc(p); try { const r = await api("GET", "/api/doc?path=" + encodeURIComponent(p)); if (r.exists) store.set(p, r.data); } catch {} return snapDoc(p); },
      set: async (data) => { const prev = store.get(p); apply(p, clone(data)); notify(); try { await api("PUT", "/api/doc", { path: p, data }); soon(); } catch (e) { apply(p, prev); notify(); throw e; } },
      update: async (data) => { const prev = store.get(p); if (prev) { apply(p, merge(prev, clone(data))); notify(); } try { await api("PATCH", "/api/doc", { path: p, data }); soon(); } catch (e) { apply(p, prev); notify(); throw e; } },
      delete: async () => { const prev = store.get(p); apply(p, null); notify(); try { await api("DELETE", "/api/doc?path=" + encodeURIComponent(p)); soon(); } catch (e) { apply(p, prev); notify(); throw e; } },
      acquire: async () => ({ acquired: true }),
      onSnapshot: (next) => { const f = () => { if (ready) next(snapDoc(p)); }; listeners.add(f); f(); return () => listeners.delete(f); },
      collection: (c) => collRef(p + "/" + c),
    };
  }
  function query(path, opts = {}) {
    const depth = path.split("/").length + 1;
    const run = () => {
      let docs = [...store.keys()].filter((k) => k.startsWith(path + "/") && k.split("/").length === depth).map(snapDoc);
      for (const [f, op, v] of opts.where || []) docs = docs.filter((d) => { const x = d.data()[f]; return op === "==" ? x === v : op === "!=" ? x !== v : op === ">" ? x > v : op === ">=" ? x >= v : op === "<" ? x < v : op === "<=" ? x <= v : op === "in" ? v.includes(x) : op === "array-contains" ? Array.isArray(x) && x.includes(v) : true; });
      if (opts.order) { const [f, dir] = opts.order; docs.sort((a, b) => { const x = a.data()[f], y = b.data()[f]; return (x > y ? 1 : x < y ? -1 : 0) * (dir === "desc" ? -1 : 1); }); }
      if (opts.limit) docs = docs.slice(0, opts.limit);
      return { docs, size: docs.length, empty: !docs.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: false } };
    };
    return {
      where: (f, op, v) => query(path, { ...opts, where: [...(opts.where || []), [f, op, v]] }),
      orderBy: (f, dir) => query(path, { ...opts, order: [f, dir] }),
      limit: (n) => query(path, { ...opts, limit: n }),
      get: async () => run(),
      onSnapshot: (next) => { const f = () => { if (ready) next(run()); }; listeners.add(f); f(); return () => listeners.delete(f); },
    };
  }
  function collRef(path) { return Object.assign(query(path), { path, doc: (id) => docRef(path + "/" + (id || Date.now().toString(36) + Math.random().toString(36).slice(2, 8))), add: async (d) => { const r = docRef(path + "/" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8)); await r.set(d); return r; } }); }
  const db = Object.freeze({ doc: docRef, collection: collRef });

  /* ---------- sync: server-sent events (own server) or polling (Vercel) ---------- */
  const POLL = () => window.UC_TRANSPORT === "poll";
  let cursor = 0, pollTimer = null, polling = false, view = "overview", peersSig = "";
  function emitPeers(list) {
    const sig = JSON.stringify(list || []); if (sig === peersSig) return; peersSig = sig; peers = list || [];
    const out = peers.map((p, i) => ({ peer: "p" + i, by: null, isMe: false, sameTab: false, kind: "viewer", presence: p, updatedAt: Date.now() }));
    peerHandlers.forEach((h) => { try { h({ peers: out, joined: [], left: [], updated: [] }); } catch {} });
  }
  function schedule(delay) { clearTimeout(pollTimer); if (stopped || !POLL()) return; pollTimer = setTimeout(pollOnce, delay ?? (document.hidden ? 20000 : 3000)); }
  function soon() { if (POLL()) schedule(400); }
  async function pollOnce() {
    if (stopped || polling) return; polling = true;
    try {
      const r = await api("GET", "/api/poll?since=" + cursor + "&view=" + encodeURIComponent(view));
      cursor = r.cursor || cursor; let changed = false;
      for (const d of r.docs || []) { const prev = store.get(d.p); if (JSON.stringify(prev ?? null) !== JSON.stringify(d.d ?? null)) { apply(d.p, d.d); changed = true; } }
      if (changed) notify();
      emitPeers(r.peers);
    } catch (e) { /* sign-out and lock are dispatched by api(); other errors just retry */ }
    finally { polling = false; schedule(); }
  }
  document.addEventListener("visibilitychange", () => { if (!document.hidden) schedule(200); });
  async function pull(since) {
    if (POLL()) {
      const r = await api("GET", "/api/sync");
      store.clear(); for (const d of r.docs) apply(d.p, d.d);
      cursor = r.cursor || 0; ready = true; notify(); return;
    }
    const r = await api("GET", "/api/sync?since=" + (since || 0));
    if (!since) store.clear();
    for (const d of r.docs) apply(d.p, d.d);
    seq = Math.max(seq, r.seq || 0); ready = true; notify();
  }
  function openStream() {
    if (POLL()) { schedule(100); return; }
    if (es) { try { es.close(); } catch {} }
    es = new EventSource("/api/stream");
    let first = true;
    es.onopen = () => { if (!first) pull(seq).catch(() => {}); first = false; };
    es.onmessage = (ev) => {
      let m; try { m = JSON.parse(ev.data); } catch { return; }
      if (m.t === "doc") { apply(m.p, m.d); if (m.s) seq = Math.max(seq, m.s); notify(); }
      else if (m.t === "peers") { peers = m.peers || []; const list = peers.map((p, i) => ({ peer: "p" + i, by: null, isMe: false, sameTab: false, kind: "viewer", presence: p, updatedAt: Date.now() })); peerHandlers.forEach((h) => { try { h({ peers: list, joined: [], left: [], updated: [] }); } catch {} }); }
      else if (m.t === "auth") { try { es.close(); } catch {} es = null; window.dispatchEvent(new CustomEvent("uc-auth", { detail: { state: m.state } })); }
    };
    es.onerror = () => {
      // A refused connection (signed out or locked) closes the stream for good; check why.
      if (es && es.readyState === 2 && !stopped) { es = null; setTimeout(() => { if (!stopped) api("GET", "/api/auth/status").then((st) => { if (!st.session) window.dispatchEvent(new CustomEvent("uc-auth", { detail: { state: "out" } })); else if (st.session.locked) window.dispatchEvent(new CustomEvent("uc-auth", { detail: { state: "locked" } })); else window.UC_SYNC.resume(); }).catch(() => setTimeout(() => !stopped && window.UC_SYNC.resume(), 5000)); }, 1500); }
    };
  }
  window.UC_SYNC = {
    start: async () => { stopped = false; await pull(0); openStream(); },
    resume: async () => { stopped = false; if (!POLL()) { try { await pull(seq); } catch {} } openStream(); },
    pause: () => { stopped = true; clearTimeout(pollTimer); if (es) { try { es.close(); } catch {} es = null; } },
    stop: () => { stopped = true; clearTimeout(pollTimer); if (es) { try { es.close(); } catch {} es = null; } ready = false; seq = 0; cursor = 0; peersSig = ""; store.clear(); peers = []; listeners.forEach((l) => { try { l(); } catch {} }); },
  };

  /* ---------- presence ---------- */
  const room = Object.freeze({
    onPeers: (h) => { peerHandlers.add(h); return () => peerHandlers.delete(h); },
    presence: async (patch) => { if (!patch || !patch.view) return; view = patch.view; if (POLL()) { schedule(150); return; } return api("POST", "/api/presence", { view: patch.view }).catch(() => {}); },
    emit: async () => {}, on: () => () => {}, peers: () => [], connected: () => !!es, onConnection: (h) => { h(!!es); return () => {}; },
  });

  /* ---------- AI ---------- */
  function parseJSON(text) {
    const t = String(text || "").trim();
    try { return JSON.parse(t); } catch {}
    const f = t.match(/```(?:json)?\s*([\s\S]*?)```/); if (f) { try { return JSON.parse(f[1]); } catch {} }
    const a = Math.min(...["{", "["].map((c) => (t.indexOf(c) < 0 ? Infinity : t.indexOf(c)))); const b = Math.max(t.lastIndexOf("}"), t.lastIndexOf("]"));
    if (a < Infinity && b > a) { try { return JSON.parse(t.slice(a, b + 1)); } catch {} }
    throw { code: "invalid_json", message: "Claude's answer couldn't be read.", text: t };
  }
  async function ask(input, opts = {}, json = false) {
    let images;
    if (opts.images) { images = []; for (const b of Array.from(opts.images.length !== undefined && !(opts.images instanceof Blob) ? opts.images : [opts.images]).slice(0, 8)) { const buf = new Uint8Array(await b.arrayBuffer()); let bin = ""; for (let i = 0; i < buf.length; i += 0x8000) bin += String.fromCharCode.apply(null, buf.subarray(i, i + 0x8000)); images.push({ type: b.type || "image/jpeg", data: btoa(bin) }); } }
    const r = await api("POST", "/api/ai", { input, tier: opts.modelTier || "default", json, images }, { signal: opts.signal });
    if (opts.onText && r.text) { try { opts.onText({ text: r.text, delta: r.text }); } catch {} }
    return r;
  }
  const sample = Object.assign(async (input, opts) => { const r = await ask(input, opts, false); return { text: r.text, truncated: !!r.truncated }; }, {
    json: async (input, opts) => { const r = await ask(input, opts, true); if (r.truncated) throw { code: "invalid_json", message: "Answer was cut short.", text: r.text }; return parseJSON(r.text); },
    limits: async () => ({ maxInputBytes: 200000, images: { maxCount: 8, maxBytes: 5000000, mediaTypes: ["image/jpeg", "image/png", "image/webp"] } }),
  });

  /* ---------- files ---------- */
  const assets = Object.freeze({
    upload: async (blob) => {
      if (!POLL()) return api("POST", "/api/files", blob, { headers: { "Content-Type": blob.type || "application/octet-stream", "X-Filename": encodeURIComponent(blob.name || "file") } });
      const init = await api("POST", "/api/files", { name: blob.name || "file", size: blob.size });
      for (let i = 0; i < init.chunks; i++) await api("PUT", `/api/files/${init.id}/${i}`, blob.slice(i * init.chunkSize, (i + 1) * init.chunkSize), { headers: { "Content-Type": "application/octet-stream" } });
      return api("POST", `/api/files/${init.id}/complete`, {});
    },
    delete: async (id) => api("DELETE", "/api/files/" + String(id).replace(/^\/_blob\//, "")),
    list: async () => ({ assets: [], usage: {} }),
  });

  /* ---------- downloads ---------- */
  const downloads = Object.freeze({
    save: async ({ filename, data }) => {
      const blob = data instanceof Blob ? data : new Blob([data], { type: /\.json$/.test(filename) ? "application/json" : /\.csv$/.test(filename) ? "text/csv" : "application/octet-stream" });
      const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = filename; document.body.appendChild(a); a.click();
      setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1500);
      return { saved: true };
    },
  });

  /* Vault files on Vercel are fetched in pieces and reassembled in the browser. */
  const INLINE = /^(application\/pdf|image\/|text\/)/;
  async function fetchFile(id) {
    const meta = await api("GET", "/api/files/" + id); const parts = [];
    for (let i = 0; i < meta.chunks; i++) { const r = await fetch(`/api/files/${id}/${i}`, { credentials: "same-origin" }); if (!r.ok) throw new Error("chunk"); parts.push(await r.arrayBuffer()); }
    return { meta, blob: new Blob(parts, { type: meta.type }) };
  }
  document.addEventListener("click", async (e) => {
    if (!POLL()) return;
    const a = e.target.closest && e.target.closest('a[href^="/_blob/"]'); if (!a) return;
    e.preventDefault();
    const id = a.getAttribute("href").split("/").pop();
    const w = window.open("", "_blank");
    if (w) { try { w.document.title = "Opening…"; w.document.body.style.cssText = "background:#0D0C0B;color:#B9B0A2;font:14px system-ui;display:grid;place-items:center;height:100vh;margin:0"; w.document.body.textContent = "Opening document…"; } catch {} }
    try {
      const { meta, blob } = await fetchFile(id); const url = URL.createObjectURL(blob);
      if (w && INLINE.test(meta.type)) w.location.href = url;
      else { if (w) w.close(); const d = document.createElement("a"); d.href = url; d.download = meta.name; document.body.appendChild(d); d.click(); d.remove(); }
      setTimeout(() => URL.revokeObjectURL(url), 120000);
    } catch { if (w) w.close(); if (typeof toast === "function") toast("That document couldn't be opened.", true); }
  }, true);
  window.UC_FETCH_FILE = fetchFile;

  window.claude = { use: async (n) => ({ db, room, sample: window.UC_AI ? sample : null, assets, downloads }[n] || null) };
  window.UC_DEVICE = DEVICE;
})();
"use strict";
/* =========================================================
   UConnect · core utilities & reference data
   ========================================================= */
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const uid = (p = "") => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
const now = () => Date.now();
const DAY = 86400000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const clamp = (n, a, b) => Math.max(a, Math.min(b, n));
const uniq = (a) => Array.from(new Set((a || []).filter((x) => x !== undefined && x !== null && x !== "")));
const arr = (v) => (Array.isArray(v) ? v : v ? [v] : []);
const debounce = (fn, ms) => { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; };
const norm = (s) => String(s || "").toLowerCase().normalize("NFKD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9@.+ ]/g, " ").replace(/\s+/g, " ").trim();
const initials = (name) => String(name || "?").split(/\s+/).filter(Boolean).slice(0, 2).map((w) => w[0].toUpperCase()).join("") || "?";
const todayISO = () => new Date(Date.now() - new Date().getTimezoneOffset() * 60000).toISOString().slice(0, 10);
const dayKey = (t = Date.now()) => new Date(t - new Date(t).getTimezoneOffset() * 60000).toISOString().slice(0, 10);
const plural = (n, w, p) => `${n.toLocaleString("en-GB")} ${n === 1 ? w : p || w + "s"}`;

function fmtDate(v, opts) {
  if (!v) return "";
  const d = typeof v === "number" ? new Date(v) : new Date(String(v).length === 10 ? v + "T12:00:00" : v);
  if (isNaN(d)) return String(v);
  return d.toLocaleDateString("en-GB", opts || { day: "numeric", month: "short", year: "numeric" });
}
function fmtTime(t) { return new Date(t).toLocaleString("en-GB", { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" }); }
function toTime(v) { if (!v) return 0; if (typeof v === "number") return v; const d = new Date(String(v).length === 10 ? v + "T12:00:00" : v); return isNaN(d) ? 0 : d.getTime(); }
function ago(v) {
  const t = toTime(v); if (!t) return "never";
  const s = (Date.now() - t) / 1000;
  if (s < 0) { const d = Math.ceil(-s / 86400); return d <= 1 ? "tomorrow" : `in ${d} days`; }
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  const d = Math.floor(s / 86400);
  if (d < 31) return `${d}d ago`;
  if (d < 365) return `${Math.floor(d / 30)}mo ago`;
  return `${(d / 365).toFixed(1)}y ago`;
}
function daysSince(v) { const t = toTime(v); return t ? Math.floor((Date.now() - t) / DAY) : Infinity; }

/* ---------- Money ---------- */
const CURRENCIES = ["GBP", "USD", "EUR", "CHF", "AED", "SAR", "QAR", "KWD", "SGD", "HKD", "JPY", "INR", "CAD", "AUD"];
const CUR_SYM = { GBP: "£", USD: "$", EUR: "€", CHF: "CHF ", AED: "AED ", SAR: "SAR ", QAR: "QAR ", KWD: "KWD ", SGD: "S$", HKD: "HK$", JPY: "¥", INR: "₹", CAD: "C$", AUD: "A$" };
const DEFAULT_FX = { GBP: 1, USD: 0.74, EUR: 0.85, CHF: 0.92, AED: 0.2, SAR: 0.197, QAR: 0.203, KWD: 2.42, SGD: 0.575, HKD: 0.095, JPY: 0.005, INR: 0.0086, CAD: 0.54, AUD: 0.49 };
function toGBP(amount, cur) { const a = Number(amount) || 0; const fx = (S.settings.fx || DEFAULT_FX)[cur || "GBP"] ?? DEFAULT_FX[cur] ?? 1; return a * fx; }
function money(n, cur = "GBP", { compact = true, dp } = {}) {
  n = Number(n) || 0; const sym = CUR_SYM[cur] ?? cur + " ";
  if (!compact) return sym + Math.round(n).toLocaleString("en-GB");
  const abs = Math.abs(n);
  const f = (v, s) => sym + (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(dp ?? 1).replace(/\.0$/, "") : v.toFixed(dp ?? 2).replace(/\.?0+$/, "")) + s;
  if (abs >= 1e9) return f(n / 1e9, "bn");
  if (abs >= 1e6) return f(n / 1e6, "m");
  if (abs >= 1e3) return f(n / 1e3, "k");
  return sym + Math.round(n).toLocaleString("en-GB");
}
function ticketText(c) {
  const cur = c.ticketCurrency || "GBP";
  if (c.ticketMin && c.ticketMax) return `${money(c.ticketMin, cur)}–${money(c.ticketMax, cur).replace(CUR_SYM[cur] || "", "")}`;
  if (c.ticketMin) return `${money(c.ticketMin, cur)}+`;
  if (c.ticketMax) return `up to ${money(c.ticketMax, cur)}`;
  return "";
}
/** Parse "£2m-£10m", "$5-25M", "5,000,000", "500k" → {min,max,cur} */
function parseMoneyRange(txt) {
  if (txt === undefined || txt === null || txt === "") return {};
  const s = String(txt).replace(/,/g, "").trim();
  let cur;
  for (const [c, sym] of Object.entries(CUR_SYM)) if (s.toUpperCase().includes(c) || (sym.trim().length === 1 && s.includes(sym.trim()))) { cur = c; break; }
  if (!cur && s.includes("$")) cur = "USD";
  const nums = [...s.matchAll(/(\d+(?:\.\d+)?)\s*(bn|b|m|mm|k|million|billion|thousand)?/gi)].map((m) => {
    let v = parseFloat(m[1]); const u = (m[2] || "").toLowerCase();
    if (u === "bn" || u === "b" || u === "billion") v *= 1e9; else if (u === "m" || u === "mm" || u === "million") v *= 1e6; else if (u === "k" || u === "thousand") v *= 1e3;
    return { v, u };
  });
  if (!nums.length) return {};
  const UNIT = { bn: 1e9, b: 1e9, billion: 1e9, m: 1e6, mm: 1e6, million: 1e6, k: 1e3, thousand: 1e3 };
  if (nums.length >= 2 && !nums[0].u && nums[1].u) nums[0].v *= UNIT[nums[1].u.toLowerCase()] || 1;
  const out = { min: nums[0].v, cur };
  if (nums[1]) out.max = nums[1].v;
  return out;
}

/* ---------- Reference data ---------- */
const CONTACT_TYPES = ["Single family office", "Multi-family office", "HNWI / Private investor", "Institutional LP", "Pension fund", "Sovereign wealth fund", "Endowment / Foundation", "Insurance company", "Fund of funds", "Corporate / Strategic", "Private bank / Wealth manager", "Angel / Syndicate lead", "Fund manager (GP)", "Intermediary / Advisor", "Other"];
const ASSET_CLASSES = {
  "Private Equity": ["Buyout", "Growth equity", "Minority stakes", "Secondaries", "Co-investment", "Search fund", "Special situations"],
  "Venture Capital": ["Pre-seed / Seed", "Series A", "Series B", "Series C+ / Late stage", "Venture debt"],
  "Real Estate": ["Residential", "Commercial / Office", "Logistics", "Hospitality", "Development", "Student / BTR", "Real estate debt"],
  "Infrastructure": ["Energy transition", "Digital infrastructure", "Transport", "Social infrastructure"],
  "Private Credit": ["Direct lending", "Mezzanine", "Asset-backed", "Distressed / Special sits", "Trade finance"],
  "Public Markets": ["Listed equities", "Fixed income", "PIPEs", "Pre-IPO"],
  "Hedge Funds": ["Long/short", "Macro", "Multi-strategy", "Quant"],
  "Digital Assets": ["Tokens", "Web3 venture", "Funds"],
  "Natural Resources": ["Mining", "Oil & gas", "Agriculture", "Commodities"],
};
const ASSET_KEYS = Object.keys(ASSET_CLASSES);
const ASSET_SHORT = { "Private Equity": "PE", "Venture Capital": "VC", "Real Estate": "RE", "Infrastructure": "Infra", "Private Credit": "Credit", "Public Markets": "Public", "Hedge Funds": "HF", "Digital Assets": "Digital", "Natural Resources": "Nat Res" };
const STRUCTURES = ["Fund commitments", "Direct deals", "Co-investments", "Club deals", "Secondaries", "SPVs"];
const LEAD_PREFS = ["Any", "Lead", "Co-lead", "Follow"];
const OWNERSHIP = ["Either", "Majority", "Minority"];
const CONSTRAINTS = ["Sharia-compliant only", "ESG / impact mandate", "No leverage", "No tobacco / gambling / alcohol", "Board seat required", "Local currency only", "UK EIS/SEIS"];
const SECTORS = ["Generalist", "Technology", "Fintech", "Healthcare", "Consumer", "Industrials", "Energy & Climate", "Real assets", "Financial services", "Media & Telecom", "Education", "Food & Agri", "Logistics", "Business services", "Sports & Entertainment", "Hospitality"];
const REGIONS = ["UK & Ireland", "Europe", "North America", "Latin America", "GCC", "Middle East & N. Africa", "Sub-Saharan Africa", "South Asia", "East Asia", "Southeast Asia", "Oceania", "Global"];
const STAGES = ["Prospect", "In discussion", "Diligence", "Committed", "Closed"];
const ALL_STAGES = [...STAGES, "Passed"];
const ACTIVE_STAGES = ["Prospect", "In discussion", "Diligence", "Committed"];
const STAGE_COLOR = { Prospect: "#6F685E", "In discussion": "#93AAC4", Diligence: "#DCA65A", Committed: "#C9AE80", Closed: "#8FB38B", Passed: "#5E574E" };
const STRENGTH = ["", "Cold", "Aware", "Warm", "Strong", "Inner circle"];
const TIERS = ["A", "B", "C"];
const LAWFUL = ["Legitimate interest", "Consent", "Contract", "Not yet assessed"];
const CHANNELS = ["Email", "Phone", "WhatsApp", "In person", "LinkedIn", "Via EA"];
const DEAL_TYPES = ["Placement (fee-based)", "Co-invest / Syndication", "Direct allocation"];
const DEAL_STATUS = ["Live", "Paused", "Closed", "Archived"];
const INSTRUMENTS = ["Equity", "Preferred equity", "Convertible", "Senior debt", "Mezzanine", "Fund LP interest", "SPV units", "Other"];
const DOC_CATS = ["NDA", "Teaser", "Information memorandum", "Pitch deck", "Financial model", "Term sheet", "Subscription docs", "KYC / AML", "Fee agreement", "Other"];
const FEE_AGREEMENT = ["None", "Verbal", "Email confirmed", "Signed"];
const FEE_STATUS = ["Not yet due", "Invoiced", "Received", "Waived", "Disputed"];
const INTRO_DIR = ["We introduced (fee receivable)", "Introduced to us (fee payable)"];
const ACT_TYPES = { note: "Note", call: "Call", meeting: "Meeting", email: "Email", whatsapp: "WhatsApp" };

/* Countries → [region, IANA timezone] */
const COUNTRIES = {
  "United Kingdom": ["UK & Ireland", "Europe/London"], "Ireland": ["UK & Ireland", "Europe/Dublin"], "Jersey": ["UK & Ireland", "Europe/Jersey"], "Guernsey": ["UK & Ireland", "Europe/Guernsey"],
  "France": ["Europe", "Europe/Paris"], "Germany": ["Europe", "Europe/Berlin"], "Switzerland": ["Europe", "Europe/Zurich"], "Monaco": ["Europe", "Europe/Monaco"], "Luxembourg": ["Europe", "Europe/Luxembourg"], "Netherlands": ["Europe", "Europe/Amsterdam"], "Belgium": ["Europe", "Europe/Brussels"], "Spain": ["Europe", "Europe/Madrid"], "Portugal": ["Europe", "Europe/Lisbon"], "Italy": ["Europe", "Europe/Rome"], "Austria": ["Europe", "Europe/Vienna"], "Sweden": ["Europe", "Europe/Stockholm"], "Norway": ["Europe", "Europe/Oslo"], "Denmark": ["Europe", "Europe/Copenhagen"], "Finland": ["Europe", "Europe/Helsinki"], "Poland": ["Europe", "Europe/Warsaw"], "Greece": ["Europe", "Europe/Athens"], "Cyprus": ["Europe", "Asia/Nicosia"], "Malta": ["Europe", "Europe/Malta"], "Liechtenstein": ["Europe", "Europe/Vaduz"], "Czech Republic": ["Europe", "Europe/Prague"], "Romania": ["Europe", "Europe/Bucharest"], "Turkey": ["Europe", "Europe/Istanbul"],
  "United States": ["North America", "America/New_York"], "Canada": ["North America", "America/Toronto"], "Mexico": ["Latin America", "America/Mexico_City"], "Brazil": ["Latin America", "America/Sao_Paulo"], "Argentina": ["Latin America", "America/Argentina/Buenos_Aires"], "Chile": ["Latin America", "America/Santiago"], "Colombia": ["Latin America", "America/Bogota"], "Cayman Islands": ["Latin America", "America/Cayman"], "Bermuda": ["North America", "Atlantic/Bermuda"],
  "United Arab Emirates": ["GCC", "Asia/Dubai"], "Saudi Arabia": ["GCC", "Asia/Riyadh"], "Qatar": ["GCC", "Asia/Qatar"], "Kuwait": ["GCC", "Asia/Kuwait"], "Bahrain": ["GCC", "Asia/Bahrain"], "Oman": ["GCC", "Asia/Muscat"],
  "Egypt": ["Middle East & N. Africa", "Africa/Cairo"], "Jordan": ["Middle East & N. Africa", "Asia/Amman"], "Lebanon": ["Middle East & N. Africa", "Asia/Beirut"], "Israel": ["Middle East & N. Africa", "Asia/Jerusalem"], "Morocco": ["Middle East & N. Africa", "Africa/Casablanca"], "Iraq": ["Middle East & N. Africa", "Asia/Baghdad"],
  "Nigeria": ["Sub-Saharan Africa", "Africa/Lagos"], "Kenya": ["Sub-Saharan Africa", "Africa/Nairobi"], "South Africa": ["Sub-Saharan Africa", "Africa/Johannesburg"], "Ghana": ["Sub-Saharan Africa", "Africa/Accra"], "Mauritius": ["Sub-Saharan Africa", "Indian/Mauritius"],
  "India": ["South Asia", "Asia/Kolkata"], "Pakistan": ["South Asia", "Asia/Karachi"], "Bangladesh": ["South Asia", "Asia/Dhaka"], "Sri Lanka": ["South Asia", "Asia/Colombo"],
  "China": ["East Asia", "Asia/Shanghai"], "Hong Kong": ["East Asia", "Asia/Hong_Kong"], "Japan": ["East Asia", "Asia/Tokyo"], "South Korea": ["East Asia", "Asia/Seoul"], "Taiwan": ["East Asia", "Asia/Taipei"],
  "Singapore": ["Southeast Asia", "Asia/Singapore"], "Malaysia": ["Southeast Asia", "Asia/Kuala_Lumpur"], "Indonesia": ["Southeast Asia", "Asia/Jakarta"], "Thailand": ["Southeast Asia", "Asia/Bangkok"], "Vietnam": ["Southeast Asia", "Asia/Ho_Chi_Minh"], "Philippines": ["Southeast Asia", "Asia/Manila"],
  "Australia": ["Oceania", "Australia/Sydney"], "New Zealand": ["Oceania", "Pacific/Auckland"],
};
const COUNTRY_ALIASES = { uk: "United Kingdom", "u.k.": "United Kingdom", "great britain": "United Kingdom", britain: "United Kingdom", england: "United Kingdom", scotland: "United Kingdom", wales: "United Kingdom", gb: "United Kingdom", usa: "United States", us: "United States", "u.s.": "United States", "u.s.a.": "United States", america: "United States", "united states of america": "United States", uae: "United Arab Emirates", emirates: "United Arab Emirates", ksa: "Saudi Arabia", saudi: "Saudi Arabia", korea: "South Korea", "hong kong sar": "Hong Kong", hk: "Hong Kong", holland: "Netherlands", "the netherlands": "Netherlands", czechia: "Czech Republic", turkiye: "Turkey", "türkiye": "Turkey" };
const CITIES = {
  london: ["United Kingdom"], manchester: ["United Kingdom"], edinburgh: ["United Kingdom"], birmingham: ["United Kingdom"], dublin: ["Ireland"],
  paris: ["France"], geneva: ["Switzerland"], zurich: ["Switzerland"], "zürich": ["Switzerland"], lugano: ["Switzerland"], frankfurt: ["Germany"], munich: ["Germany"], berlin: ["Germany"], hamburg: ["Germany"], amsterdam: ["Netherlands"], madrid: ["Spain"], barcelona: ["Spain"], milan: ["Italy"], rome: ["Italy"], lisbon: ["Portugal"], stockholm: ["Sweden"], oslo: ["Norway"], copenhagen: ["Denmark"], vienna: ["Austria"], brussels: ["Belgium"], istanbul: ["Turkey"], monaco: ["Monaco"], "monte carlo": ["Monaco"], luxembourg: ["Luxembourg"],
  "new york": ["United States", "America/New_York"], nyc: ["United States", "America/New_York"], boston: ["United States", "America/New_York"], miami: ["United States", "America/New_York"], chicago: ["United States", "America/Chicago"], dallas: ["United States", "America/Chicago"], houston: ["United States", "America/Chicago"], "san francisco": ["United States", "America/Los_Angeles"], "los angeles": ["United States", "America/Los_Angeles"], "palo alto": ["United States", "America/Los_Angeles"], seattle: ["United States", "America/Los_Angeles"], toronto: ["Canada", "America/Toronto"], vancouver: ["Canada", "America/Vancouver"], "sao paulo": ["Brazil"],
  dubai: ["United Arab Emirates"], "abu dhabi": ["United Arab Emirates"], riyadh: ["Saudi Arabia"], jeddah: ["Saudi Arabia"], doha: ["Qatar"], "kuwait city": ["Kuwait"], manama: ["Bahrain"], muscat: ["Oman"], cairo: ["Egypt"], beirut: ["Lebanon"], amman: ["Jordan"], "tel aviv": ["Israel"], casablanca: ["Morocco"],
  lagos: ["Nigeria"], nairobi: ["Kenya"], johannesburg: ["South Africa"], "cape town": ["South Africa"],
  mumbai: ["India"], delhi: ["India"], "new delhi": ["India"], bangalore: ["India"], bengaluru: ["India"], karachi: ["Pakistan"], lahore: ["Pakistan"], islamabad: ["Pakistan"], dhaka: ["Bangladesh"],
  singapore: ["Singapore"], "kuala lumpur": ["Malaysia"], jakarta: ["Indonesia"], bangkok: ["Thailand"], shanghai: ["China"], beijing: ["China"], shenzhen: ["China"], tokyo: ["Japan"], seoul: ["South Korea"], taipei: ["Taiwan"],
  sydney: ["Australia", "Australia/Sydney"], melbourne: ["Australia", "Australia/Melbourne"], perth: ["Australia", "Australia/Perth"], auckland: ["New Zealand"],
};
function canonCountry(s) {
  if (!s) return "";
  const k = String(s).trim(); const l = k.toLowerCase();
  if (COUNTRIES[k]) return k;
  const hit = Object.keys(COUNTRIES).find((c) => c.toLowerCase() === l);
  if (hit) return hit;
  if (COUNTRY_ALIASES[l]) return COUNTRY_ALIASES[l];
  if (CITIES[l]) return CITIES[l][0];
  return k;
}
/** From free-text location → {city,country} */
function parseLocation(txt) {
  if (!txt) return {};
  const parts = String(txt).split(/[,/|·]/).map((p) => p.trim()).filter(Boolean);
  let city = "", country = "";
  for (const p of parts) {
    const l = p.toLowerCase(); const cc = canonCountry(p);
    if (!city && CITIES[l]) { city = p; if (!country) country = CITIES[l][0]; continue; }
    if (!country && COUNTRIES[cc]) { country = cc; continue; }
  }
  if (!city && parts[0] && canonCountry(parts[0]) !== country) city = parts[0];
  return { city, country };
}
function regionOf(country) { return (COUNTRIES[canonCountry(country)] || [])[0] || ""; }
function tzOf(c) {
  const city = String(c.city || "").toLowerCase();
  if (CITIES[city] && CITIES[city][1]) return CITIES[city][1];
  return (COUNTRIES[canonCountry(c.country)] || [])[1] || "";
}
function localTime(c) {
  const tz = tzOf(c); if (!tz) return "";
  try { return new Date().toLocaleTimeString("en-GB", { timeZone: tz, hour: "2-digit", minute: "2-digit" }); } catch { return ""; }
}

/* Fuzzy normalisers for imported free text */
const ASSET_WORDS = [
  [/private equity|\bpe\b|buy-?out|lbo|growth equity|secondar|minority stake/i, "Private Equity"],
  [/venture|\bvc\b|seed|series [a-d]|start-?up|early stage|angel/i, "Venture Capital"],
  [/real estate|property|\bre\b|reit|residential|commercial|logistics|hospitality|btr|pbsa/i, "Real Estate"],
  [/infra|energy transition|renewable|digital infra|data cent|transport/i, "Infrastructure"],
  [/credit|debt|lending|loan|mezz|asset.backed|distress|special sit/i, "Private Credit"],
  [/public|listed|equities|stocks|fixed income|bonds|pipe|pre-?ipo/i, "Public Markets"],
  [/hedge|long.short|macro|quant|multi-?strat/i, "Hedge Funds"],
  [/crypto|digital asset|web3|token|blockchain/i, "Digital Assets"],
  [/mining|oil|gas|commodit|natural resource|agri/i, "Natural Resources"],
];
function parseAssetClasses(txt) { if (!txt) return []; const s = String(txt); return uniq(ASSET_WORDS.filter(([re]) => re.test(s)).map(([, v]) => v)); }
const REGION_WORDS = [
  [/\buk\b|united kingdom|britain|ireland|london|england/i, "UK & Ireland"],
  [/europe|\beu\b|dach|nordic|benelux|france|germany|switzerland|spain|italy/i, "Europe"],
  [/north america|\busa?\b|united states|canada|americas/i, "North America"],
  [/latam|latin america|brazil|mexico|south america/i, "Latin America"],
  [/gcc|gulf|uae|saudi|ksa|qatar|kuwait|bahrain|oman|dubai|abu dhabi|riyadh/i, "GCC"],
  [/mena|middle east|north africa|egypt|jordan|levant|turkey/i, "Middle East & N. Africa"],
  [/africa|nigeria|kenya|south africa/i, "Sub-Saharan Africa"],
  [/south asia|india|pakistan|bangladesh/i, "South Asia"],
  [/east asia|china|japan|korea|hong kong|taiwan/i, "East Asia"],
  [/southeast asia|sea\b|asean|singapore|malaysia|indonesia|vietnam|thailand/i, "Southeast Asia"],
  [/australia|new zealand|oceania|anz/i, "Oceania"],
  [/global|worldwide|international|agnostic/i, "Global"],
];
function parseRegions(txt) { if (!txt) return []; const s = String(txt); return uniq(REGION_WORDS.filter(([re]) => re.test(s)).map(([, v]) => v)); }
function parseSectors(txt) { if (!txt) return []; const s = String(txt).toLowerCase(); return SECTORS.filter((x) => s.includes(x.toLowerCase().split(" ")[0])); }
function parseType(txt) {
  if (!txt) return "";
  const s = String(txt).toLowerCase();
  if (/multi.?family/.test(s)) return "Multi-family office";
  if (/family office|\bsfo\b|\bfo\b/.test(s)) return "Single family office";
  if (/\bmfo\b/.test(s)) return "Multi-family office";
  if (/sovereign|\bswf\b/.test(s)) return "Sovereign wealth fund";
  if (/pension/.test(s)) return "Pension fund";
  if (/endow|foundation/.test(s)) return "Endowment / Foundation";
  if (/insur/.test(s)) return "Insurance company";
  if (/fund of funds|\bfof\b/.test(s)) return "Fund of funds";
  if (/private bank|wealth/.test(s)) return "Private bank / Wealth manager";
  if (/angel|syndicate/.test(s)) return "Angel / Syndicate lead";
  if (/hnw|private investor|individual|uhnw/.test(s)) return "HNWI / Private investor";
  if (/\blp\b|institution/.test(s)) return "Institutional LP";
  if (/corporate|strategic/.test(s)) return "Corporate / Strategic";
  if (/\bgp\b|fund manager|asset manager/.test(s)) return "Fund manager (GP)";
  if (/advis|bank|broker|placement|lawyer|intermediar/.test(s)) return "Intermediary / Advisor";
  return CONTACT_TYPES.find((t) => t.toLowerCase() === s) || "";
}

/* ---------- Icons ---------- */
const IC = {
  overview: '<path d="M3 13h7V3H3zM14 21h7V11h-7zM3 21h7v-5H3zM14 3v5h7V3z"/>',
  contacts: '<circle cx="9" cy="8" r="3.5"/><path d="M2.5 20c.8-3.6 3.4-5.5 6.5-5.5s5.7 1.9 6.5 5.5"/><path d="M16 4.5a3.5 3.5 0 0 1 0 7M21.5 20c-.5-2.6-2-4.3-4-5"/>',
  match: '<path d="M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9z"/><path d="M19 16l.8 2.2L22 19l-2.2.8L19 22l-.8-2.2L16 19l2.2-.8z"/>',
  deals: '<rect x="3" y="4" width="5" height="16" rx="1"/><rect x="10" y="4" width="5" height="11" rx="1"/><rect x="17" y="4" width="4" height="7" rx="1"/>',
  intros: '<path d="M7 7h11l-3-3M17 17H6l3 3"/>',
  tasks: '<path d="M4 6l2 2 3-3M4 13l2 2 3-3M4 20l2 2 3-3" transform="translate(0 -2)"/><path d="M12 5h9M12 12h9M12 19h9"/>',
  vault: '<rect x="3" y="4" width="18" height="16" rx="1.5"/><circle cx="12" cy="12" r="3.2"/><path d="M12 8.8V7M12 17v-1.8M15.2 12H17M7 12h1.8"/>',
  security: '<path d="M12 3l8 3v6c0 4.6-3.3 8-8 9-4.7-1-8-4.4-8-9V6z"/><path d="M9 12l2 2 4-4"/>',
  settings: '<circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.7 1.7 0 0 0 .3 1.8l.1.1a2 2 0 1 1-2.8 2.8l-.1-.1a1.7 1.7 0 0 0-1.8-.3 1.7 1.7 0 0 0-1 1.5V21a2 2 0 1 1-4 0v-.1a1.7 1.7 0 0 0-1.1-1.5 1.7 1.7 0 0 0-1.8.3l-.1.1a2 2 0 1 1-2.8-2.8l.1-.1a1.7 1.7 0 0 0 .3-1.8 1.7 1.7 0 0 0-1.5-1H3a2 2 0 1 1 0-4h.1a1.7 1.7 0 0 0 1.5-1.1 1.7 1.7 0 0 0-.3-1.8l-.1-.1a2 2 0 1 1 2.8-2.8l.1.1a1.7 1.7 0 0 0 1.8.3H9a1.7 1.7 0 0 0 1-1.5V3a2 2 0 1 1 4 0v.1a1.7 1.7 0 0 0 1 1.5 1.7 1.7 0 0 0 1.8-.3l.1-.1a2 2 0 1 1 2.8 2.8l-.1.1a1.7 1.7 0 0 0-.3 1.8V9a1.7 1.7 0 0 0 1.5 1H21a2 2 0 1 1 0 4h-.1a1.7 1.7 0 0 0-1.5 1z"/>',
  search: '<circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/>',
  bell: '<path d="M6 8a6 6 0 1 1 12 0c0 7 3 9 3 9H3s3-2 3-9"/><path d="M10.3 21a1.9 1.9 0 0 0 3.4 0"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  upload: '<path d="M12 16V4M7 9l5-5 5 5"/><path d="M4 16v3a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-3"/>',
  download: '<path d="M12 4v12M7 11l5 5 5-5"/><path d="M4 16v3a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-3"/>',
  close: '<path d="M6 6l12 12M18 6L6 18"/>',
  eye: '<path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/>',
  eyeoff: '<path d="M3 3l18 18M10.6 5.1A10 10 0 0 1 12 5c6.4 0 10 7 10 7a17 17 0 0 1-3 3.7M6.6 6.6A17 17 0 0 0 2 12s3.6 7 10 7a9.7 9.7 0 0 0 5.4-1.6"/><path d="M9.9 9.9a3 3 0 0 0 4.2 4.2"/>',
  logout: '<path d="M15 4h3a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-3M10 17l-5-5 5-5M5 12h11"/>',
  lock: '<rect x="4" y="11" width="16" height="10" rx="1.5"/><path d="M8 11V7a4 4 0 0 1 8 0v4"/>',
  sparkle: '<path d="M12 3l1.9 5.1L19 10l-5.1 1.9L12 17l-1.9-5.1L5 10l5.1-1.9z"/>',
  more: '<circle cx="5" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/>',
  back: '<path d="M15 18l-6-6 6-6"/>',
  file: '<path d="M14 3H6a1 1 0 0 0-1 1v16a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V8z"/><path d="M14 3v5h5"/>',
  edit: '<path d="M4 20h4L19 9l-4-4L4 16z"/>',
  phone: '<path d="M5 3h4l2 5-3 2a11 11 0 0 0 6 6l2-3 5 2v4a2 2 0 0 1-2 2A17 17 0 0 1 3 5a2 2 0 0 1 2-2"/>',
  mail: '<rect x="3" y="5" width="18" height="14" rx="1.5"/><path d="M3 7l9 6 9-6"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
};
const icon = (k, cls = "") => `<svg viewBox="0 0 24 24" class="ic ${cls}" aria-hidden="true">${IC[k] || ""}</svg>`;
/* =========================================================
   State, capabilities, persistence, audit
   ========================================================= */
const S = {
  db: null, room: null, sample: null, assets: null, downloads: null,
  ready: false, me: null, session: null,
  security: null, settings: { fx: { ...DEFAULT_FX }, cadence: { A: 30, B: 60, C: 120 }, stallDays: 21 },
  users: new Map(), contacts: new Map(), deals: new Map(), intros: new Map(), tasks: new Map(), docs: new Map(), matches: new Map(),
  auditDays: [], peers: [], loaded: {},
  route: { name: "overview", id: null }, ui: { contacts: { q: "", type: "", asset: "", region: "", strength: "", tier: "", owner: "", tag: "", sort: "name", limit: 100, sel: new Set() }, deals: { view: "cards", q: "", status: "Live" }, tasks: { who: "all" }, security: { tab: "log", q: "", user: "", kind: "" }, contactTab: "profile", dealTab: "pipeline" },
  discreet: false,
};
const LS = {
  get(k, d) { try { const v = localStorage.getItem("uc." + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem("uc." + k, JSON.stringify(v)); } catch {} },
  del(k) { try { localStorage.removeItem("uc." + k); } catch {} },
};
const SS = {
  get(k, d) { try { const v = sessionStorage.getItem("uc." + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { sessionStorage.setItem("uc." + k, JSON.stringify(v)); } catch {} },
  del(k) { try { sessionStorage.removeItem("uc." + k); } catch {} },
};
function deviceId() { let d = LS.get("device"); if (!d) { d = uid("dev_"); LS.set("device", d); } return d; }
function deviceLabel() {
  const ua = navigator.userAgent;
  const os = /iPhone|iPad/.test(ua) ? (/iPad/.test(ua) ? "iPad" : "iPhone") : /Android/.test(ua) ? "Android" : /Mac OS X/.test(ua) ? "Mac" : /Windows/.test(ua) ? "Windows" : /Linux/.test(ua) ? "Linux" : "Device";
  const br = /Edg\//.test(ua) ? "Edge" : /OPR\//.test(ua) ? "Opera" : /Chrome\//.test(ua) ? "Chrome" : /Firefox\//.test(ua) ? "Firefox" : /Safari\//.test(ua) ? "Safari" : "Browser";
  let tz = ""; try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone; } catch {}
  return `${br} on ${os}${tz ? " · " + tz : ""} · ${deviceId().slice(-6)}`;
}

/* ---------- Crypto (PBKDF2-SHA256) ---------- */
const b64 = (buf) => btoa(String.fromCharCode(...new Uint8Array(buf)));
const unb64 = (s) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
async function pbkdf2(secret, saltB64, iter) {
  const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(secret), "PBKDF2", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: unb64(saltB64), iterations: iter }, key, 256);
  return b64(bits);
}
function newSalt() { return b64(crypto.getRandomValues(new Uint8Array(16))); }
function safeEq(a, b) { if (!a || !b || a.length !== b.length) return false; let r = 0; for (let i = 0; i < a.length; i++) r |= a.charCodeAt(i) ^ b.charCodeAt(i); return r === 0; }
async function hashSecret(secret, iter = 150000) { const salt = newSalt(); return { salt, hash: await pbkdf2(secret, salt, iter), iter }; }

/* ---------- Capabilities ---------- */
async function initCapabilities() {
  const use = (n) => (window.claude && window.claude.use ? window.claude.use(n).catch(() => null) : Promise.resolve(null));
  const [db, room, sample, assets, downloads] = await Promise.all([use("db"), use("room"), use("sample"), use("assets"), use("downloads")]);
  Object.assign(S, { db, room, sample, assets, downloads });
}

/* ---------- Live subscriptions ---------- */
const unsubs = [];
function subscribeCore() {
  // Security + settings are needed before sign-in.
  unsubs.push(S.db.doc("settings/security").onSnapshot((snap) => { S.security = snap.exists ? snap.data() : null; S.loaded.security = true; gateRefresh(); }, dbFail));
  unsubs.push(S.db.doc("settings/general").onSnapshot((snap) => { if (snap.exists) S.settings = { ...S.settings, ...snap.data(), fx: { ...DEFAULT_FX, ...(snap.data().fx || {}) } }; scheduleRender(); }, dbFail));
  unsubs.push(S.db.collection("users").onSnapshot((snap) => { S.users = new Map(snap.docs.map((d) => [d.id, { id: d.id, ...d.data() }])); S.loaded.users = true; if (S.me) { const fresh = S.users.get(S.me.id); if (!fresh || fresh.active === false) { forceSignOut("Your access was revoked by a super admin."); return; } S.me = fresh; } gateRefresh(); scheduleRender(); }, dbFail));
}
function subscribeData() {
  if (S._dataSubbed) return; S._dataSubbed = true;
  const coll = (name, key) => unsubs.push(S.db.collection(name).onSnapshot((snap) => { S[key] = new Map(snap.docs.map((d) => [d.id, { id: d.id, ...d.data() }])); S.loaded[key] = true; scheduleRender(); }, dbFail));
  coll("contacts", "contacts"); coll("deals", "deals"); coll("intros", "intros"); coll("tasks", "tasks"); coll("docs", "docs"); coll("matches", "matches");
  unsubs.push(S.db.collection("audit").orderBy("day", "desc").limit(60).onSnapshot((snap) => { S.auditDays = snap.docs.map((d) => ({ id: d.id, ...d.data() })); scheduleRender(); }, dbFail));
}
function dbFail(e) {
  console.warn("db", e);
  if (e && e.code === "revoked") toast("Access to this workspace changed. Reload to continue.", true);
  else if (e && e.code === "resource_exhausted") toast("Too many live connections. Reload the page.", true);
}

/* ---------- Writes (stamped + audited) ---------- */
function stamp(data, isNew) {
  const t = now(); const by = S.me ? S.me.id : "system";
  return isNew ? { ...data, createdAt: data.createdAt || t, createdBy: data.createdBy || by, updatedAt: t, updatedBy: by } : { ...data, updatedAt: t, updatedBy: by };
}
function clean(o) { const out = {}; for (const [k, v] of Object.entries(o)) if (v !== undefined && k !== "id") out[k] = v; return out; }
async function withRetry(fn) {
  try { return await fn(); } catch (e) {
    if (e && (e.code === "unavailable" || e.code === "resource_exhausted")) { await sleep(600 + Math.random() * 900); return await fn(); }
    throw e;
  }
}
function writeErr(e) {
  const code = e && e.code;
  if (code === "quota_exceeded") toast("Storage is full (5,000 records). Archive or delete old records, then try again.", true);
  else if (code === "invalid_argument") toast("That record is too large or malformed. Shorten long notes and try again.", true);
  else toast("Couldn't save. Check your connection and try again.", true);
  console.warn(e);
}
async function put(coll, id, data, summary) {
  const isNew = !S[collKey(coll)]?.has?.(id);
  const body = clean(stamp(data, isNew));
  try { await withRetry(() => S.db.doc(`${coll}/${id}`).set(body)); } catch (e) { writeErr(e); throw e; }
  if (summary !== false) audit(isNew ? "create" : "update", coll, id, summary || `${isNew ? "Created" : "Updated"} ${entityLabel(coll, { id, ...body })}`);
  return id;
}
async function patch(coll, id, data, summary) {
  const body = clean(stamp(data, false));
  try { await withRetry(() => S.db.doc(`${coll}/${id}`).update(body)); } catch (e) { writeErr(e); throw e; }
  if (summary !== false) audit("update", coll, id, summary || `Updated ${entityLabel(coll, { id, ...(S[collKey(coll)]?.get?.(id) || {}) })}`);
}
async function remove(coll, id, summary) {
  try { await withRetry(() => S.db.doc(`${coll}/${id}`).delete()); } catch (e) { writeErr(e); throw e; }
  audit("delete", coll, id, summary || `Deleted ${coll.slice(0, -1)}`);
}
function collKey(c) { return { contacts: "contacts", deals: "deals", intros: "intros", tasks: "tasks", docs: "docs", users: "users", matches: "matches" }[c]; }
function entityLabel(coll, o) {
  if (coll === "contacts") return `contact ${contactName(o)}`;
  if (coll === "deals") return `deal ${o.name || ""}`;
  if (coll === "tasks") return `task “${o.title || ""}”`;
  if (coll === "docs") return `document ${o.name || ""}`;
  if (coll === "intros") return "introduction";
  if (coll === "users") return `user ${o.name || ""}`;
  return coll;
}

/* Audit: one document per day, events merged in as a map (bounded growth). */
const auditQueue = [];
let auditFlushing = false;
function audit(action, entity, id, summary, extra) {
  const ev = { t: now(), u: S.me ? S.me.id : null, n: S.me ? S.me.name : extra?.name || "Unknown", a: action, e: entity || "", i: id || "", s: String(summary || "").slice(0, 300), d: deviceLabel() };
  auditQueue.push(ev); flushAudit();
}
async function flushAudit() {
  if (auditFlushing || !S.db) return; auditFlushing = true;
  try {
    while (auditQueue.length) {
      const batch = auditQueue.splice(0, 25);
      const day = dayKey(batch[0].t);
      const ev = {}; batch.forEach((b) => (ev[uid("e")] = b));
      const ref = S.db.doc(`audit/${day}`);
      try { await ref.update({ ev }); }
      catch (e) {
        if (e && e.code === "invalid_argument") {
          const snap = await ref.get().catch(() => null);
          if (snap && !snap.exists) await ref.set({ day, ev }).catch(() => {});
          else { const alt = S.db.doc(`audit/${day}-${Date.now().toString(36)}`); await alt.set({ day, ev }).catch(() => {}); }
        } else { auditQueue.unshift(...batch); await sleep(1500); if (e && e.code === "revoked") break; }
      }
    }
  } finally { auditFlushing = false; }
}
function auditEvents(limit = 500) {
  const all = [];
  for (const d of S.auditDays) for (const [k, v] of Object.entries(d.ev || {})) if (v) all.push({ k, ...v });
  all.sort((a, b) => b.t - a.t);
  return all.slice(0, limit);
}

/* ---------- Helpers on records ---------- */
function contactName(c) { if (!c) return ""; return c.fullName || [c.firstName, c.lastName].filter(Boolean).join(" ") || c.email || "Unnamed"; }
function userName(id) { const u = S.users.get(id); return u ? u.name : id === "system" ? "System" : ""; }
function strengthDots(n) { n = Number(n) || 0; return `<span class="strength" title="${esc(STRENGTH[n] || "Unrated")}">${[1, 2, 3, 4, 5].map((i) => `<i class="${i <= n ? "on" : ""}"></i>`).join("")}</span>`; }
function lastTouch(c) { return Math.max(toTime(c.lastContacted), ...Object.values(c.log || {}).filter(Boolean).map((l) => toTime(l.date))); }
function isCold(c) { const days = (S.settings.cadence || {})[c.tier || "B"] || 60; const lt = lastTouch(c); return c.tier && c.tier !== "C" ? (!lt || daysSince(lt) > days) : lt ? daysSince(lt) > days : false; }
function engagementsOf(contactId) {
  const out = [];
  for (const d of S.deals.values()) { const e = (d.pipeline || {})[contactId]; if (e) out.push({ deal: d, e }); }
  return out;
}
function allEngagements() {
  const out = [];
  for (const d of S.deals.values()) for (const [cid, e] of Object.entries(d.pipeline || {})) if (e) out.push({ deal: d, cid, e, c: S.contacts.get(cid) });
  return out;
}
function docCount() { return S.contacts.size + S.deals.size + S.intros.size + S.tasks.size + S.docs.size + S.users.size + S.matches.size + S.auditDays.length + 2; }

/* ---------- UI helpers ---------- */
function toast(msg, err) {
  const el = document.createElement("div"); el.className = "toast" + (err ? " err" : ""); el.textContent = msg;
  $("#toasts").appendChild(el); setTimeout(() => el.remove(), err ? 6000 : 3200);
}
let modalStack = [];
function openModal(html, { size = "", onClose } = {}) {
  const wrap = document.createElement("div");
  wrap.innerHTML = `<div class="scrim" data-act="modal-close"></div><div class="modal ${size}" role="dialog" aria-modal="true">${html}</div>`;
  document.body.appendChild(wrap); modalStack.push({ wrap, onClose });
  setTimeout(() => { const f = wrap.querySelector("[autofocus], .modal input:not([type=hidden]), .modal textarea, .modal select"); if (f) f.focus(); }, 30);
  return wrap.querySelector(".modal");
}
function closeModal() { const m = modalStack.pop(); if (m) { m.wrap.remove(); m.onClose && m.onClose(); } }
function modalShell(title, body, foot, sub = "") {
  return `<div class="modal-h"><div><div class="h-section">${title}</div>${sub ? `<div class="hint" style="margin-top:4px">${sub}</div>` : ""}</div><button class="iconbtn" data-act="modal-close" aria-label="Close">${icon("close")}</button></div><div class="modal-b">${body}</div>${foot ? `<div class="modal-f">${foot}</div>` : ""}`;
}
function confirmBox(title, text, okLabel = "Confirm", danger = false) {
  return new Promise((res) => {
    const m = openModal(modalShell(title, `<p class="dim" style="margin-top:14px">${text}</p>`, `<button class="btn ghost" id="cf-no">Cancel</button><button class="btn ${danger ? "danger" : "primary"}" id="cf-yes">${esc(okLabel)}</button>`), { size: "narrow", onClose: () => res(false) });
    m.querySelector("#cf-yes").onclick = () => { modalStack.pop().wrap.remove(); res(true); };
    m.querySelector("#cf-no").onclick = () => closeModal();
  });
}
function pinPrompt(title, text) {
  return new Promise((res) => {
    const m = openModal(modalShell(title, `<p class="dim" style="margin-top:14px">${text}</p><div class="field"><label for="pp-pin">Your PIN</label><input class="input" id="pp-pin" type="password" inputmode="numeric" autocomplete="off" maxlength="12" autofocus></div><div class="gate-msg" id="pp-msg"></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="pp-ok">Confirm</button>`), { size: "narrow", onClose: () => res(false) });
    const go = async () => {
      const v = m.querySelector("#pp-pin").value; const u = S.users.get(S.me.id);
      if (u && safeEq(await pbkdf2(v, u.pinSalt, u.iter), u.pinHash)) { modalStack.pop().wrap.remove(); res(true); }
      else { m.querySelector("#pp-msg").textContent = "That PIN doesn't match."; audit("security", "users", S.me.id, "Failed PIN re-confirmation"); }
    };
    m.querySelector("#pp-ok").onclick = go; m.querySelector("#pp-pin").onkeydown = (e) => e.key === "Enter" && go();
  });
}
/* Keep what someone is typing when live data re-renders a region */
function captureDirty(root, sel) {
  const out = [];
  if (!root) return out;
  root.querySelectorAll(sel).forEach((el) => {
    if (!el.id) return;
    const dirty = el.type === "checkbox" || el.type === "radio" ? el.checked !== el.defaultChecked : el.tagName === "SELECT" ? [...el.options].some((o) => o.selected !== o.defaultSelected) : el.value !== el.defaultValue;
    if (dirty) out.push({ id: el.id, v: el.value, c: el.checked, name: el.name });
  });
  return out;
}
function restoreDirty(list) {
  for (const d of list) { const el = document.getElementById(d.id); if (!el) continue; if (el.type === "checkbox" || el.type === "radio") el.checked = d.c; else el.value = d.v; }
}
let drawerState = null;
function openDrawer(kind, id) { drawerState = { kind, id }; renderDrawer(); }
function closeDrawer() { drawerState = null; const d = $("#drawer"); d.innerHTML = ""; }
function closeMenus() { $$(".menu.float").forEach((m) => m.remove()); }
function showMenu(anchor, items) {
  closeMenus();
  const m = document.createElement("div"); m.className = "menu float";
  m.innerHTML = items.map((it, i) => (it === "-" ? `<div class="divider"></div>` : `<button data-mi="${i}">${it.icon ? icon(it.icon) : ""}${esc(it.label)}</button>`)).join("");
  document.body.appendChild(m);
  const r = anchor.getBoundingClientRect(); const w = Math.max(220, m.offsetWidth);
  m.style.top = `${Math.min(r.bottom + 6 + window.scrollY, window.scrollY + innerHeight - m.offsetHeight - 10)}px`;
  m.style.left = `${clamp(r.right - w + window.scrollX, 8, innerWidth - w - 8)}px`;
  m.onclick = (e) => { const b = e.target.closest("[data-mi]"); if (!b) return; const it = items[+b.dataset.mi]; closeMenus(); it.run && it.run(); };
  setTimeout(() => document.addEventListener("click", closeMenus, { once: true }), 0);
}
async function saveFile(filename, data) {
  if (!S.downloads) { toast("Downloads aren't available in this view.", true); return false; }
  try { await S.downloads.save({ filename, data }); audit("export", "", "", `Exported ${filename}`); return true; }
  catch (e) { if (e && e.code === "declined") toast("Download cancelled."); else if (e && e.code === "rate_limited") toast("A download prompt is already open."); else toast("That download couldn't be prepared.", true); return false; }
}
function toCSV(rows, cols) {
  const q = (v) => { const s = Array.isArray(v) ? v.join("; ") : String(v ?? ""); return /[",\n;]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  return [cols.map((c) => q(c[0])).join(","), ...rows.map((r) => cols.map((c) => q(typeof c[1] === "function" ? c[1](r) : r[c[1]])).join(","))].join("\n");
}
function readFileText(file) { return new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result)); r.onerror = rej; r.readAsText(file); }); }
function readFileBuf(file) { return new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsArrayBuffer(file); }); }
function loadScript(src) {
  return new Promise((res, rej) => { if ($$(`script[src="${src}"]`).length) return res(); const s = document.createElement("script"); s.src = src; s.onload = res; s.onerror = () => rej(new Error("script")); document.head.appendChild(s); });
}
async function pdfText(file) {
  await loadScript("https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js");
  await loadScript("https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js");
  const lib = window.pdfjsLib; lib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  const pdf = await lib.getDocument({ data: new Uint8Array(await readFileBuf(file)) }).promise;
  let out = "";
  for (let p = 1; p <= Math.min(pdf.numPages, 30); p++) { const pg = await pdf.getPage(p); const tc = await pg.getTextContent(); out += tc.items.map((i) => i.str).join(" ") + "\n\n"; if (out.length > 40000) break; }
  return out;
}
async function pdfImages(file, maxPages = 6) {
  await loadScript("https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js");
  await loadScript("https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js");
  const lib = window.pdfjsLib; lib.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js";
  const pdf = await lib.getDocument({ data: new Uint8Array(await readFileBuf(file)), isEvalSupported: false }).promise;
  const out = [];
  for (let p = 1; p <= Math.min(pdf.numPages, maxPages); p++) {
    const page = await pdf.getPage(p); const v0 = page.getViewport({ scale: 1 }); const scale = Math.min(2, 1300 / Math.max(v0.width, v0.height));
    const vp = page.getViewport({ scale }); const c = document.createElement("canvas"); c.width = Math.round(vp.width); c.height = Math.round(vp.height);
    const ctx = c.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, c.width, c.height);
    await page.render({ canvasContext: ctx, viewport: vp }).promise;
    out.push(await new Promise((r) => c.toBlob(r, "image/jpeg", 0.72)));
  }
  return out.filter(Boolean);
}
const decodeXml = (s) => s.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n)).replace(/&amp;/g, "&");
async function zipOf(file) { await loadScript("https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"); return window.JSZip.loadAsync(await readFileBuf(file)); }
async function pptxText(file) {
  const zip = await zipOf(file); const num = (k) => parseInt((k.match(/(\d+)\.xml$/) || [0, 0])[1], 10);
  const slides = Object.keys(zip.files).filter((k) => /^ppt\/slides\/slide\d+\.xml$/.test(k)).sort((a, b) => num(a) - num(b));
  let out = "";
  for (let i = 0; i < slides.length && out.length < 45000; i++) {
    const xml = await zip.file(slides[i]).async("string");
    const paras = xml.split(/<\/a:p>/).map((p) => [...p.matchAll(/<a:t>([^<]*)<\/a:t>/g)].map((m) => decodeXml(m[1])).join("")).filter((t) => t.trim());
    const notesFile = zip.file(`ppt/notesSlides/notesSlide${num(slides[i])}.xml`);
    const notes = notesFile ? [...(await notesFile.async("string")).matchAll(/<a:t>([^<]*)<\/a:t>/g)].map((m) => decodeXml(m[1])).join(" ") : "";
    out += `\n[Slide ${i + 1}]\n${paras.join("\n")}${notes.trim() ? `\nSpeaker notes: ${notes}` : ""}\n`;
  }
  return out;
}
async function docxText(file) {
  const zip = await zipOf(file); const f = zip.file("word/document.xml"); if (!f) return "";
  return (await f.async("string")).split(/<\/w:p>/).map((p) => [...p.matchAll(/<w:t[^>]*>([^<]*)<\/w:t>/g)].map((m) => decodeXml(m[1])).join("")).filter((t) => t.trim()).join("\n");
}
/** Read any supported document into text (and page images for image-heavy PDFs). */
async function readDocument(file, { wantImages = false } = {}) {
  const n = file.name.toLowerCase(); let text = "", images = [];
  if (n.endsWith(".pdf")) { text = await pdfText(file); if (wantImages && text.replace(/\s+/g, "").length < 600) images = await pdfImages(file); }
  else if (n.endsWith(".pptx")) text = await pptxText(file);
  else if (n.endsWith(".docx")) text = await docxText(file);
  else if (/\.xlsx?$/.test(n)) text = (await xlsxRows(file)).map((r) => r.filter((c) => String(c).trim()).join(" | ")).filter(Boolean).join("\n");
  else if (/\.(txt|md|csv)$/.test(n)) text = await readFileText(file);
  else if (/\.(png|jpe?g|webp)$/.test(n)) images = [file];
  else throw Object.assign(new Error("unsupported"), { code: "unsupported" });
  return { text: text.replace(/[ \t]{2,}/g, " ").replace(/\n{3,}/g, "\n\n").trim(), images };
}
async function xlsxRows(file) {
  await loadScript("https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js");
  const wb = window.XLSX.read(await readFileBuf(file), { type: "array", cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  return window.XLSX.utils.sheet_to_json(ws, { header: 1, raw: false, defval: "" });
}
function parseCSV(text) {
  const rows = []; let row = [], f = "", q = false;
  const delim = (() => { const l = text.split(/\r?\n/).find((x) => x.trim()) || ""; const c = (l.match(/,/g) || []).length, s = (l.match(/;/g) || []).length, t = (l.match(/\t/g) || []).length; return t > c && t > s ? "\t" : s > c ? ";" : ","; })();
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (q) { if (ch === '"') { if (text[i + 1] === '"') { f += '"'; i++; } else q = false; } else f += ch; }
    else if (ch === '"') q = true;
    else if (ch === delim) { row.push(f); f = ""; }
    else if (ch === "\n" || ch === "\r") { if (ch === "\r" && text[i + 1] === "\n") i++; row.push(f); rows.push(row); row = []; f = ""; }
    else f += ch;
  }
  if (f !== "" || row.length) { row.push(f); rows.push(row); }
  return rows.filter((r) => r.some((c) => String(c).trim() !== ""));
}
function parseVCF(text) {
  const unfolded = text.replace(/\r?\n[ \t]/g, "");
  const cards = unfolded.split(/BEGIN:VCARD/i).slice(1);
  return cards.map((card) => {
    const o = {}; const lines = card.split(/\r?\n/);
    for (const line of lines) {
      const m = line.match(/^([^:;]+)([^:]*):(.*)$/); if (!m) continue;
      const key = m[1].toUpperCase().replace(/^ITEM\d+\./, ""); const val = m[3].replace(/\\n/g, "\n").replace(/\\,/g, ",").replace(/\\;/g, ";").trim();
      if (key === "FN") o.fullName = val;
      else if (key === "N") { const [ln, fn] = val.split(";"); o.lastName = ln; o.firstName = fn; }
      else if (key === "ORG") o.organisation = val.split(";")[0];
      else if (key === "TITLE") o.title = val;
      else if (key === "EMAIL" && !o.email) o.email = val;
      else if (key === "TEL" && !o.phone) o.phone = val;
      else if (key === "ADR" && !o.location) { const p = val.split(";"); o.location = [p[3], p[6]].filter(Boolean).join(", "); }
      else if (key === "NOTE") o.notes = val;
      else if (key === "URL" && /linkedin/i.test(val)) o.linkedin = val;
    }
    return o;
  }).filter((o) => o.fullName || o.firstName || o.email);
}

/* ---------- AI (Claude via sample) ---------- */
function aiErr(e) {
  const c = e && e.code;
  if (c === "not_granted") return "Claude access wasn't allowed for this page, so AI features are off for this visit.";
  if (c === "rate_limited") return "Claude is busy with other requests. Wait a moment, then try again.";
  if (c === "prompt_too_large") return "That's too much text for one request. Shorten the brief or import fewer rows at a time.";
  if (c === "invalid_json") return "Claude's answer couldn't be read. Try again.";
  if (c === "cancelled") return "Stopped.";
  if (c === "refused") return "Claude declined that request.";
  return "Claude couldn't complete that request. Try again shortly.";
}
/* =========================================================
   Entrance: access code → identity → PIN; sessions & idle lock
   ========================================================= */
const gate = { step: "loading", userId: null, pin: "", msg: "", busy: false, codeOk: false };

function drawGuilloche(canvas) {
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  const w = canvas.clientWidth, h = canvas.clientHeight; if (!w || !h) return;
  canvas.width = w * dpr; canvas.height = h * dpr;
  const ctx = canvas.getContext("2d"); ctx.scale(dpr, dpr);
  const cx = w / 2, cy = h * 0.42; const R = Math.min(w, h) * 0.44;
  ctx.lineWidth = 0.5;
  // Guilloche rosettes: many phase-shifted epitrochoid bands, the engraving of banknotes and share certificates.
  const bands = [[R, 0.22, 12, 0.055, 26], [R * 0.72, 0.3, 18, 0.05, 22], [R * 0.44, 0.35, 9, 0.06, 18]];
  for (const [r, k, lobes, alpha, strokes] of bands) {
    ctx.strokeStyle = `rgba(201,174,128,${alpha})`;
    for (let s = 0; s < strokes; s++) {
      const ph = (s / strokes) * (Math.PI * 2 / lobes);
      ctx.beginPath();
      for (let i = 0; i <= 1440; i++) {
        const t = (i / 1440) * Math.PI * 2;
        const rr = r * (1 - k + k * Math.cos(lobes * (t + ph))) + r * 0.03 * Math.cos(lobes * 3 * t);
        const x = cx + rr * Math.cos(t), y = cy + rr * Math.sin(t);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      }
      ctx.stroke();
    }
  }
}

function gateHTML() {
  const brand = `<div class="wordmark">U<i>Connect</i></div><div class="gate-tag">Private capital, well connected.</div><div class="gate-rule"></div>`;
  const foot = `<div class="gate-foot">Every entry, exit and failed attempt is recorded.</div>`;
  const lock = lockoutRemaining();
  switch (gate.step) {
    case "loading":
      return `${brand}<div class="row" style="justify-content:center;gap:12px"><span class="spin"></span><span class="muted">Establishing a secure session…</span></div>`;
    case "nodb": {
      if (!window.UC_STANDALONE) return `${brand}<p class="dim" style="max-width:340px">UConnect needs its private database. Open it from your Claude workspace link rather than a saved copy.</p>`;
      const be = gate.bootErr || {};
      const why = be.code === "not_configured"
        ? "No database is connected yet. In Vercel, open this project, go to Storage, create a Neon Postgres database and connect it, then redeploy the latest deployment."
        : be.code === "db_unreachable"
        ? "The database is connected but not answering. Check it is running in Vercel under Storage, then redeploy."
        : be.message && be.code !== "unavailable"
          ? esc(be.message)
          : "UConnect can't reach its server right now. Check your connection and reload.";
      return `${brand}<p class="dim" style="max-width:360px">${why}</p>${be.status ? `<p class="dim" style="font-size:11px;opacity:.6">Server said: ${be.status} ${esc(be.code || "")}</p>` : ""}<button class="btn" data-act="reload">Try again</button>`;
    }
    case "uninit":
      return `${brand}<p class="dim" style="max-width:340px">${window.UC_STANDALONE ? "No access code has been set on the server yet. Add the ACCESS_CODE setting to the server, restart it, then reload this page." : "This workspace hasn't been initialised yet. Ask Claude to seed the access settings, then this page will open automatically."}</p>`;
    case "code":
      return `${brand}<form class="gate-form" id="g-code-form" autocomplete="off">
        <label class="gate-label" for="g-code">Access code</label>
        <input class="code-input" id="g-code" type="password" autocomplete="off" spellcheck="false" autocapitalize="off" ${lock ? "disabled" : "autofocus"}>
        <div class="gate-msg ${gate.msg && !lock ? "" : "ok"}" id="g-msg">${lock ? `Too many attempts. Try again in ${Math.ceil(lock / 60000)} min.` : esc(gate.msg)}</div>
        <button class="btn primary" type="submit" ${lock || gate.busy ? "disabled" : ""}>${gate.busy ? '<span class="spin"></span>' : "Enter"}</button>
      </form>${foot}`;
    case "setup":
      return `${brand}<form class="gate-form" id="g-setup" autocomplete="off" style="text-align:left">
        <p class="dim" style="text-align:center;margin:0">First entry. Create the super admin accounts. Each person chooses their own PIN of at least 6 digits.</p>
        <div class="field"><label for="su-n1">Super admin 1 · name</label><input class="input" id="su-n1" value="Ada" required></div>
        <div class="fgrid"><div class="field"><label for="su-p1">PIN</label><input class="input" id="su-p1" type="password" inputmode="numeric" minlength="6" maxlength="12" required></div><div class="field"><label for="su-c1">Confirm PIN</label><input class="input" id="su-c1" type="password" inputmode="numeric" maxlength="12" required></div></div>
        <div class="divider"></div>
        <div class="field"><label for="su-n2">Super admin 2 · name <span class="muted" style="text-transform:none;letter-spacing:0">(the principal · optional now)</span></label><input class="input" id="su-n2" placeholder="Full name"></div>
        <div class="fgrid"><div class="field"><label for="su-p2">PIN</label><input class="input" id="su-p2" type="password" inputmode="numeric" maxlength="12"></div><div class="field"><label for="su-c2">Confirm PIN</label><input class="input" id="su-c2" type="password" inputmode="numeric" maxlength="12"></div></div>
        <div class="gate-msg" id="g-msg">${esc(gate.msg)}</div>
        <button class="btn primary" type="submit">Create accounts &amp; enter</button>
      </form>`;
    case "who": {
      const users = [...S.users.values()].filter((u) => u.active !== false).sort((a, b) => a.name.localeCompare(b.name));
      return `${brand}<div class="gate-label">Who is signing in?</div>
        <div class="who-grid">${users.map((u) => `<button class="who" data-act="gate-who" data-id="${u.id}"><span class="av">${esc(initials(u.name))}</span><span>${esc(u.name)}</span><span class="hint">${u.role === "superadmin" ? "Super admin" : "Member"}</span></button>`).join("")}</div>
        <button class="btn ghost sm" data-act="gate-back">Use a different code</button>${foot}`;
    }
    case "pin": {
      const u = S.users.get(gate.userId);
      return `${brand}<div class="stack" style="align-items:center;gap:10px"><span class="av" style="width:48px;height:48px;font-size:16px">${esc(initials(u?.name))}</span><div>${esc(u?.name || "")}</div></div>
        <div class="gate-label">Enter your PIN</div>
        <div class="pin-dots" id="g-dots">${Array.from({ length: Math.max(6, gate.pin.length) }, (_, i) => `<span class="${i < gate.pin.length ? "on" : ""}"></span>`).join("")}</div>
        <div class="gate-msg" id="g-msg">${lock ? `Too many attempts. Try again in ${Math.ceil(lock / 60000)} min.` : esc(gate.msg)}</div>
        <div class="pinpad">${[1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => `<button data-act="pin-key" data-k="${n}" ${lock ? "disabled" : ""}>${n}</button>`).join("")}<button class="ghost" data-act="gate-back-who">Back</button><button data-act="pin-key" data-k="0" ${lock ? "disabled" : ""}>0</button><button class="ghost" data-act="pin-key" data-k="enter" ${lock ? "disabled" : ""}>Enter</button></div>${foot}`;
    }
  }
  return brand;
}
function renderGate(shake) {
  const g = $("#gate"); g.hidden = false; $("#app").hidden = true;
  if (!g.querySelector("canvas")) { g.innerHTML = `<canvas aria-hidden="true"></canvas><div class="gate-card fade-in"></div>`; requestAnimationFrame(() => drawGuilloche(g.querySelector("canvas"))); }
  const card = g.querySelector(".gate-card"); card.innerHTML = gateHTML();
  if (shake) { card.classList.remove("shake"); void card.offsetWidth; card.classList.add("shake"); }
  const f = card.querySelector("#g-code-form"); if (f) f.onsubmit = (e) => { e.preventDefault(); submitCode(); };
  const su = card.querySelector("#g-setup"); if (su) su.onsubmit = (e) => { e.preventDefault(); submitSetup(); };
  const ai = card.querySelector("[autofocus]"); if (ai) ai.focus();
}

/* Gate actions */
function pinKey(k) {
  if (k === "enter") { if (gate.pin.length >= 4) submitPin(); return; }
  if (k === "back") { gate.pin = gate.pin.slice(0, -1); }
  else if (gate.pin.length < 12) gate.pin += k;
  gate.msg = ""; const dots = $("#g-dots");
  if (dots) dots.innerHTML = Array.from({ length: Math.max(6, gate.pin.length) }, (_, i) => `<span class="${i < gate.pin.length ? "on" : ""}"></span>`).join("");
  const m = $("#g-msg"); if (m) m.textContent = "";
}
document.addEventListener("keydown", (e) => {
  if (S.me || gate.step !== "pin") return;
  if (/^\d$/.test(e.key)) { e.preventDefault(); pinKey(e.key); }
  else if (e.key === "Backspace") { e.preventDefault(); pinKey("back"); }
  else if (e.key === "Enter") { e.preventDefault(); pinKey("enter"); }
  else if (e.key === "Escape") { gate.step = "who"; gate.pin = ""; renderGate(); }
});

/* Presence: who is online right now */
function initPresence() {
  if (!S.room) return;
  if (S._presenceInit) { updatePresence(); return; }
  S._presenceInit = true;
  try {
    S.room.onPeers(({ peers }) => { S.peers = peers.filter((p) => p.kind === "viewer" && p.presence && p.presence.uid); renderPresence(); });
    updatePresence();
  } catch (e) { console.warn(e); }
}
function updatePresence() { if (S.room && S.me) S.room.presence({ uid: S.me.id, name: S.me.name, view: S.route.name, since: S.session?.start || now() }).catch(() => {}); }
/* =========================================================
   Shell: navigation, routing, render loop, global search, alerts
   ========================================================= */
const NAV = [
  { k: "overview", label: "Overview", icon: "overview" },
  { k: "contacts", label: "Contacts", icon: "contacts" },
  { k: "match", label: "AI Matcher", icon: "match" },
  { k: "deals", label: "Deals", icon: "deals" },
  { sep: "Relationships" },
  { k: "intros", label: "Intros & Fees", icon: "intros" },
  { k: "tasks", label: "Tasks", icon: "tasks" },
  { k: "vault", label: "Vault", icon: "vault" },
  { sep: "Workspace" },
  { k: "security", label: "Security", icon: "security", admin: true },
  { k: "settings", label: "Settings", icon: "settings" },
];
const TITLES = { overview: "Overview", contacts: "Contacts", match: "AI Matcher", deals: "Deals", deal: "Deal", intros: "Intros & Fees", tasks: "Tasks", vault: "Vault", security: "Security", settings: "Settings" };
const isAdmin = () => S.me && S.me.role === "superadmin";

function enterApp() {
  $("#gate").hidden = true; $("#app").hidden = false;
  subscribeData(); initPresence();
  S.discreet = LS.get("discreet", false); document.body.classList.toggle("discreet", S.discreet);
  renderShell(); parseRoute(); render();
}
function renderShell() {
  $("#app").innerHTML = `
  <aside class="rail">
    <div class="rail-brand"><a href="#/overview" class="wordmark" style="text-decoration:none">U<i>Connect</i></a></div>
    <nav class="nav" id="nav"></nav>
    <div class="rail-foot">
      <div class="me"><span class="av">${esc(initials(S.me.name))}</span><div style="min-width:0"><div class="who-name">${esc(S.me.name)}</div><div class="who-role">${isAdmin() ? "Super admin" : "Member"}</div></div></div>
      <div class="row" style="gap:6px">
        <button class="btn sm ghost" data-act="discreet" id="discreet-btn" title="Discreet mode masks amounts and contact details (hover to reveal)">${icon(S.discreet ? "eyeoff" : "eye")}<span>${S.discreet ? "Discreet on" : "Discreet"}</span></button>
        <button class="btn sm ghost" data-act="lock" title="Lock screen">${icon("lock")}</button>
        <button class="btn sm ghost" data-act="signout" title="Sign out">${icon("logout")}</button>
      </div>
    </div>
  </aside>
  <header class="topbar">
    <div class="page-title" id="page-title"></div>
    <div class="grow"></div>
    <div class="gsearch"><svg viewBox="0 0 24 24">${IC.search}</svg><input id="gsearch" placeholder="Search people, firms, deals…" autocomplete="off" aria-label="Search"><kbd>/</kbd><div class="gresults" id="gresults" hidden></div></div>
    <div class="presence" id="presence"></div>
    <button class="iconbtn" data-act="alerts" aria-label="Alerts" id="alerts-btn">${icon("bell")}</button>
    <button class="btn primary sm hide-m" data-act="new-menu">${icon("plus")}New</button>
    <button class="iconbtn show-m" data-act="m-menu" aria-label="Menu">${icon("more")}</button>
  </header>
  <main id="view"></main>
  <nav class="tabbar" id="tabbar"></nav>`;
  const gs = $("#gsearch");
  gs.addEventListener("input", debounce(globalSearch, 120));
  gs.addEventListener("focus", globalSearch);
  gs.addEventListener("keydown", (e) => {
    const items = $$("#gresults a"); let i = items.findIndex((a) => a.classList.contains("sel"));
    if (e.key === "ArrowDown") { e.preventDefault(); i = Math.min(items.length - 1, i + 1); }
    else if (e.key === "ArrowUp") { e.preventDefault(); i = Math.max(0, i - 1); }
    else if (e.key === "Enter") { e.preventDefault(); (items[i] || items[0])?.click(); return; }
    else if (e.key === "Escape") { gs.blur(); $("#gresults").hidden = true; return; }
    else return;
    items.forEach((a, j) => a.classList.toggle("sel", j === i));
  });
  gs.addEventListener("blur", () => setTimeout(() => ($("#gresults").hidden = true), 180));
}
function renderNav() {
  const alerts = computeAlerts();
  const openTasks = [...S.tasks.values()].filter((t) => !t.done && t.due && t.due <= todayISO()).length;
  const route = S.route.name === "deal" ? "deals" : S.route.name;
  $("#nav").innerHTML = NAV.filter((n) => !n.admin || isAdmin()).map((n) => n.sep ? `<div class="eyebrow nav-sep">${n.sep}</div>` : `<a href="#/${n.k}" class="${route === n.k ? "on" : ""}">${icon(n.icon)}<span>${n.label}</span>${n.k === "tasks" && openTasks ? `<span class="badge crit">${openTasks}</span>` : ""}</a>`).join("");
  $("#tabbar").innerHTML = ["overview", "contacts", "match", "deals", "tasks"].map((k) => { const n = NAV.find((x) => x.k === k); return `<a href="#/${k}" class="${route === k ? "on" : ""}">${icon(n.icon)}<span>${n.label.replace("AI ", "")}</span></a>`; }).join("");
  const crit = alerts.filter((a) => a.sev === "crit").length + alerts.filter((a) => a.sev === "warn").length;
  $("#alerts-btn").innerHTML = icon("bell") + (crit ? `<span class="dot">${crit > 99 ? "99+" : crit}</span>` : "");
  $("#page-title").textContent = S.route.name === "deal" ? (S.deals.get(S.route.id)?.name || "Deal") : TITLES[S.route.name] || "";
  renderPresence();
}
function renderPresence() {
  const el = $("#presence"); if (!el) return;
  const byUser = new Map(); for (const p of S.peers) if (!byUser.has(p.presence.uid)) byUser.set(p.presence.uid, p.presence);
  if (!byUser.size) { el.innerHTML = ""; return; }
  el.innerHTML = `<span class="live" title="Live"></span>` + [...byUser.values()].slice(0, 4).map((p) => `<span class="av" title="${esc(p.name)} · on ${esc(TITLES[p.view] || p.view || "")}">${esc(initials(p.name))}</span>`).join("");
}

/* Routing */
function parseRoute() {
  const h = location.hash.replace(/^#\/?/, "").split("/");
  let name = h[0] || "overview"; const id = h[1] ? decodeURIComponent(h[1]) : null;
  if (name === "deals" && id) name = "deal";
  if (!TITLES[name] || (name === "security" && !isAdmin())) name = "overview";
  S.route = { name, id };
  if (name === "contacts" && id) { S.route = { name: "contacts", id: null }; openDrawer("contact", id); }
}
window.addEventListener("hashchange", () => { if (!S.me) return; if (drawerState) closeDrawer(); parseRoute(); closeMenus(); render(true); updatePresence(); });
function go(hash) {
  const m = hash.match(/^#\/contacts\/(.+)$/); if (m) { openDrawer("contact", decodeURIComponent(m[1])); return; }
  if (location.hash === hash) render(true); else location.hash = hash;
}

/* Render loop */
let renderPending = false;
function scheduleRender() { if (!S.me || renderPending) return; renderPending = true; requestAnimationFrame(() => { renderPending = false; render(); }); }
function render(resetScroll) {
  if (!S.me) return;
  const view = $("#view"); if (!view) return;
  const act = document.activeElement; const actId = act && act.id && view.contains(act) ? act.id : null;
  const selS = actId && "selectionStart" in act ? act.selectionStart : null;
  const scroll = window.scrollY;
  const V = VIEWS[S.route.name] || VIEWS.overview;
  const dirty = resetScroll ? [] : captureDirty(view, "form input, form textarea, form select");
  try { view.innerHTML = V(); } catch (e) { console.error(e); view.innerHTML = `<div class="empty"><div class="h-section">Something went wrong drawing this page.</div><div class="hint">${esc(e.message)}</div></div>`; }
  renderNav(); restoreDirty(dirty);
  if (actId) { const el = document.getElementById(actId); if (el) { el.focus({ preventScroll: true }); if (selS !== null && el.setSelectionRange) try { el.setSelectionRange(selS, selS); } catch {} } }
  if (resetScroll) window.scrollTo(0, 0); else window.scrollTo(0, scroll);
  if (drawerState && !drawerState.editing) renderDrawer();
  afterRender();
}
const afterHooks = [];
function afterRender() { afterHooks.forEach((f) => { try { f(); } catch (e) { console.warn(e); } }); }

/* Global search */
function globalSearch() {
  const q = norm($("#gsearch").value); const box = $("#gresults");
  if (!q) { box.hidden = true; return; }
  const res = [];
  for (const c of S.contacts.values()) { const hay = norm([contactName(c), c.organisation, c.email, c.city, c.country, (c.tags || []).join(" ")].join(" ")); if (hay.includes(q)) res.push({ k: "c", c, score: norm(contactName(c)).startsWith(q) ? 2 : 1 }); if (res.length > 60) break; }
  for (const d of S.deals.values()) if (norm([d.name, d.sponsor, d.assetClass].join(" ")).includes(q)) res.push({ k: "d", d, score: 3 });
  for (const t of S.tasks.values()) if (!t.done && norm(t.title).includes(q)) res.push({ k: "t", t, score: 0 });
  res.sort((a, b) => b.score - a.score);
  box.innerHTML = res.slice(0, 12).map((r) => r.k === "c"
    ? `<a href="#/contacts/${encodeURIComponent(r.c.id)}"><span class="av" style="width:26px;height:26px;font-size:10px">${esc(initials(contactName(r.c)))}</span><span style="min-width:0"><div>${esc(contactName(r.c))}</div><div class="hint">${esc([r.c.organisation, r.c.contactType].filter(Boolean).join(" · "))}</div></span></a>`
    : r.k === "d" ? `<a href="#/deals/${encodeURIComponent(r.d.id)}">${icon("deals")}<span><div>${esc(r.d.name)}</div><div class="hint">Deal · ${esc(r.d.assetClass || "")}</div></span></a>`
    : `<a href="#/tasks">${icon("tasks")}<span><div>${esc(r.t.title)}</div><div class="hint">Task · due ${esc(fmtDate(r.t.due) || "—")}</div></span></a>`).join("") || `<div class="hint" style="padding:10px">No matches for “${esc($("#gsearch").value)}”.</div>`;
  box.hidden = false;
}

/* Alerts: what needs attention */
function computeAlerts() {
  const out = []; const today = todayISO(); const stall = S.settings.stallDays || 21;
  for (const t of S.tasks.values()) {
    if (t.done || !t.due) continue;
    if (t.due < today) out.push({ tag: "Overdue", sev: "crit", t1: `Overdue: ${t.title}`, t2: `Was due ${fmtDate(t.due)}${t.assignee ? " · " + userName(t.assignee) : ""}`, go: "#/tasks", sort: 0 });
    else if (t.due === today) out.push({ tag: "Today", sev: "info", t1: `Due today: ${t.title}`, t2: t.assignee ? userName(t.assignee) : "Unassigned", go: "#/tasks", sort: 3 });
  }
  for (const { deal, cid, e, c } of allEngagements()) {
    if (!c || deal.status !== "Live" || !ACTIVE_STAGES.includes(e.stage)) continue;
    const d = daysSince(e.stageAt || e.addedAt);
    if (e.nextDate && e.nextDate < today) out.push({ tag: "Next step", sev: "warn", t1: `Next step overdue · ${contactName(c)}`, t2: `${deal.name} · ${e.nextStep || "Follow up"} (due ${fmtDate(e.nextDate)})`, go: `#/deals/${deal.id}`, sort: 1 });
    else if (d > stall && e.stage !== "Committed") out.push({ tag: "Stalled", sev: "warn", t1: `Stalled ${d} days in ${e.stage} · ${contactName(c)}`, t2: deal.name, go: `#/deals/${deal.id}`, sort: 2 });
  }
  for (const c of S.contacts.values()) {
    if (c.archived || c.doNotContact || !(c.tier === "A" || c.tier === "B")) continue;
    if (isCold(c)) { const lt = lastTouch(c); out.push({ tag: "Going cold", sev: c.tier === "A" ? "warn" : "info", t1: `Going cold · ${contactName(c)}`, t2: `Tier ${c.tier} · last touch ${lt ? ago(lt) : "never recorded"}`, go: `#/contacts/${c.id}`, sort: c.tier === "A" ? 2 : 4 }); }
    if (c.nextFollowUp && c.nextFollowUp <= today) out.push({ tag: "Follow-up", sev: c.nextFollowUp < today ? "warn" : "info", t1: `Follow up with ${contactName(c)}`, t2: `Planned for ${fmtDate(c.nextFollowUp)}`, go: `#/contacts/${c.id}`, sort: 1 });
  }
  return out.sort((a, b) => a.sort - b.sort);
}
function alertsHTML(list, max = 8) {
  if (!list.length) return `<div class="empty" style="padding:28px"><div>All clear.</div><div class="hint">No overdue tasks, stalled investors or cold priority relationships.</div></div>`;
  return `<div class="alerts">${list.slice(0, max).map((a) => `<div class="alert" data-act="go" data-href="${esc(a.go)}"><span class="sev ${a.sev}"></span><div style="min-width:0"><div class="t1">${esc(a.t1)}</div><div class="t2">${esc(a.t2)}</div></div><span class="chip ${a.sev === "crit" ? "crit" : a.sev === "warn" ? "warn" : "info"}">${esc(a.tag || "")}</span></div>`).join("")}</div>${list.length > max ? `<div class="hint" style="padding:10px 20px">+ ${list.length - max} more</div>` : ""}`;
}

/* Event delegation */
const ACT = {};
document.addEventListener("click", (e) => {
  const a = e.target.closest('a[href^="#/contacts/"]'); if (!a || !S.me) return;
  e.preventDefault(); openDrawer("contact", decodeURIComponent(a.getAttribute("href").split("/")[2] || ""));
  $("#gresults") && ($("#gresults").hidden = true);
});
document.addEventListener("click", (e) => {
  const el = e.target.closest("[data-act]"); if (!el) return;
  const fn = ACT[el.dataset.act]; if (!fn) return;
  if (el.tagName === "A" && !el.getAttribute("href")) e.preventDefault();
  fn(el, e);
});
document.addEventListener("change", (e) => { const el = e.target.closest("[data-on]"); if (el && ACT[el.dataset.on]) ACT[el.dataset.on](el, e); });
document.addEventListener("input", (e) => { const el = e.target.closest("[data-in]"); if (el && ACT[el.dataset.in]) ACT[el.dataset.in](el, e); });
document.addEventListener("keydown", (e) => {
  if (!S.me) return;
  const typing = /INPUT|TEXTAREA|SELECT/.test(document.activeElement?.tagName || "");
  if (e.key === "/" && !typing) { e.preventDefault(); $("#gsearch")?.focus(); }
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); $("#gsearch")?.focus(); }
  if (e.key === "Escape") { if (modalStack.length) closeModal(); else if (drawerState) closeDrawer(); }
});

Object.assign(ACT, {
  reload: () => location.reload(),
  "gate-who": (el) => { gate.userId = el.dataset.id; gate.pin = ""; gate.msg = ""; gate.step = "pin"; renderGate(); },
  "gate-back": () => { gate.step = "code"; gate.codeOk = false; renderGate(); },
  "gate-back-who": () => { gate.step = "who"; gate.pin = ""; renderGate(); },
  "pin-key": (el) => pinKey(el.dataset.k),
  "modal-close": () => closeModal(),
  "drawer-close": () => closeDrawer(),
  go: (el) => { closeMenus(); go(el.dataset.href); },
  signout: async () => { if (await confirmBox("Sign out of UConnect?", "You'll need the access code and your PIN to return.", "Sign out")) signOut(); },
  lock: () => lockScreen("manual"),
  discreet: () => { S.discreet = !S.discreet; LS.set("discreet", S.discreet); document.body.classList.toggle("discreet", S.discreet); renderShell(); render(); toast(S.discreet ? "Discreet mode on. Hover to reveal a value." : "Discreet mode off."); },
  alerts: (el) => {
    const list = computeAlerts();
    const m = openModal(modalShell("Needs attention", alertsHTML(list, 40), "", `${plural(list.length, "item")}, most urgent first`));
    m.addEventListener("click", (e) => { if (e.target.closest("[data-act=go]")) closeModal(); });
  },
  "new-menu": (el) => showMenu(el, [
    { label: "Contact", icon: "contacts", run: () => contactForm() },
    { label: "Deal", icon: "deals", run: () => dealForm() },
    { label: "Deal from a document", icon: "sparkle", run: () => { dealForm(); setTimeout(() => $("#df-file")?.click(), 0); } },
    { label: "Task", icon: "tasks", run: () => taskForm() },
    { label: "Introduction", icon: "intros", run: () => introForm() },
    { label: "Log a touchpoint", icon: "phone", run: () => pickContact("Log a touchpoint with…", (c) => logForm(c.id)) },
    "-",
    { label: "Import contacts", icon: "upload", run: () => importWizard() },
  ]),
  "m-menu": (el) => showMenu(el, [
    { label: "New contact", icon: "plus", run: () => contactForm() },
    { label: "New deal", icon: "plus", run: () => dealForm() },
    { label: "New task", icon: "plus", run: () => taskForm() },
    "-",
    { label: "Intros & Fees", icon: "intros", run: () => go("#/intros") },
    { label: "Vault", icon: "vault", run: () => go("#/vault") },
    ...(isAdmin() ? [{ label: "Security", icon: "security", run: () => go("#/security") }] : []),
    { label: "Settings", icon: "settings", run: () => go("#/settings") },
    "-",
    { label: S.discreet ? "Discreet mode off" : "Discreet mode on", icon: "eye", run: () => ACT.discreet() },
    { label: "Lock screen", icon: "lock", run: () => lockScreen("manual") },
    { label: "Sign out", icon: "logout", run: () => ACT.signout() },
  ]),
});

/* Shared field builders */
function fSelect(id, label, opts, val, { blank = "—", cls = "", attrs = "" } = {}) {
  return `<div class="field ${cls}"><label for="${id}">${label}</label><select class="input" id="${id}" ${attrs}>${blank !== null ? `<option value="">${blank}</option>` : ""}${opts.map((o) => { const [v, l] = Array.isArray(o) ? o : [o, o]; return `<option value="${esc(v)}" ${String(val ?? "") === String(v) ? "selected" : ""}>${esc(l)}</option>`; }).join("")}</select></div>`;
}
function fInput(id, label, val, { type = "text", cls = "", ph = "", attrs = "" } = {}) {
  return `<div class="field ${cls}"><label for="${id}">${label}</label><input class="input" id="${id}" type="${type}" value="${esc(val ?? "")}" placeholder="${esc(ph)}" ${attrs}></div>`;
}
function fText(id, label, val, { cls = "", ph = "", rows = 3 } = {}) {
  return `<div class="field ${cls}"><label for="${id}">${label}</label><textarea class="input" id="${id}" rows="${rows}" placeholder="${esc(ph)}">${esc(val ?? "")}</textarea></div>`;
}
function fPick(id, label, opts, selected, cls = "") {
  const sel = new Set(selected || []);
  return `<div class="field ${cls}"><span class="flabel">${label}</span><div class="pick" id="${id}" data-pick>${opts.map((o) => `<button type="button" class="${sel.has(o) ? "on" : ""}" data-v="${esc(o)}" aria-pressed="${sel.has(o)}">${esc(o)}</button>`).join("")}</div></div>`;
}
document.addEventListener("click", (e) => {
  const b = e.target.closest("[data-pick] button"); if (!b) return;
  b.classList.toggle("on"); b.setAttribute("aria-pressed", b.classList.contains("on"));
  const box = b.closest("[data-pick]"); box.dispatchEvent(new CustomEvent("pickchange", { bubbles: true }));
});
const pickVal = (root, id) => $$(`#${id} button.on`, root).map((b) => b.dataset.v);
const val = (root, id) => { const el = $(`#${id}`, root); return el ? el.value.trim() : ""; };
const numVal = (root, id) => { const v = val(root, id); if (!v) return null; const p = parseMoneyRange(v); return p.min ?? (Number(v.replace(/[^\d.]/g, "")) || null); };
function userOpts() { return [...S.users.values()].filter((u) => u.active !== false).map((u) => [u.id, u.name]); }
function contactOpts() { return [...S.contacts.values()].filter((c) => !c.archived).sort((a, b) => contactName(a).localeCompare(contactName(b))).map((c) => [c.id, `${contactName(c)}${c.organisation ? " · " + c.organisation : ""}`]); }
function dealOpts(liveOnly) { return [...S.deals.values()].filter((d) => !liveOnly || d.status === "Live").sort((a, b) => (a.name || "").localeCompare(b.name || "")).map((d) => [d.id, d.name]); }

function pickContact(title, cb, { multi = false, exclude = [] } = {}) {
  const ex = new Set(exclude);
  const m = openModal(modalShell(title, `<div class="field" style="margin-top:14px"><input class="input" id="pc-q" placeholder="Search by name, firm, asset class or city" autofocus></div><div id="pc-list" class="stack" style="gap:0;margin-top:10px;max-height:52vh;overflow:auto"></div>`, multi ? `<span class="hint" id="pc-n" style="margin-right:auto">0 selected</span><button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="pc-ok">Add selected</button>` : ""), { size: "" });
  const chosen = new Set();
  const draw = () => {
    const q = norm(val(m, "pc-q"));
    const list = [...S.contacts.values()].filter((c) => !c.archived && !ex.has(c.id) && (!q || norm([contactName(c), c.organisation, (c.assetClasses || []).join(" "), c.city, c.country, c.contactType].join(" ")).includes(q))).sort((a, b) => contactName(a).localeCompare(contactName(b))).slice(0, 80);
    $("#pc-list", m).innerHTML = list.map((c) => `<label class="row" style="padding:10px 4px;border-bottom:1px solid var(--line);cursor:pointer;flex-wrap:nowrap" data-cid="${c.id}">${multi ? `<input type="checkbox" ${chosen.has(c.id) ? "checked" : ""} style="accent-color:var(--gold)">` : ""}<span class="av" style="width:28px;height:28px;font-size:10.5px">${esc(initials(contactName(c)))}</span><span style="min-width:0;flex:1"><div>${esc(contactName(c))}</div><div class="hint">${esc([c.organisation, (c.assetClasses || []).map((a) => ASSET_SHORT[a] || a).join("/"), c.city || c.country].filter(Boolean).join(" · "))}</div></span>${strengthDots(c.strength)}</label>`).join("") || `<div class="hint" style="padding:14px">No contacts match.</div>`;
  };
  draw();
  $("#pc-q", m).oninput = draw;
  $("#pc-list", m).onclick = (e) => {
    const row = e.target.closest("[data-cid]"); if (!row) return;
    const id = row.dataset.cid;
    if (!multi) { closeModal(); cb(S.contacts.get(id)); return; }
    if (e.target.tagName !== "INPUT") { e.preventDefault(); const cb2 = row.querySelector("input"); cb2.checked = !cb2.checked; }
    const on = row.querySelector("input").checked; on ? chosen.add(id) : chosen.delete(id);
    $("#pc-n", m).textContent = `${chosen.size} selected`;
  };
  if (multi) $("#pc-ok", m).onclick = () => { closeModal(); cb([...chosen].map((id) => S.contacts.get(id))); };
}
/* =========================================================
   Overview + Contacts
   ========================================================= */
const VIEWS = {};

function exampleBanner() {
  const n = [...S.contacts.values()].filter((c) => c.example).length + [...S.deals.values()].filter((d) => d.example).length;
  if (!n) return "";
  return `<div class="banner"><span class="chip ex">Example</span><span style="flex:1;min-width:200px">You're looking at example records so you can see how UConnect works. Import your real contacts, then remove these.</span>${isAdmin() ? `<button class="btn sm" data-act="clear-examples">Remove example data</button>` : ""}</div>`;
}
ACT["clear-examples"] = async () => {
  if (!(await confirmBox("Remove all example data?", "This deletes every record marked Example (contacts, deals, tasks and intros). Your own records are untouched.", "Remove examples", true))) return;
  const jobs = [];
  for (const [k, coll] of [["contacts", "contacts"], ["deals", "deals"], ["tasks", "tasks"], ["intros", "intros"]]) for (const r of S[k].values()) if (r.example) jobs.push(() => S.db.doc(`${coll}/${r.id}`).delete());
  for (const j of jobs) { try { await withRetry(j); } catch {} }
  audit("delete", "", "", `Removed ${jobs.length} example records`); toast(`Removed ${jobs.length} example records.`);
};

/* ---------------- Overview ---------------- */
VIEWS.overview = () => {
  const contacts = [...S.contacts.values()].filter((c) => !c.archived);
  const live = [...S.deals.values()].filter((d) => d.status === "Live");
  const eng = allEngagements().filter((x) => x.deal.status === "Live");
  const sumStage = (st) => eng.filter((x) => st.includes(x.e.stage)).reduce((s, x) => s + toGBP(st.includes("Committed") || st.includes("Closed") ? x.e.committed || x.e.indicated : x.e.indicated, x.e.currency || x.deal.currency), 0);
  const pipeline = sumStage(["In discussion", "Diligence"]);
  const committed = eng.filter((x) => x.e.stage === "Committed" || x.e.stage === "Closed").reduce((s, x) => s + toGBP(x.e.committed || x.e.indicated, x.e.currency || x.deal.currency), 0);
  const target = live.reduce((s, d) => s + toGBP(d.target, d.currency), 0);
  const fees = feeTotals();
  const warm = contacts.filter((c) => (c.strength || 0) >= 3).length;
  const hr = new Date().getHours(); const greet = hr < 12 ? "Good morning" : hr < 18 ? "Good afternoon" : "Good evening";
  const alerts = computeAlerts();
  const upcoming = [...S.tasks.values()].filter((t) => !t.done && t.due && t.due >= todayISO() && daysSince(t.due) >= -7).sort((a, b) => a.due.localeCompare(b.due)).slice(0, 6);
  const byStage = STAGES.map((st) => ({ l: st, v: eng.filter((x) => x.e.stage === st).reduce((s, x) => s + toGBP(st === "Committed" || st === "Closed" ? x.e.committed || x.e.indicated : x.e.indicated, x.e.currency || x.deal.currency), 0), n: eng.filter((x) => x.e.stage === st).length }));
  const byAsset = ASSET_KEYS.map((a) => ({ l: a, v: contacts.filter((c) => (c.assetClasses || []).includes(a)).length })).filter((x) => x.v).sort((a, b) => b.v - a.v);
  const byRegion = REGIONS.map((r) => ({ l: r, v: contacts.filter((c) => (c.regions || []).includes(r) || regionOf(c.country) === r).length })).filter((x) => x.v).sort((a, b) => b.v - a.v).slice(0, 8);
  const feed = auditEvents(12).filter((e) => e.a !== "security");
  const prev = S.prevLogin;
  return `${exampleBanner()}
  <div class="pagehead"><div><div class="eyebrow">${esc(new Date().toLocaleDateString("en-GB", { weekday: "long", day: "numeric", month: "long", year: "numeric" }))}</div><h1 class="h-display" style="margin-top:6px">${greet}, ${esc(S.me.name.split(" ")[0])}.</h1>
  <div class="sub">${prev ? `Your previous sign-in was ${esc(fmtTime(prev.at))} from ${esc(prev.dev || "an unknown device")}. Not you? Tell a super admin.` : "Welcome to UConnect."}</div></div>
  <div class="row"><button class="btn" data-act="import">${icon("upload")}Import</button><button class="btn primary" data-act="go" data-href="#/match">${icon("sparkle")}Match a deal</button></div></div>
  <div class="kpis">
    <button type="button" class="kpi kpi-link" data-act="kpi-go" data-k="contacts" title="Open Contacts"><span class="eyebrow">Relationships</span><span class="v num">${contacts.length.toLocaleString("en-GB")}</span><span class="s">${warm.toLocaleString("en-GB")} warm or stronger</span></button>
    <button type="button" class="kpi kpi-link" data-act="kpi-go" data-k="deals" title="Open live deals"><span class="eyebrow">Live deals</span><span class="v num">${live.length}</span><span class="s sens">${target ? money(target) + " combined target" : "No targets set"}</span></button>
    <button type="button" class="kpi kpi-link" data-act="kpi-go" data-k="pipeline" title="See every investor in discussion or diligence"><span class="eyebrow">Pipeline</span><span class="v num sens">${money(pipeline)}</span><span class="s">Indicated in discussion and diligence</span></button>
    <button type="button" class="kpi kpi-link" data-act="kpi-go" data-k="committed" title="See committed investors"><span class="eyebrow">Committed</span><span class="v num sens">${money(committed)}</span><span class="s sens">${target ? Math.round((committed / target) * 100) + "% of live targets" : "Across live deals"}</span></button>
    <button type="button" class="kpi kpi-link" data-act="kpi-go" data-k="fees" title="Open Intros & Fees"><span class="eyebrow">Fees receivable</span><span class="v num sens">${money(fees.expected)}</span><span class="s sens">${money(fees.received)} received</span></button>
  </div>
  <div class="grid g-main" style="margin-bottom:18px">
    <section class="panel"><div class="panel-h"><h2 class="h-card">Needs attention</h2><span class="badge ${alerts.some((a) => a.sev === "crit") ? "crit" : ""}">${alerts.length}</span></div>${alertsHTML(alerts, 7)}</section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Next 7 days</h2><button class="btn sm ghost" data-act="task-new">${icon("plus")}Task</button></div>
      ${upcoming.length ? `<div class="alerts">${upcoming.map((t) => `<div class="alert" data-act="task-edit" data-id="${t.id}"><span class="sev ${t.priority === "High" ? "warn" : "info"}"></span><div style="min-width:0"><div class="t1">${esc(t.title)}</div><div class="t2">${esc([t.contactId ? contactName(S.contacts.get(t.contactId)) : "", t.assignee ? userName(t.assignee) : ""].filter(Boolean).join(" · "))}</div></div><span class="hint">${t.due === todayISO() ? "Today" : esc(fmtDate(t.due, { weekday: "short", day: "numeric" }))}</span></div>`).join("")}</div>` : `<div class="empty" style="padding:28px"><div class="hint">Nothing scheduled this week.</div></div>`}
    </section>
  </div>
  <div class="grid g3" style="margin-bottom:18px">
    <section class="panel"><div class="panel-h"><h2 class="h-card">Capital by stage</h2><span class="hint">GBP equiv.</span></div><div class="panel-b">${barsHTML(byStage.map((x) => ({ l: x.l, v: x.v, t: `${money(x.v)} · ${plural(x.n, "investor")}`, lab: money(x.v) })), true)}</div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Network by asset class</h2><span class="hint">contacts</span></div><div class="panel-b">${byAsset.length ? barsHTML(byAsset.map((x) => ({ l: x.l, v: x.v, t: `${x.v} contacts`, lab: x.v }))) : `<div class="hint">Tag contacts with asset classes to see coverage.</div>`}</div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Network by geography</h2><span class="hint">contacts</span></div><div class="panel-b">${byRegion.length ? barsHTML(byRegion.map((x) => ({ l: x.l, v: x.v, t: `${x.v} contacts`, lab: x.v }))) : `<div class="hint">Add locations or regional focus to see coverage.</div>`}</div></section>
  </div>
  <section class="panel"><div class="panel-h"><h2 class="h-card">Live activity</h2><span class="hint">Updates from everyone, as they happen</span></div>
    ${feed.length ? `<div class="feed">${feed.map((e) => `<div class="feed-i"><span class="av">${esc(initials(e.n))}</span><div style="min-width:0"><div>${esc(e.s)}</div><div class="when">${esc(e.n)} · ${esc(ago(e.t))}</div></div></div>`).join("")}</div>` : `<div class="empty" style="padding:24px"><div class="hint">Activity will appear here as your team works.</div></div>`}
  </section>`;
};
ACT["kpi-go"] = (el) => {
  const k = el.dataset.k;
  if (k === "contacts") { Object.assign(S.ui.contacts, { q: "", type: "", asset: "", region: "", strength: "", tier: "", owner: "", tag: "", limit: 100 }); return go("#/contacts"); }
  if (k === "deals") { Object.assign(S.ui.deals, { view: "cards", status: "Live", q: "" }); return go("#/deals"); }
  if (k === "pipeline") { Object.assign(S.ui.deals, { view: "table", status: "Live", q: "", stage: "", stages: ["In discussion", "Diligence"] }); return go("#/deals"); }
  if (k === "committed") { Object.assign(S.ui.deals, { view: "table", status: "Live", q: "", stage: "Committed", stages: null }); return go("#/deals"); }
  if (k === "fees") return go("#/intros");
};
function barsHTML(items, sens) {
  const max = Math.max(1, ...items.map((i) => i.v));
  return `<div class="bars">${items.map((i) => `<div class="bar-row" title="${esc(i.l)}: ${esc(i.t)}"><span class="lab">${esc(i.l)}</span><div class="bar-track"><div class="bar-fill" style="width:${(i.v / max) * 100}%"></div></div><span class="val num ${sens ? "sens" : ""}">${esc(i.lab)}</span></div>`).join("")}</div>`;
}

/* ---------------- Contacts list ---------------- */
function filteredContacts() {
  const f = S.ui.contacts; const q = norm(f.q);
  let list = [...S.contacts.values()].filter((c) => (f.tag === "__archived" ? c.archived : !c.archived));
  if (q) list = list.filter((c) => norm([contactName(c), c.organisation, c.title, c.email, c.city, c.country, c.contactType, (c.assetClasses || []).join(" "), (c.subStrategies || []).join(" "), (c.sectors || []).join(" "), (c.tags || []).join(" "), c.notes].join(" ")).includes(q));
  if (f.type) list = list.filter((c) => c.contactType === f.type);
  if (f.asset) list = list.filter((c) => (c.assetClasses || []).includes(f.asset));
  if (f.region) list = list.filter((c) => (c.regions || []).includes(f.region) || regionOf(c.country) === f.region);
  if (f.strength) list = list.filter((c) => (c.strength || 0) >= +f.strength);
  if (f.tier) list = list.filter((c) => c.tier === f.tier);
  if (f.owner) list = list.filter((c) => c.owner === f.owner);
  if (f.tag && f.tag !== "__archived") list = f.tag === "__cold" ? list.filter(isCold) : f.tag === "__dnc" ? list.filter((c) => c.doNotContact) : list.filter((c) => (c.tags || []).includes(f.tag));
  const s = f.sort;
  const cmp = {
    name: (a, b) => contactName(a).localeCompare(contactName(b)),
    touch: (a, b) => lastTouch(b) - lastTouch(a),
    strength: (a, b) => (b.strength || 0) - (a.strength || 0),
    recent: (a, b) => (b.createdAt || 0) - (a.createdAt || 0),
    ticket: (a, b) => toGBP(b.ticketMax || b.ticketMin, b.ticketCurrency) - toGBP(a.ticketMax || a.ticketMin, a.ticketCurrency),
  }[s] || ((a, b) => 0);
  return list.sort(cmp);
}
VIEWS.contacts = () => {
  const f = S.ui.contacts; const list = filteredContacts(); const shown = list.slice(0, f.limit);
  const tags = uniq([...S.contacts.values()].flatMap((c) => c.tags || [])).sort();
  const total = [...S.contacts.values()].filter((c) => !c.archived).length;
  const sel = f.sel;
  const mandate = (c) => (c.assetClasses || []).slice(0, 3).map((a) => `<span class="chip">${esc(ASSET_SHORT[a] || a)}</span>`).join("") + ((c.assetClasses || []).length > 3 ? `<span class="chip">+${c.assetClasses.length - 3}</span>` : "");
  const geo = (c) => esc([c.city, c.country].filter(Boolean).join(", ") || (c.regions || []).join(", "));
  const touch = (c) => { const lt = lastTouch(c); const cold = isCold(c); return `<span class="${cold ? "gold" : "muted"}" title="${cold ? "Beyond the cadence for this tier" : ""}">${lt ? esc(ago(lt)) : "—"}</span>`; };
  return `${exampleBanner()}
  <div class="pagehead"><div><h1 class="h-display">Contacts</h1><div class="sub">${total.toLocaleString("en-GB")} relationships · ${list.length.toLocaleString("en-GB")} match the current filters</div></div>
    <div class="row"><button class="btn" data-act="export-contacts">${icon("download")}Export</button><button class="btn" data-act="import">${icon("upload")}Import</button><button class="btn primary" data-act="contact-new">${icon("plus")}Add contact</button></div></div>
  <div class="toolbar">
    <input class="input" id="c-q" style="max-width:280px" placeholder="Search name, firm, mandate, notes…" value="${esc(f.q)}" data-in="c-filter" data-k="q">
    <select class="input" id="c-type" style="width:auto" data-on="c-filter" data-k="type"><option value="">All types</option>${CONTACT_TYPES.map((t) => `<option ${f.type === t ? "selected" : ""}>${esc(t)}</option>`).join("")}</select>
    <select class="input" id="c-asset" style="width:auto" data-on="c-filter" data-k="asset"><option value="">All asset classes</option>${ASSET_KEYS.map((t) => `<option ${f.asset === t ? "selected" : ""}>${esc(t)}</option>`).join("")}</select>
    <select class="input" id="c-region" style="width:auto" data-on="c-filter" data-k="region"><option value="">All geographies</option>${REGIONS.map((t) => `<option ${f.region === t ? "selected" : ""}>${esc(t)}</option>`).join("")}</select>
    <select class="input" id="c-strength" style="width:auto" data-on="c-filter" data-k="strength"><option value="">Any strength</option>${[2, 3, 4, 5].map((n) => `<option value="${n}" ${String(f.strength) === String(n) ? "selected" : ""}>${STRENGTH[n]} or stronger</option>`).join("")}</select>
    <select class="input" id="c-tier" style="width:auto" data-on="c-filter" data-k="tier"><option value="">All tiers</option>${TIERS.map((t) => `<option value="${t}" ${f.tier === t ? "selected" : ""}>Tier ${t}</option>`).join("")}</select>
    <select class="input" id="c-owner" style="width:auto" data-on="c-filter" data-k="owner"><option value="">Any owner</option>${userOpts().map(([v, l]) => `<option value="${v}" ${f.owner === v ? "selected" : ""}>${esc(l)}</option>`).join("")}</select>
    <select class="input" id="c-tag" style="width:auto" data-on="c-filter" data-k="tag"><option value="">All tags</option><option value="__cold" ${f.tag === "__cold" ? "selected" : ""}>Going cold</option><option value="__dnc" ${f.tag === "__dnc" ? "selected" : ""}>Do not contact</option><option value="__archived" ${f.tag === "__archived" ? "selected" : ""}>Archived</option>${tags.map((t) => `<option ${f.tag === t ? "selected" : ""}>${esc(t)}</option>`).join("")}</select>
    <select class="input" id="c-sort" style="width:auto" data-on="c-filter" data-k="sort">${[["name", "Sort: Name"], ["touch", "Sort: Last touch"], ["strength", "Sort: Strength"], ["ticket", "Sort: Ticket size"], ["recent", "Sort: Recently added"]].map(([v, l]) => `<option value="${v}" ${f.sort === v ? "selected" : ""}>${l}</option>`).join("")}</select>
    ${Object.entries(f).some(([k, v]) => !["sort", "limit", "sel"].includes(k) && v) ? `<button class="btn ghost sm" data-act="c-clear">Clear filters</button>` : ""}
  </div>
  ${sel.size ? `<div class="banner" style="border-style:solid"><strong>${sel.size} selected</strong><span class="spacer"></span><button class="btn sm" data-act="bulk-deal">Add to deal</button><button class="btn sm" data-act="bulk-tag">Add tag</button><button class="btn sm" data-act="bulk-owner">Set owner</button><button class="btn sm" data-act="bulk-export">Export</button>${isAdmin() ? `<button class="btn sm danger" data-act="bulk-delete">Delete</button>` : ""}<button class="btn sm ghost" data-act="bulk-clear">Clear</button></div>` : ""}
  ${!total && !S.loaded.contacts ? `<div class="empty"><span class="spin"></span></div>` : !total ? `<div class="panel"><div class="empty"><div class="h-section">Bring the network in.</div><div style="max-width:460px">Upload LinkedIn's Connections.csv, phone or Outlook contacts (.vcf / .csv), spreadsheets, or paste scattered notes. UConnect tidies, de-duplicates and tags them.</div><div class="row"><button class="btn primary" data-act="import">${icon("upload")}Import contacts</button><button class="btn" data-act="contact-new">Add one by hand</button></div></div></div>` : `
  <div class="tablewrap resp"><table class="t"><thead><tr><th style="width:34px"><input type="checkbox" aria-label="Select all shown" data-on="c-selall" ${shown.length && shown.every((c) => sel.has(c.id)) ? "checked" : ""}></th><th>Name</th><th>Type</th><th>Mandate</th><th>Geography</th><th class="r">Ticket</th><th>Strength</th><th>Last touch</th><th>Owner</th></tr></thead><tbody>
  ${shown.map((c) => `<tr data-act="contact-open" data-id="${c.id}"><td data-stop><input type="checkbox" aria-label="Select" data-on="c-sel" data-id="${c.id}" ${sel.has(c.id) ? "checked" : ""}></td><td><div class="name">${esc(contactName(c))}${c.example ? ' <span class="chip ex">Example</span>' : ""}${c.doNotContact ? ' <span class="chip crit">DNC</span>' : ""}</div><div class="org">${esc([c.title, c.organisation].filter(Boolean).join(" · "))}</div></td><td class="dim">${esc(c.contactType || "—")}</td><td><div class="chips">${mandate(c) || '<span class="muted">—</span>'}</div></td><td class="dim">${geo(c) || "—"}</td><td class="r num sens">${esc(ticketText(c) || "—")}</td><td>${strengthDots(c.strength)}</td><td>${touch(c)}</td><td class="dim">${esc(userName(c.owner) || "—")}</td></tr>`).join("")}
  </tbody></table></div>
  <div class="cards">${shown.map((c) => `<div class="ccard" data-act="contact-open" data-id="${c.id}"><div class="row between"><div><div style="font-weight:500">${esc(contactName(c))}</div><div class="hint">${esc([c.title, c.organisation].filter(Boolean).join(" · "))}</div></div>${strengthDots(c.strength)}</div><div class="chips">${mandate(c)}${c.city || c.country ? `<span class="chip">${geo(c)}</span>` : ""}</div><div class="row between hint"><span class="sens">${esc(ticketText(c))}</span><span>Touched ${esc(ago(lastTouch(c)))}</span></div></div>`).join("")}</div>
  ${list.length > shown.length ? `<div class="row" style="justify-content:center;margin-top:16px"><button class="btn" data-act="c-more">Show ${Math.min(200, list.length - shown.length)} more of ${(list.length - shown.length).toLocaleString("en-GB")}</button></div>` : ""}`}`;
};
Object.assign(ACT, {
  "c-filter": (el) => { S.ui.contacts[el.dataset.k] = el.value; S.ui.contacts.limit = 100; render(); },
  "c-clear": () => { Object.assign(S.ui.contacts, { q: "", type: "", asset: "", region: "", strength: "", tier: "", owner: "", tag: "", limit: 100 }); render(); },
  "c-more": () => { S.ui.contacts.limit += 200; render(); },
  "c-sel": (el) => { const s = S.ui.contacts.sel; el.checked ? s.add(el.dataset.id) : s.delete(el.dataset.id); render(); },
  "c-selall": (el) => { const s = S.ui.contacts.sel; filteredContacts().slice(0, S.ui.contacts.limit).forEach((c) => (el.checked ? s.add(c.id) : s.delete(c.id))); render(); },
  "bulk-clear": () => { S.ui.contacts.sel.clear(); render(); },
  "contact-open": (el, e) => { if (e.target.closest("[data-stop]")) return; openDrawer("contact", el.dataset.id); },
  "contact-new": () => contactForm(),
  import: () => importWizard(),
  "export-contacts": () => exportContacts(filteredContacts(), "uconnect-contacts"),
  "bulk-export": () => exportContacts([...S.ui.contacts.sel].map((id) => S.contacts.get(id)).filter(Boolean), "uconnect-selected"),
  "bulk-tag": async () => {
    const m = openModal(modalShell("Add a tag", `<div class="field" style="margin-top:14px"><label for="bt-tag">Tag</label><input class="input" id="bt-tag" placeholder="e.g. Dubai trip Oct, Tech co-invest club" autofocus></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="bt-ok">Apply to ${S.ui.contacts.sel.size}</button>`), { size: "narrow" });
    $("#bt-ok", m).onclick = async () => { const t = val(m, "bt-tag"); if (!t) return; closeModal(); let n = 0; for (const id of S.ui.contacts.sel) { const c = S.contacts.get(id); if (!c) continue; await patch("contacts", id, { tags: uniq([...(c.tags || []), t]) }, false).catch(() => {}); n++; } audit("update", "contacts", "", `Tagged ${n} contacts “${t}”`); toast(`Tagged ${n} contacts.`); };
  },
  "bulk-owner": () => {
    const m = openModal(modalShell("Set relationship owner", fSelect("bo-u", "Owner", userOpts(), "", { blank: "No owner" }), `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="bo-ok">Apply</button>`), { size: "narrow" });
    $("#bo-ok", m).onclick = async () => { const u = val(m, "bo-u"); closeModal(); let n = 0; for (const id of S.ui.contacts.sel) { await patch("contacts", id, { owner: u }, false).catch(() => {}); n++; } audit("update", "contacts", "", `Set owner of ${n} contacts to ${userName(u) || "none"}`); toast("Owner updated."); };
  },
  "bulk-deal": () => {
    if (!S.deals.size) { toast("Create a deal first."); return dealForm(); }
    const m = openModal(modalShell("Add to a deal pipeline", fSelect("bd-d", "Deal", dealOpts(true), "", { blank: "Choose a live deal" }), `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="bd-ok">Add ${S.ui.contacts.sel.size} as Prospects</button>`), { size: "narrow" });
    $("#bd-ok", m).onclick = async () => { const d = val(m, "bd-d"); if (!d) return; closeModal(); await addToPipeline(d, [...S.ui.contacts.sel]); S.ui.contacts.sel.clear(); };
  },
  "bulk-delete": async () => {
    const n = S.ui.contacts.sel.size;
    if (!(await confirmBox(`Delete ${n} contacts permanently?`, "They'll be removed from every deal pipeline view. This can't be undone. Consider exporting first.", "Delete permanently", true))) return;
    if (!(await pinPrompt("Confirm with your PIN", "Permanent deletion needs your PIN."))) return;
    for (const id of S.ui.contacts.sel) await S.db.doc(`contacts/${id}`).delete().catch(() => {});
    audit("delete", "contacts", "", `Permanently deleted ${n} contacts`); S.ui.contacts.sel.clear(); toast(`Deleted ${n} contacts.`);
  },
});
function exportContacts(list, name) {
  const cols = [["First name", "firstName"], ["Last name", "lastName"], ["Full name", (c) => contactName(c)], ["Organisation", "organisation"], ["Title", "title"], ["Type", "contactType"], ["Email", "email"], ["Phone", "phone"], ["LinkedIn", "linkedin"], ["City", "city"], ["Country", "country"], ["Geographic focus", "regions"], ["Asset classes", "assetClasses"], ["Sub-strategies", "subStrategies"], ["Sectors", "sectors"], ["Structures", "structures"], ["Ticket min", "ticketMin"], ["Ticket max", "ticketMax"], ["Ticket currency", "ticketCurrency"], ["AUM", "aum"], ["AUM currency", "aumCurrency"], ["Lead preference", "leadPref"], ["Constraints", "constraints"], ["Strength", (c) => STRENGTH[c.strength] || ""], ["Tier", "tier"], ["Owner", (c) => userName(c.owner)], ["Introduced by", (c) => contactName(S.contacts.get(c.introducedBy)) || c.introducedByText || ""], ["Last touch", (c) => (lastTouch(c) ? new Date(lastTouch(c)).toISOString().slice(0, 10) : "")], ["Next follow-up", "nextFollowUp"], ["Preferred channel", "channel"], ["EA / gatekeeper", "gatekeeper"], ["Tags", "tags"], ["Source", "source"], ["Lawful basis", "lawfulBasis"], ["Do not contact", (c) => (c.doNotContact ? "Yes" : "")], ["Notes", "notes"]];
  saveFile(`${name}-${todayISO()}.csv`, toCSV(list, cols));
}

/* ---------------- Contact drawer ---------------- */
function renderDrawer() {
  const d = $("#drawer"); if (!drawerState) { d.innerHTML = ""; return; }
  if (drawerState.kind === "contact") {
    const c = S.contacts.get(drawerState.id);
    if (!c) { d.innerHTML = S.loaded.contacts ? "" : ""; if (S.loaded.contacts) { closeDrawer(); toast("That contact no longer exists."); } return; }
    const keepScroll = d.querySelector(".drawer-b")?.scrollTop || 0;
    const dirty = captureDirty(d, "input, textarea, select"); const act = document.activeElement && d.contains(document.activeElement) ? document.activeElement.id : null;
    d.innerHTML = `<div class="scrim" data-act="drawer-close"></div><aside class="drawer" role="dialog" aria-label="${esc(contactName(c))}">${contactDrawerHTML(c)}</aside>`;
    const b = d.querySelector(".drawer-b"); if (b) b.scrollTop = keepScroll;
    restoreDirty(dirty); if (act) document.getElementById(act)?.focus({ preventScroll: true });
    const f = d.querySelector("#lg-form"); if (f) f.onsubmit = (e) => { e.preventDefault(); quickLog(c.id); };
  }
}
function contactDrawerHTML(c) {
  const tab = S.ui.contactTab; const lt = lastTouch(c); const lt2 = localTime(c);
  const engs = engagementsOf(c.id);
  const intros = [...S.intros.values()].filter((i) => i.contactId === c.id || i.introducerId === c.id);
  const tasks = [...S.tasks.values()].filter((t) => t.contactId === c.id);
  const docs = [...S.docs.values()].filter((x) => x.contactId === c.id || (x.sentTo || {})[c.id]);
  const introducer = S.contacts.get(c.introducedBy);
  const kv = (pairs) => `<dl class="kv">${pairs.filter(([, v]) => v).map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("")}</dl>`;
  const chips = (a) => (a || []).length ? `<div class="chips">${a.map((x) => `<span class="chip">${esc(x)}</span>`).join("")}</div>` : "";
  const log = Object.entries(c.log || {}).filter(([, v]) => v).map(([k, v]) => ({ k, ...v })).sort((a, b) => toTime(b.date) - toTime(a.date) || (b.at || 0) - (a.at || 0));
  const history = auditEvents(2000).filter((e) => e.i === c.id);
  let body = "";
  if (tab === "profile") body = `
    <div class="stack" style="gap:26px">
      <section class="stack"><h3 class="fsec-title">Mandate</h3>${kv([
        ["Asset classes", chips((c.assetClasses || []))], ["Strategies", chips(c.subStrategies)], ["Sectors", chips(c.sectors)], ["Geographic focus", chips(c.regions)],
        ["Ticket size", ticketText(c) ? `<span class="sens">${esc(ticketText(c))}</span>` : ""], ["AUM / wealth", c.aum ? `<span class="sens">${esc(money(c.aum, c.aumCurrency || "GBP"))}</span>` : ""],
        ["Structures", chips(c.structures)], ["Lead preference", esc(c.leadPref || "")], ["Ownership", esc(c.ownership && c.ownership !== "Either" ? c.ownership : "")], ["Constraints", chips(c.constraints)], ["Mandate notes", esc(c.mandateNotes || "")]]) || `<div class="hint">No mandate recorded yet. <a href="#" data-act="contact-edit" data-id="${c.id}">Add it</a> so the AI Matcher can find this investor.</div>`}</section>
      <section class="stack"><h3 class="fsec-title">Contact</h3>${kv([
        ["Email", c.email ? `<a class="sens" href="mailto:${esc(c.email)}">${esc(c.email)}</a>` : ""], ["Phone", c.phone ? `<a class="sens" href="tel:${esc(c.phone)}">${esc(c.phone)}</a>` : ""],
        ["LinkedIn", c.linkedin ? `<a href="${esc(/^https?:/.test(c.linkedin) ? c.linkedin : "https://" + c.linkedin)}" target="_blank" rel="noopener">Profile</a>` : ""],
        ["Based in", esc([c.city, c.country].filter(Boolean).join(", "))], ["Local time", lt2 ? `${esc(lt2)} <span class="muted">(${esc(tzOf(c).split("/").pop().replace(/_/g, " "))})</span>` : ""],
        ["Prefers", esc(c.channel || "")], ["EA / gatekeeper", esc(c.gatekeeper || "")], ["Languages", esc(c.languages || "")]]) || `<div class="hint">No contact details yet.</div>`}</section>
      <section class="stack"><h3 class="fsec-title">Relationship</h3>${kv([
        ["Owner", esc(userName(c.owner) || "Unassigned")], ["Strength", `${strengthDots(c.strength)} <span class="muted" style="margin-left:6px">${esc(STRENGTH[c.strength] || "Unrated")}</span>`], ["Tier", c.tier ? `Tier ${esc(c.tier)} · touch every ${S.settings.cadence?.[c.tier] || "—"} days` : ""],
        ["Introduced by", introducer ? `<a href="#/contacts/${introducer.id}">${esc(contactName(introducer))}</a>` : esc(c.introducedByText || "")], ["Last touch", lt ? `${esc(fmtDate(lt))} <span class="${isCold(c) ? "gold" : "muted"}">(${esc(ago(lt))})</span>` : "Never recorded"],
        ["Next follow-up", esc(fmtDate(c.nextFollowUp))], ["Tags", chips(c.tags)], ["Source", esc(c.source || "")], ["Notes", c.notes ? `<span style="white-space:pre-wrap">${esc(c.notes)}</span>` : ""]])}</section>
    </div>`;
  else if (tab === "deals") body = `
    <div class="stack" style="gap:22px">
      <section class="stack"><div class="row between"><h3 class="fsec-title">Deal involvement</h3><button class="btn sm" data-act="contact-add-deal" data-id="${c.id}">${icon("plus")}Add to deal</button></div>
      ${engs.length ? `<div class="tablewrap"><table class="t"><thead><tr><th>Deal</th><th>Stage</th><th class="r">Indicated</th><th class="r">Committed</th><th>Next step</th></tr></thead><tbody>${engs.map(({ deal, e }) => `<tr data-act="eng-open" data-deal="${deal.id}" data-cid="${c.id}"><td><div class="name">${esc(deal.name)}</div><div class="org">${esc(deal.status)}</div></td><td><span class="row" style="gap:8px;flex-wrap:nowrap"><span class="stage-dot" style="background:${STAGE_COLOR[e.stage]}"></span>${esc(e.stage)}</span><div class="org">${esc(ago(e.stageAt || e.addedAt))}</div></td><td class="r num sens">${e.indicated ? esc(money(e.indicated, e.currency || deal.currency)) : "—"}</td><td class="r num sens">${e.committed ? esc(money(e.committed, e.currency || deal.currency)) : "—"}</td><td class="dim">${esc(e.nextStep || "")}${e.nextDate ? `<div class="org">${esc(fmtDate(e.nextDate))}</div>` : ""}</td></tr>`).join("")}</tbody></table></div>` : `<div class="hint">Not in any deal pipeline yet.</div>`}</section>
      <section class="stack"><h3 class="fsec-title">Introductions & fees</h3>${intros.length ? `<div class="stack" style="gap:8px">${intros.map((i) => `<div class="row between" style="padding:10px 0;border-bottom:1px solid var(--line)"><div><div>${esc(introTitle(i))}</div><div class="hint">${esc(fmtDate(i.date))} · ${esc(i.agreement || "No agreement")}</div></div><span class="chip ${i.status === "Received" ? "good" : i.status === "Disputed" ? "crit" : ""}">${esc(i.status || "")}</span></div>`).join("")}</div>` : `<div class="hint">No introductions recorded.</div>`}</section>
      <section class="stack"><div class="row between"><h3 class="fsec-title">Tasks</h3><button class="btn sm" data-act="task-new" data-cid="${c.id}">${icon("plus")}Task</button></div>${tasks.length ? tasks.map((t) => `<label class="check" style="padding:6px 0"><input type="checkbox" data-on="task-toggle" data-id="${t.id}" ${t.done ? "checked" : ""}><span style="${t.done ? "text-decoration:line-through;opacity:.6" : ""}">${esc(t.title)}</span><span class="hint">${esc(fmtDate(t.due))}</span></label>`).join("") : `<div class="hint">No tasks.</div>`}</section>
      <section class="stack"><h3 class="fsec-title">Documents shared</h3>${docs.length ? docs.map((x) => `<div class="row" style="padding:6px 0">${icon("file", "")}<a href="${esc("/_blob/" + x.assetId)}" target="_blank" rel="noopener" data-act="doc-open" data-id="${x.id}">${esc(x.name)}</a><span class="hint">${esc(x.category || "")}${(x.sentTo || {})[c.id] ? " · sent " + esc(fmtDate(x.sentTo[c.id])) : ""}</span></div>`).join("") : `<div class="hint">Nothing recorded as sent.</div>`}</section>
    </div>`;
  else if (tab === "activity") body = `
    <form class="stack" id="lg-form" style="margin-bottom:24px"><div class="row" style="gap:8px">${Object.entries(ACT_TYPES).map(([k, l], i) => `<label class="check"><input type="radio" id="lg-type-${k}" name="lg-type" value="${k}" ${i === 0 ? "checked" : ""}> ${l}</label>`).join("")}</div>
      <textarea class="input" id="lg-text" rows="3" placeholder="What was discussed? Appetite, objections, timing, next step…"></textarea>
      <div class="row"><input class="input" type="date" id="lg-date" value="${todayISO()}" style="width:auto"><label class="check"><input type="checkbox" id="lg-fu"> Set a follow-up</label><input class="input" type="date" id="lg-fudate" style="width:auto"><span class="spacer"></span><button class="btn primary" type="submit">Log touchpoint</button></div></form>
    ${log.length ? `<div class="timeline">${log.map((l) => `<div class="tl"><span class="pt ${esc(l.type)}"></span><div><div class="meta">${esc(ACT_TYPES[l.type] || "Note")} · ${esc(fmtDate(l.date))} · ${esc(l.byName || userName(l.by))}</div><div class="body">${esc(l.summary)}</div></div></div>`).join("")}</div>` : `<div class="hint">No touchpoints logged yet. Logging a call or meeting updates “last touch” and keeps alerts accurate.</div>`}`;
  else if (tab === "compliance") body = `
    <div class="stack" style="gap:24px">
      <section class="stack"><h3 class="fsec-title">Data protection (UK GDPR)</h3>${kv([["Lawful basis", esc(c.lawfulBasis || "Not yet assessed")], ["Consent date", esc(fmtDate(c.consentDate))], ["Source", esc(c.source || "Not recorded")], ["Do not contact", c.doNotContact ? '<span class="chip crit">Yes</span>' : "No"], ["Record created", `${esc(fmtTime(c.createdAt || 0))} by ${esc(userName(c.createdBy) || "import")}`], ["Last updated", `${esc(fmtTime(c.updatedAt || 0))} by ${esc(userName(c.updatedBy) || "—")}`]])}
      <div class="row"><button class="btn sm" data-act="contact-sar" data-id="${c.id}">${icon("download")}Export this person's data</button>${isAdmin() ? `<button class="btn sm danger" data-act="contact-erase" data-id="${c.id}">Erase (right to be forgotten)</button>` : ""}</div>
      <div class="hint">Export answers a subject access request. Erase removes the person everywhere, including pipelines and intros, and leaves only an anonymous entry in the audit log.</div></section>
      <section class="stack"><h3 class="fsec-title">Change history</h3>${history.length ? `<div class="timeline">${history.slice(0, 60).map((e) => `<div class="tl"><span class="pt"></span><div><div class="meta">${esc(fmtTime(e.t))} · ${esc(e.n)}</div><div class="body">${esc(e.s)}</div></div></div>`).join("")}</div>` : `<div class="hint">No changes recorded in the last 60 days of history.</div>`}</section>
    </div>`;
  return `
  <div class="drawer-h">
    <div class="row between" style="align-items:flex-start;flex-wrap:nowrap">
      <div style="min-width:0"><div class="eyebrow">${esc(c.contactType || "Contact")}${c.tier ? " · Tier " + esc(c.tier) : ""}</div><h2 class="h-display" style="margin-top:6px;font-size:32px">${esc(contactName(c))}</h2><div class="dim" style="margin-top:4px">${esc([c.title, c.organisation].filter(Boolean).join(" · "))}</div>
      <div class="chips" style="margin-top:10px">${c.example ? '<span class="chip ex">Example</span>' : ""}${c.doNotContact ? '<span class="chip crit">Do not contact</span>' : ""}${(c.assetClasses || []).map((a) => `<span class="chip gold">${esc(a)}</span>`).join("")}${lt2 ? `<span class="chip">${icon("clock", "")} ${esc(lt2)} local</span>` : ""}</div></div>
      <button class="iconbtn" data-act="drawer-close" aria-label="Close">${icon("close")}</button>
    </div>
    <div class="row"><button class="btn sm primary" data-act="contact-log" data-id="${c.id}">${icon("phone")}Log touch</button><button class="btn sm" data-act="contact-edit" data-id="${c.id}">${icon("edit")}Edit</button><button class="btn sm" data-act="contact-add-deal" data-id="${c.id}">Add to deal</button><button class="btn sm" data-act="task-new" data-cid="${c.id}">Task</button><button class="btn sm ghost" data-act="contact-more" data-id="${c.id}">${icon("more")}</button></div>
    <div class="tabs" style="margin-bottom:0">${[["profile", "Profile"], ["deals", `Deals (${engs.length})`], ["activity", `Activity (${log.length})`], ["compliance", "Compliance"]].map(([k, l]) => `<button class="${tab === k ? "on" : ""}" data-act="contact-tab" data-k="${k}">${l}</button>`).join("")}</div>
  </div>
  <div class="drawer-b">${body}</div>`;
}
Object.assign(ACT, {
  "contact-tab": (el) => { S.ui.contactTab = el.dataset.k; renderDrawer(); },
  "contact-edit": (el, e) => { e && e.preventDefault(); contactForm(el.dataset.id); },
  "contact-log": (el) => { S.ui.contactTab = "activity"; renderDrawer(); setTimeout(() => $("#lg-text")?.focus(), 30); },
  "contact-add-deal": (el) => {
    const id = el.dataset.id; if (!S.deals.size) { toast("Create a deal first."); return dealForm(); }
    const m = openModal(modalShell("Add to a deal pipeline", fSelect("ad-d", "Deal", dealOpts(true), "", { blank: "Choose a live deal" }), `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="ad-ok">Add as Prospect</button>`), { size: "narrow" });
    $("#ad-ok", m).onclick = async () => { const d = val(m, "ad-d"); if (!d) return; closeModal(); await addToPipeline(d, [id]); };
  },
  "contact-more": (el) => {
    const c = S.contacts.get(el.dataset.id);
    showMenu(el, [
      { label: c.archived ? "Restore from archive" : "Archive", run: async () => { await patch("contacts", c.id, { archived: !c.archived }, `${c.archived ? "Restored" : "Archived"} ${contactName(c)}`); toast(c.archived ? "Restored." : "Archived. Find it under Tags → Archived."); } },
      { label: c.doNotContact ? "Clear do-not-contact" : "Mark do not contact", run: () => patch("contacts", c.id, { doNotContact: !c.doNotContact }, `${c.doNotContact ? "Cleared" : "Set"} do-not-contact for ${contactName(c)}`) },
      { label: "Find similar investors", run: () => { closeDrawer(); MATCH.prefill = { assetClass: (c.assetClasses || [])[0] || "", regions: c.regions || [], sectors: c.sectors || [], brief: "" }; go("#/match"); } },
      ...(isAdmin() ? ["-", { label: "Delete permanently", run: async () => { if (!(await confirmBox("Delete this contact?", `${esc(contactName(c))} will be removed permanently.`, "Delete", true))) return; await remove("contacts", c.id, `Deleted contact ${contactName(c)}`); closeDrawer(); } }] : []),
    ]);
  },
  "contact-sar": (el) => {
    const c = S.contacts.get(el.dataset.id);
    const pack = { exportedAt: new Date().toISOString(), exportedBy: S.me.name, contact: c, deals: engagementsOf(c.id).map(({ deal, e }) => ({ deal: deal.name, ...e })), introductions: [...S.intros.values()].filter((i) => i.contactId === c.id || i.introducerId === c.id), tasks: [...S.tasks.values()].filter((t) => t.contactId === c.id), documentsSent: [...S.docs.values()].filter((d) => (d.sentTo || {})[c.id]).map((d) => ({ name: d.name, sent: d.sentTo[c.id] })) };
    saveFile(`subject-access-${norm(contactName(c)).replace(/ /g, "-")}-${todayISO()}.json`, JSON.stringify(pack, null, 2));
  },
  "contact-erase": async (el) => {
    const c = S.contacts.get(el.dataset.id);
    if (!(await confirmBox("Erase this person completely?", `This removes ${esc(contactName(c))}, their activity, pipeline entries, intros and task links. The audit log keeps only “A contact was erased”. This can't be undone.`, "Erase", true))) return;
    if (!(await pinPrompt("Confirm erasure", "Enter your PIN to confirm the erasure."))) return;
    for (const d of S.deals.values()) if ((d.pipeline || {})[c.id]) await S.db.doc(`deals/${d.id}`).update({ pipeline: { [c.id]: null } }).catch(() => {});
    for (const i of S.intros.values()) if (i.contactId === c.id || i.introducerId === c.id) await S.db.doc(`intros/${i.id}`).delete().catch(() => {});
    for (const t of S.tasks.values()) if (t.contactId === c.id) await S.db.doc(`tasks/${t.id}`).update({ contactId: "" }).catch(() => {});
    for (const x of S.docs.values()) if ((x.sentTo || {})[c.id]) await S.db.doc(`docs/${x.id}`).update({ sentTo: { [c.id]: null } }).catch(() => {});
    await S.db.doc(`contacts/${c.id}`).delete().catch(() => {});
    audit("erase", "contacts", "", "A contact was erased under a right-to-erasure request"); closeDrawer(); toast("Erased.");
  },
});
async function quickLog(cid) {
  const text = val(document, "lg-text"); if (!text) { $("#lg-text").focus(); return; }
  const type = ($("input[name=lg-type]:checked") || {}).value || "note"; const date = val(document, "lg-date") || todayISO();
  const lid = uid("l"); const c = S.contacts.get(cid);
  const upd = { log: { [lid]: { type, date, summary: text.slice(0, 4000), by: S.me.id, byName: S.me.name, at: now() } } };
  if (toTime(date) > toTime(c.lastContacted)) upd.lastContacted = date;
  if ($("#lg-fu").checked && val(document, "lg-fudate")) upd.nextFollowUp = val(document, "lg-fudate");
  await patch("contacts", cid, upd, `Logged ${ACT_TYPES[type].toLowerCase()} with ${contactName(c)}`);
  toast("Touchpoint logged.");
}
function logForm(cid) { openDrawer("contact", cid); S.ui.contactTab = "activity"; renderDrawer(); setTimeout(() => $("#lg-text")?.focus(), 40); }

/* ---------------- Contact form ---------------- */
function contactForm(id, preset = {}) {
  const c = id ? { ...S.contacts.get(id) } : { strength: 2, ticketCurrency: "GBP", aumCurrency: "GBP", owner: S.me.id, lawfulBasis: "Legitimate interest", ...preset };
  const subs = uniq((c.assetClasses || []).flatMap((a) => ASSET_CLASSES[a] || []));
  const body = `
  <div class="fsec"><div class="fsec-title">Person</div><div class="fgrid">
    ${fInput("cf-first", "First name", c.firstName)}${fInput("cf-last", "Last name", c.lastName)}
    ${fInput("cf-org", "Organisation", c.organisation)}${fInput("cf-title", "Title / role", c.title)}
    ${fSelect("cf-type", "Investor type", CONTACT_TYPES, c.contactType)}${fSelect("cf-owner", "Relationship owner", userOpts(), c.owner, { blank: "Unassigned" })}
  </div></div>
  <div class="fsec"><div class="fsec-title">Mandate</div>
    <div id="cf-assets-wrap">${fPick("cf-assets", "Asset classes", ASSET_KEYS, c.assetClasses)}</div>
    <div id="cf-subs-wrap">${subs.length ? fPick("cf-subs", "Strategies", subs, c.subStrategies) : ""}</div>
    ${fPick("cf-regions", "Geographic focus (where they invest)", REGIONS, c.regions)}
    ${fPick("cf-sectors", "Sectors", SECTORS, c.sectors)}
    <div class="fgrid">
      ${fInput("cf-tmin", "Ticket from", c.ticketMin ? String(c.ticketMin) : "", { ph: "e.g. 2m" })}${fInput("cf-tmax", "Ticket to", c.ticketMax ? String(c.ticketMax) : "", { ph: "e.g. 10m" })}
      ${fSelect("cf-tcur", "Ticket currency", CURRENCIES, c.ticketCurrency || "GBP", { blank: null })}${fInput("cf-aum", "AUM / investable wealth", c.aum ? String(c.aum) : "", { ph: "e.g. 1.2bn" })}
      ${fSelect("cf-lead", "Lead preference", LEAD_PREFS, c.leadPref)}${fSelect("cf-own", "Ownership appetite", OWNERSHIP, c.ownership)}
    </div>
    ${fPick("cf-struct", "Preferred structures", STRUCTURES, c.structures)}
    ${fPick("cf-cons", "Constraints", CONSTRAINTS, c.constraints)}
    ${fText("cf-mnotes", "Mandate notes", c.mandateNotes, { ph: "Anything the matcher should know: return targets, hold periods, exclusions, recent allocations…", rows: 2 })}
  </div>
  <div class="fsec"><div class="fsec-title">Contact details</div><div class="fgrid">
    ${fInput("cf-email", "Email", c.email, { type: "email" })}${fInput("cf-phone", "Phone", c.phone, { type: "tel" })}
    ${fInput("cf-city", "City", c.city)}${fInput("cf-country", "Country", c.country, { attrs: 'list="dl-countries"' })}
    ${fInput("cf-li", "LinkedIn URL", c.linkedin)}${fSelect("cf-chan", "Preferred channel", CHANNELS, c.channel)}
    ${fInput("cf-ea", "EA / gatekeeper", c.gatekeeper, { ph: "Name, email or phone" })}${fInput("cf-lang", "Languages", c.languages, { ph: "e.g. English, Arabic" })}
  </div><datalist id="dl-countries">${Object.keys(COUNTRIES).map((k) => `<option value="${esc(k)}">`).join("")}</datalist></div>
  <div class="fsec"><div class="fsec-title">Relationship</div><div class="fgrid">
    ${fSelect("cf-str", "Strength", [1, 2, 3, 4, 5].map((n) => [n, `${n} · ${STRENGTH[n]}`]), c.strength, { blank: "Unrated" })}${fSelect("cf-tier", "Priority tier", TIERS.map((t) => [t, `Tier ${t} · every ${S.settings.cadence?.[t] || ""} days`]), c.tier, { blank: "No tier" })}
    ${fSelect("cf-intro", "Introduced by", contactOpts().filter(([v]) => v !== id), c.introducedBy, { blank: "—" })}${fInput("cf-introtxt", "…or introduced by (not in UConnect)", c.introducedByText)}
    ${fInput("cf-lt", "Last touch", c.lastContacted, { type: "date" })}${fInput("cf-fu", "Next follow-up", c.nextFollowUp, { type: "date" })}
    ${fInput("cf-tags", "Tags (comma separated)", (c.tags || []).join(", "), { cls: "span2" })}
    ${fText("cf-notes", "Notes", c.notes, { cls: "span2", rows: 3, ph: "Background, family, interests, how you met…" })}
  </div></div>
  <div class="fsec"><div class="fsec-title">Compliance</div><div class="fgrid">
    ${fSelect("cf-lawful", "Lawful basis", LAWFUL, c.lawfulBasis)}${fInput("cf-consent", "Consent / basis date", c.consentDate, { type: "date" })}
    ${fInput("cf-source", "Source of this contact", c.source, { ph: "e.g. Met at SuperReturn, LinkedIn import" })}<div class="field" style="justify-content:flex-end"><label class="check"><input type="checkbox" id="cf-dnc" ${c.doNotContact ? "checked" : ""}> Do not contact</label></div>
  </div></div>`;
  const m = openModal(modalShell(id ? "Edit contact" : "New contact", body, `<span class="hint" id="cf-dupe" style="margin-right:auto"></span><button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="cf-save">${id ? "Save changes" : "Add contact"}</button>`), { size: "wide" });
  if (drawerState) drawerState.editing = true;
  m.addEventListener("pickchange", (e) => {
    if (e.target.id !== "cf-assets") return;
    const cur = pickVal(m, "cf-subs"); const s2 = uniq(pickVal(m, "cf-assets").flatMap((a) => ASSET_CLASSES[a] || []));
    $("#cf-subs-wrap", m).innerHTML = s2.length ? fPick("cf-subs", "Strategies", s2, cur) : "";
  });
  const dupeCheck = () => {
    const email = norm(val(m, "cf-email")); const nm = norm(`${val(m, "cf-first")} ${val(m, "cf-last")}`); const org = norm(val(m, "cf-org"));
    const hit = [...S.contacts.values()].find((x) => x.id !== id && ((email && norm(x.email) === email) || (nm.length > 3 && norm(contactName(x)) === nm && (!org || !x.organisation || norm(x.organisation) === org))));
    $("#cf-dupe", m).innerHTML = hit ? `Possible duplicate: <a href="#/contacts/${hit.id}" data-act="modal-close">${esc(contactName(hit))}</a>` : "";
    return hit;
  };
  ["cf-email", "cf-first", "cf-last", "cf-org"].forEach((k) => $("#" + k, m).addEventListener("change", dupeCheck));
  $("#cf-save", m).onclick = async () => {
    const first = val(m, "cf-first"), last = val(m, "cf-last");
    if (!first && !last && !val(m, "cf-org")) { toast("Add at least a name or organisation.", true); return; }
    const tmin = parseMoneyRange(val(m, "cf-tmin")), tmax = parseMoneyRange(val(m, "cf-tmax")), aum = parseMoneyRange(val(m, "cf-aum"));
    const country = canonCountry(val(m, "cf-country"));
    const data = {
      ...(id ? S.contacts.get(id) : {}),
      firstName: first, lastName: last, fullName: [first, last].filter(Boolean).join(" "), organisation: val(m, "cf-org"), title: val(m, "cf-title"), contactType: val(m, "cf-type"), owner: val(m, "cf-owner"),
      assetClasses: pickVal(m, "cf-assets"), subStrategies: pickVal(m, "cf-subs"), regions: pickVal(m, "cf-regions"), sectors: pickVal(m, "cf-sectors"),
      ticketMin: tmin.min || null, ticketMax: tmax.min || null, ticketCurrency: val(m, "cf-tcur") || tmin.cur || "GBP", aum: aum.min || null, aumCurrency: aum.cur || val(m, "cf-tcur") || "GBP",
      leadPref: val(m, "cf-lead"), ownership: val(m, "cf-own"), structures: pickVal(m, "cf-struct"), constraints: pickVal(m, "cf-cons"), mandateNotes: val(m, "cf-mnotes"),
      email: val(m, "cf-email"), phone: val(m, "cf-phone"), city: val(m, "cf-city"), country, linkedin: val(m, "cf-li"), channel: val(m, "cf-chan"), gatekeeper: val(m, "cf-ea"), languages: val(m, "cf-lang"),
      strength: Number(val(m, "cf-str")) || null, tier: val(m, "cf-tier"), introducedBy: val(m, "cf-intro"), introducedByText: val(m, "cf-introtxt"), lastContacted: val(m, "cf-lt"), nextFollowUp: val(m, "cf-fu"),
      tags: uniq(val(m, "cf-tags").split(",").map((s) => s.trim())), notes: val(m, "cf-notes"),
      lawfulBasis: val(m, "cf-lawful"), consentDate: val(m, "cf-consent"), source: val(m, "cf-source"), doNotContact: $("#cf-dnc", m).checked,
    };
    delete data.id;
    if (!id && dupeCheck() && !(await confirmBox("Looks like a duplicate", "A contact with the same email or name already exists. Add anyway?", "Add anyway"))) return;
    const nid = id || uid("c_");
    try { await put("contacts", nid, data, id ? `Updated contact ${contactName(data)}` : `Added contact ${contactName(data)}`); closeModal(); if (drawerState) drawerState.editing = false; toast(id ? "Saved." : "Contact added."); openDrawer("contact", nid); } catch {}
  };
  const origClose = modalStack[modalStack.length - 1].onClose;
  modalStack[modalStack.length - 1].onClose = () => { if (drawerState) { drawerState.editing = false; renderDrawer(); } origClose && origClose(); };
}
/* =========================================================
   Import: files (CSV, XLSX, VCF, LinkedIn), notes → AI, tidy, dedupe
   ========================================================= */
const IMPORT_FIELDS = [["", "Ignore"], ["fullName", "Full name"], ["firstName", "First name"], ["lastName", "Last name"], ["organisation", "Organisation"], ["title", "Title / role"], ["email", "Email"], ["phone", "Phone"], ["linkedin", "LinkedIn URL"], ["city", "City"], ["country", "Country"], ["location", "Location (city, country)"], ["contactType", "Investor type"], ["assetClasses", "Asset classes / strategy"], ["regions", "Geographic focus"], ["sectors", "Sectors"], ["ticket", "Ticket size (range)"], ["ticketMin", "Ticket min"], ["ticketMax", "Ticket max"], ["aum", "AUM / wealth"], ["strength", "Relationship strength"], ["tier", "Tier (A/B/C)"], ["tags", "Tags / groups"], ["notes", "Notes"], ["lastContacted", "Last contacted"], ["connectedOn", "Connected on"], ["owner", "Owner"], ["source", "Source"], ["gatekeeper", "EA / gatekeeper"]];
const MULTI_FIELDS = new Set(["notes", "tags", "assetClasses", "regions", "sectors"]);
const AUTO_MAP = [
  ["email", /e-?mail/i], ["phone", /phone|mobile|\btel\b|cell/i], ["linkedin", /linkedin|^url$|profile url/i],
  ["firstName", /^(first|given)[ _-]?name|^forename/i], ["lastName", /^(last|family|sur)[ _-]?name|^surname$/i],
  ["fullName", /^(full[ _-]?)?name$|^contact( name)?$|^display name$/i], ["title", /title|position|job|^role$/i],
  ["organisation", /company|organi[sz]ation|^firm|employer|^fund$|family office|institution/i], ["city", /city|town/i], ["country", /country/i], ["location", /location|address|based/i],
  ["contactType", /investor type|^type$|category|segment/i], ["assetClasses", /asset|strategy|focus|mandate|interest/i], ["regions", /geograph|region/i], ["sectors", /sector|industr/i],
  ["ticketMin", /(ticket|cheque|check).*(min|from)|min.*(ticket|cheque|check)/i], ["ticketMax", /(ticket|cheque|check).*(max|to)|max.*(ticket|cheque|check)/i], ["ticket", /ticket|cheque|check size|allocation/i], ["aum", /\baum\b|assets under|net worth|wealth/i],
  ["strength", /strength|warmth|relationship/i], ["tier", /tier|priority/i], ["tags", /tag|label|group/i], ["notes", /note|comment|description|\bbio\b|remarks/i],
  ["lastContacted", /last (contact|touch|met|spoke)/i], ["connectedOn", /connected on/i], ["owner", /owner/i], ["source", /source/i], ["gatekeeper", /assistant|\bea\b|gatekeeper/i],
];
function autoMap(headers) {
  const used = new Set();
  return headers.map((h) => {
    const s = String(h || "").trim(); if (!s) return "";
    for (const [f, re] of AUTO_MAP) if (re.test(s) && (MULTI_FIELDS.has(f) || !used.has(f))) { used.add(f); return f; }
    return "";
  });
}
function parseDateLoose(v) {
  if (!v) return "";
  const s = String(v).trim();
  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})/); if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  m = s.match(/^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})$/); if (m) { const y = m[3].length === 2 ? "20" + m[3] : m[3]; return `${y}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`; }
  const MON = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };
  m = s.match(/^(\d{1,2})\s+([A-Za-z]{3})[a-z]*\.?\s+(\d{4})$/); if (m && MON[m[2].toLowerCase()]) return `${m[3]}-${String(MON[m[2].toLowerCase()]).padStart(2, "0")}-${m[1].padStart(2, "0")}`;
  m = s.match(/^([A-Za-z]{3})[a-z]*\.?\s+(\d{1,2}),?\s+(\d{4})$/); if (m && MON[m[1].toLowerCase()]) return `${m[3]}-${String(MON[m[1].toLowerCase()]).padStart(2, "0")}-${m[2].padStart(2, "0")}`;
  const d = new Date(s); return isNaN(d) ? "" : d.toISOString().slice(0, 10);
}
function parseStrength(v) {
  if (!v) return null; const s = String(v).toLowerCase();
  const n = parseInt(s, 10); if (n >= 1 && n <= 5) return n;
  if (/inner|close|family/.test(s)) return 5; if (/strong|very warm/.test(s)) return 4; if (/warm/.test(s)) return 3; if (/aware|met|luke/.test(s)) return 2; if (/cold/.test(s)) return 1;
  return null;
}
/** Turn a raw record (field → text) into a normalised contact. */
function normaliseContact(r, defaults = {}) {
  const c = { ...defaults };
  const t = (k) => (r[k] === undefined || r[k] === null ? "" : String(r[k]).trim());
  c.firstName = t("firstName"); c.lastName = t("lastName"); c.fullName = t("fullName");
  if (c.fullName && !c.firstName && !c.lastName) { const p = c.fullName.replace(/\s+/g, " ").split(" "); c.firstName = p.shift(); c.lastName = p.join(" "); }
  if (!c.fullName) c.fullName = [c.firstName, c.lastName].filter(Boolean).join(" ");
  c.organisation = t("organisation"); c.title = t("title"); c.email = t("email").toLowerCase(); c.phone = t("phone");
  const li = t("linkedin"); if (li && /linkedin/i.test(li)) c.linkedin = li;
  let city = t("city"), country = t("country");
  if (t("location")) { const loc = parseLocation(t("location")); city = city || loc.city || ""; country = country || loc.country || ""; }
  c.city = city; c.country = canonCountry(country);
  const hay = [c.organisation, c.title].join(" ");
  c.contactType = parseType(t("contactType")) || (Array.isArray(r.contactType) ? "" : "") || parseType(hay) || "";
  const assetTxt = [t("assetClasses"), Array.isArray(r.assetClasses) ? r.assetClasses.join(" ") : ""].join(" ");
  c.assetClasses = uniq([...(Array.isArray(r.assetClasses) ? r.assetClasses.filter((a) => ASSET_CLASSES[a]) : []), ...parseAssetClasses(assetTxt)]);
  if (!c.assetClasses.length) c.assetClasses = parseAssetClasses(c.organisation);
  c.subStrategies = uniq(Array.isArray(r.subStrategies) ? r.subStrategies : []).filter((s) => ASSET_KEYS.some((a) => ASSET_CLASSES[a].includes(s)));
  c.regions = uniq([...(Array.isArray(r.regions) ? r.regions.filter((x) => REGIONS.includes(x)) : []), ...parseRegions(t("regions"))]);
  c.sectors = uniq([...(Array.isArray(r.sectors) ? r.sectors.filter((x) => SECTORS.includes(x)) : []), ...parseSectors(t("sectors"))]);
  const tk = parseMoneyRange(t("ticket"));
  c.ticketMin = Number(r.ticketMin) || parseMoneyRange(t("ticketMin")).min || tk.min || null;
  c.ticketMax = Number(r.ticketMax) || parseMoneyRange(t("ticketMax")).min || tk.max || null;
  c.ticketCurrency = r.ticketCurrency || tk.cur || parseMoneyRange(t("ticketMin")).cur || "GBP";
  const au = parseMoneyRange(t("aum")); c.aum = Number(r.aum) || au.min || null; c.aumCurrency = au.cur || c.ticketCurrency;
  c.strength = parseStrength(t("strength")) || defaults.strength || null;
  const tier = t("tier").toUpperCase().match(/\b([ABC])\b/); c.tier = tier ? tier[1] : "";
  c.tags = uniq([...(defaults.tags || []), ...t("tags").split(/[;,|]/).map((x) => x.trim())]);
  c.notes = t("notes");
  c.lastContacted = parseDateLoose(t("lastContacted"));
  c.connectedOn = parseDateLoose(t("connectedOn"));
  const own = t("owner"); if (own) { const u = [...S.users.values()].find((u) => norm(u.name).includes(norm(own))); if (u) c.owner = u.id; }
  c.source = t("source") || defaults.source || "";
  c.gatekeeper = t("gatekeeper");
  for (const k of Object.keys(c)) if (c[k] === "" || c[k] === null || (Array.isArray(c[k]) && !c[k].length && !["tags"].includes(k))) delete c[k];
  return c;
}
function dupeKeys(c) {
  const k = [];
  if (c.email) k.push("e:" + c.email.toLowerCase());
  if (c.linkedin) k.push("l:" + c.linkedin.toLowerCase().replace(/^https?:\/\/(www\.)?/, "").replace(/\/$/, ""));
  const nm = norm(contactName(c)); if (nm.length > 3) k.push("n:" + nm + "|" + norm(c.organisation || ""));
  return k;
}
function existingIndex() { const m = new Map(); for (const c of S.contacts.values()) for (const k of dupeKeys(c)) m.set(k, c.id); return m; }
function mergeInto(base, inc) {
  const out = { ...base };
  for (const [k, v] of Object.entries(inc)) {
    if (Array.isArray(v)) out[k] = uniq([...(base[k] || []), ...v]);
    else if (k === "notes" && v && base.notes && !base.notes.includes(v)) out.notes = base.notes + "\n\n" + v;
    else if (base[k] === undefined || base[k] === null || base[k] === "") out[k] = v;
  }
  return out;
}

/* ---------- Wizard ---------- */
const IMP = {};
function importWizard(start = "file") {
  Object.assign(IMP, { mode: start, rows: null, headers: [], map: [], recs: null, sourceName: "", step: 1 });
  const m = openModal(`<div id="imp-root"></div>`, { size: "wide" });
  IMP.root = m; drawImport();
}
function drawImport() {
  const r = IMP.root; if (!r) return;
  const aiOn = !!S.sample;
  if (IMP.step === 1) {
    r.innerHTML = modalShell("Import contacts", `
      <div class="tabs" style="margin-top:10px">${[["file", "Upload a file"], ["notes", "Paste notes"]].map(([k, l]) => `<button class="${IMP.mode === k ? "on" : ""}" data-imp-mode="${k}">${l}</button>`).join("")}</div>
      ${IMP.mode === "file" ? `
      <label class="drop" id="imp-drop" for="imp-file"><strong>Drop a file here or choose one</strong><span>LinkedIn Connections.csv · iPhone / Android / Outlook .vcf · Gmail or Outlook .csv · Excel .xlsx · any spreadsheet</span><span class="hint">Nothing leaves this workspace except rows you choose to tidy with Claude.</span></label>
      <input type="file" id="imp-file" accept=".csv,.tsv,.txt,.vcf,.xlsx,.xls" hidden>
      <div class="stack" style="margin-top:18px;gap:8px"><div class="hint"><strong style="color:var(--ink-2)">Getting your exports</strong></div>
      <div class="hint">LinkedIn: Settings → Data privacy → Get a copy of your data → Connections. Emails only appear for connections who allow it.</div>
      <div class="hint">iPhone: iCloud.com → Contacts → select all → Export vCard. Gmail: contacts.google.com → Export → Google CSV. Outlook: People → Manage → Export contacts.</div>
      <div class="row" style="margin-top:6px"><button class="btn sm" data-act="tpl" data-k="contacts">${icon("download")}UConnect contact template (.csv)</button></div></div>`
      : `
      ${aiOn ? `<p class="dim" style="margin-top:14px">Paste anything: WhatsApp exports, meeting notes, email signatures, a scribbled list. Claude extracts each person with their firm, mandate and location for you to review before anything is saved.</p>
      <textarea class="input" id="imp-notes" rows="12" placeholder="e.g. Met Khalid at the Dubai FO dinner, runs his family's office (Al-Ameen Holdings), keen on UK logistics RE and growth PE, $5-20m tickets. khalid@alameen.ae"></textarea>
      <div class="hint" id="imp-notes-n" style="margin-top:6px">Up to about 40,000 characters per extraction.</div>` : `<div class="empty"><div>Claude isn't available in this view, so notes can't be extracted automatically.</div><div class="hint">Use a file upload, or add contacts by hand.</div></div>`}`}
    `, IMP.mode === "notes" && aiOn ? `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="imp-extract">${icon("sparkle")}Extract contacts</button>` : `<button class="btn ghost" data-act="modal-close">Cancel</button>`);
    $$("[data-imp-mode]", r).forEach((b) => (b.onclick = () => { IMP.mode = b.dataset.impMode; drawImport(); }));
    const inp = $("#imp-file", r), drop = $("#imp-drop", r);
    if (inp) {
      inp.onchange = () => inp.files[0] && loadImportFile(inp.files[0]);
      ["dragover", "dragenter"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); }));
      ["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.remove("over"); }));
      drop.addEventListener("drop", (e) => e.dataTransfer.files[0] && loadImportFile(e.dataTransfer.files[0]));
    }
    const ex = $("#imp-extract", r); if (ex) ex.onclick = extractFromNotes;
  } else if (IMP.step === 2) {
    const sample = IMP.rows.slice(0, 3);
    r.innerHTML = modalShell("Match your columns", `
      <p class="dim" style="margin-top:14px">${esc(IMP.sourceName)} · ${plural(IMP.rows.length, "row")}. We've matched what we could. Adjust anything that looks wrong; ignored columns aren't imported.</p>
      <div class="tablewrap"><table class="t"><thead><tr><th>Your column</th><th>Sample</th><th>Import as</th></tr></thead><tbody>
      ${IMP.headers.map((h, i) => `<tr class="nohover"><td class="name">${esc(h || `Column ${i + 1}`)}</td><td class="dim" style="max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${esc(sample.map((s) => s[i]).filter(Boolean).join(" · ").slice(0, 90))}</td><td><select class="input sm" data-col="${i}">${IMPORT_FIELDS.map(([v, l]) => `<option value="${v}" ${IMP.map[i] === v ? "selected" : ""}>${l}</option>`).join("")}</select></td></tr>`).join("")}
      </tbody></table></div>`, `<button class="btn ghost" id="imp-back">Back</button><button class="btn primary" id="imp-next">Review ${plural(IMP.rows.length, "contact")}</button>`);
    $$("[data-col]", r).forEach((s) => (s.onchange = () => (IMP.map[+s.dataset.col] = s.value)));
    $("#imp-back", r).onclick = () => { IMP.step = 1; drawImport(); };
    $("#imp-next", r).onclick = () => {
      if (!IMP.map.some((f) => ["fullName", "firstName", "lastName", "email", "organisation"].includes(f))) { toast("Map at least a name, email or organisation column.", true); return; }
      IMP.recs = IMP.rows.map((row) => { const o = {}; IMP.map.forEach((f, i) => { if (!f) return; const v = String(row[i] ?? "").trim(); if (!v) return; o[f] = MULTI_FIELDS.has(f) && o[f] ? o[f] + "; " + v : o[f] || v; }); return o; });
      buildReview();
    };
  } else if (IMP.step === 3) drawReview();
  else if (IMP.step === 4) {
    r.innerHTML = modalShell("Importing", `<div class="stack" style="padding:30px 0;gap:16px"><div id="imp-phase" class="dim">Preparing…</div><div class="meter"><i id="imp-bar" style="width:0%"></i></div><div class="hint" id="imp-detail"></div></div>`, `<button class="btn ghost" id="imp-stop">Stop</button>`);
    $("#imp-stop", r).onclick = () => { IMP.stop = true; IMP.aiCtl?.abort(); };
  }
}
async function loadImportFile(file) {
  const name = file.name.toLowerCase(); IMP.sourceName = file.name;
  try {
    if (name.endsWith(".vcf")) {
      const recs = parseVCF(await readFileText(file));
      IMP.recs = recs; IMP.source = "Phone / vCard import"; return buildReview();
    }
    let rows;
    if (name.endsWith(".xlsx") || name.endsWith(".xls")) { toast("Reading spreadsheet…"); rows = await xlsxRows(file); }
    else rows = parseCSV(await readFileText(file));
    // Skip preambles (LinkedIn adds "Notes:" lines before the header row)
    let hi = rows.findIndex((r) => r.filter((c) => String(c).trim()).length >= 2 && r.some((c) => /name|email|company|organi/i.test(String(c))));
    if (hi < 0) hi = 0;
    IMP.headers = rows[hi].map((h) => String(h).trim()); IMP.rows = rows.slice(hi + 1).filter((r) => r.some((c) => String(c).trim()));
    const isLinkedIn = IMP.headers.includes("Connected On") || (IMP.headers.includes("URL") && IMP.headers.includes("Position"));
    IMP.source = isLinkedIn ? "LinkedIn import" : name.endsWith(".csv") && IMP.headers.some((h) => /E-mail 1|Organization 1/i.test(h)) ? "Google Contacts import" : IMP.headers.some((h) => /E-mail Address|Business Phone/i.test(h)) ? "Outlook import" : "Spreadsheet import";
    IMP.map = autoMap(IMP.headers);
    if (!IMP.rows.length) { toast("That file has no rows to import.", true); return; }
    IMP.step = 2; drawImport();
  } catch (e) { console.warn(e); toast("That file couldn't be read. Save it as CSV and try again.", true); }
}
async function extractFromNotes() {
  const text = val(IMP.root, "imp-notes"); if (!text) return;
  const btn = $("#imp-extract", IMP.root); btn.disabled = true; btn.innerHTML = `<span class="spin"></span> Reading…`;
  const prompt = `You are building an investor CRM for a UK-based principal who allocates, co-invests and places capital (fee-based introductions). Extract every distinct person from the notes below.
For each person return an object with these keys (omit any key you cannot support from the text; never invent emails or phone numbers):
firstName, lastName, organisation, title, email, phone, city, country,
contactType (one of: ${CONTACT_TYPES.join(" | ")}),
assetClasses (array from: ${ASSET_KEYS.join(" | ")}),
subStrategies (array from: ${ASSET_KEYS.map((a) => ASSET_CLASSES[a].join(" | ")).join(" | ")}),
sectors (array from: ${SECTORS.join(" | ")}),
regions (where they invest; array from: ${REGIONS.join(" | ")}),
ticketMin, ticketMax (numbers in full units), ticketCurrency (ISO code), aum (number), strength (1-5 where 3 = warm), notes (one or two sentences of useful context from the text), tags (array).
Reply with only a JSON array of these objects.

NOTES:
"""${text.slice(0, 40000)}"""`;
  try {
    const out = await S.sample.json(prompt, { modelTier: "default" });
    const list = Array.isArray(out) ? out : Array.isArray(out?.contacts) ? out.contacts : [];
    if (!list.length) { toast("No people were found in that text.", true); btn.disabled = false; btn.innerHTML = `${icon("sparkle")}Extract contacts`; return; }
    IMP.recs = list.map((o) => ({ ...o, fullName: [o.firstName, o.lastName].filter(Boolean).join(" ") }));
    IMP.source = "Notes (AI extracted)"; IMP.sourceName = "Pasted notes"; IMP.fromAI = true;
    audit("import", "contacts", "", `Extracted ${list.length} contacts from pasted notes with Claude`);
    buildReview();
  } catch (e) { toast(aiErr(e), true); btn.disabled = false; btn.innerHTML = `${icon("sparkle")}Extract contacts`; }
}
function buildReview() {
  const idx = existingIndex(); const seen = new Map(); const out = [];
  for (const raw of IMP.recs) {
    const c = normaliseContact(raw, {});
    if (!contactName(c) || contactName(c) === "Unnamed") { if (!c.organisation && !c.email) continue; }
    const keys = dupeKeys(c);
    const inFile = keys.map((k) => seen.get(k)).find((x) => x !== undefined);
    if (inFile !== undefined) { out[inFile].c = mergeInto(out[inFile].c, c); continue; }
    const ex = keys.map((k) => idx.get(k)).find(Boolean);
    const i = out.length; keys.forEach((k) => seen.set(k, i));
    out.push({ c, existing: ex || null, include: true });
  }
  IMP.review = out; IMP.step = 3; drawImport();
}
function drawReview() {
  const r = IMP.root; const rv = IMP.review;
  const nNew = rv.filter((x) => !x.existing).length, nDup = rv.length - nNew;
  const missing = rv.filter((x) => !(x.c.assetClasses || []).length || !x.c.contactType).length;
  const aiOn = !!S.sample; const aiCap = 600;
  r.innerHTML = modalShell("Review import", `
    <div class="kpis" style="grid-template-columns:repeat(3,minmax(0,1fr));margin-top:14px"><div class="kpi"><span class="eyebrow">New</span><span class="v num">${nNew}</span></div><div class="kpi"><span class="eyebrow">Already in UConnect</span><span class="v num">${nDup}</span></div><div class="kpi"><span class="eyebrow">Missing mandate or type</span><span class="v num">${missing}</span></div></div>
    <div class="fgrid">
      ${fInput("imp-tag", "Tag every imported contact", `${IMP.source || "Import"} ${fmtDate(Date.now(), { day: "numeric", month: "short" })}`)}
      ${fSelect("imp-owner", "Relationship owner", userOpts(), S.me.id, { blank: "Unassigned" })}
      ${fSelect("imp-dupes", "When a contact already exists", [["merge", "Merge: fill gaps and add tags"], ["skip", "Skip it"]], "merge", { blank: null })}
      ${fSelect("imp-lawful", "Lawful basis (UK GDPR)", LAWFUL, "Legitimate interest", { blank: null })}
      <div class="field span2">${aiOn ? `<label class="check"><input type="checkbox" id="imp-ai" ${missing ? "checked" : ""}> Tidy with Claude: fill investor type, asset classes, strategies, sectors and geography where missing${missing > aiCap ? ` (first ${aiCap} of ${missing})` : ""}. Uses your Claude plan, about ${Math.ceil(Math.min(missing, aiCap) / 40)} request${Math.ceil(Math.min(missing, aiCap) / 40) === 1 ? "" : "s"}.</label>` : `<span class="hint">Claude isn't available in this view, so missing mandates will be left blank for you to fill in.</span>`}</div>
    </div>
    <div class="tablewrap" style="margin-top:16px;max-height:40vh;overflow:auto"><table class="t"><thead><tr><th style="width:34px"><input type="checkbox" id="imp-all" checked aria-label="Include all"></th><th>Name</th><th>Organisation</th><th>Type</th><th>Mandate</th><th>Location</th><th>Status</th></tr></thead><tbody>
    ${rv.slice(0, 400).map((x, i) => `<tr class="nohover"><td><input type="checkbox" data-inc="${i}" ${x.include ? "checked" : ""}></td><td class="name">${esc(contactName(x.c))}</td><td class="dim">${esc(x.c.organisation || "")}</td><td class="dim">${esc(x.c.contactType || "—")}</td><td><div class="chips">${(x.c.assetClasses || []).map((a) => `<span class="chip">${esc(ASSET_SHORT[a])}</span>`).join("")}</div></td><td class="dim">${esc([x.c.city, x.c.country].filter(Boolean).join(", "))}</td><td>${x.existing ? `<span class="chip warn">Exists</span>` : `<span class="chip good">New</span>`}</td></tr>`).join("")}
    </tbody></table>${rv.length > 400 ? `<div class="hint" style="padding:10px 14px">Showing the first 400 of ${rv.length}. All included rows will be imported.</div>` : ""}</div>`,
    `<button class="btn ghost" id="imp-back">Back</button><button class="btn primary" id="imp-go">Import ${plural(rv.filter((x) => x.include).length, "contact")}</button>`);
  $("#imp-back", r).onclick = () => { IMP.step = IMP.rows ? 2 : 1; drawImport(); };
  $$("[data-inc]", r).forEach((cb) => (cb.onchange = () => { rv[+cb.dataset.inc].include = cb.checked; $("#imp-go", r).textContent = `Import ${plural(rv.filter((x) => x.include).length, "contact")}`; }));
  $("#imp-all", r).onchange = (e) => { rv.forEach((x) => (x.include = e.target.checked)); drawReview(); };
  $("#imp-go", r).onclick = () => runImport({ tag: val(r, "imp-tag"), owner: val(r, "imp-owner"), dupes: val(r, "imp-dupes"), lawful: val(r, "imp-lawful"), ai: aiOn && $("#imp-ai", r)?.checked, aiCap });
}
async function aiTidy(list, onProgress) {
  const need = list.filter((c) => !(c.assetClasses || []).length || !c.contactType || !(c.sectors || []).length);
  let done = 0;
  for (let i = 0; i < need.length; i += 40) {
    if (IMP.stop) break;
    const chunk = need.slice(i, i + 40);
    const payload = chunk.map((c, j) => ({ i: j, name: contactName(c), org: c.organisation || "", title: c.title || "", location: [c.city, c.country].filter(Boolean).join(", "), domain: (c.email || "").split("@")[1] || "", notes: (c.notes || "").slice(0, 240) }));
    const prompt = `You are tidying an investor CRM for a principal who raises, allocates and places private capital. For each contact, infer ONLY what is reasonably supported by the organisation, title, notes, location and email domain, using general knowledge of well-known firms. Leave a field empty rather than guess.
Allowed values:
contactType: ${CONTACT_TYPES.join(" | ")}
assetClasses: ${ASSET_KEYS.join(" | ")}
subStrategies: ${ASSET_KEYS.map((a) => ASSET_CLASSES[a].join(" | ")).join(" | ")}
sectors: ${SECTORS.join(" | ")}
regions (where they invest): ${REGIONS.join(" | ")}
country: English country name, only if clearly implied.
Reply with only a JSON array like [{"i":0,"contactType":"","assetClasses":[],"subStrategies":[],"sectors":[],"regions":[],"country":""}].

CONTACTS:
${JSON.stringify(payload)}`;
    try {
      IMP.aiCtl = new AbortController();
      const out = await S.sample.json(prompt, { modelTier: "quick", signal: IMP.aiCtl.signal });
      for (const o of Array.isArray(out) ? out : []) {
        const c = chunk[o.i]; if (!c) continue;
        if (!c.contactType && CONTACT_TYPES.includes(o.contactType)) c.contactType = o.contactType;
        if (!(c.assetClasses || []).length) c.assetClasses = arr(o.assetClasses).filter((a) => ASSET_CLASSES[a]);
        if (!(c.subStrategies || []).length) c.subStrategies = arr(o.subStrategies).filter((s) => ASSET_KEYS.some((a) => ASSET_CLASSES[a].includes(s)));
        if (!(c.sectors || []).length) c.sectors = arr(o.sectors).filter((s) => SECTORS.includes(s));
        if (!(c.regions || []).length) c.regions = arr(o.regions).filter((s) => REGIONS.includes(s));
        if (!c.country && o.country && COUNTRIES[canonCountry(o.country)]) c.country = canonCountry(o.country);
        c.aiTagged = true;
      }
    } catch (e) {
      if (e && (e.code === "not_granted" || e.code === "cancelled")) { toast(aiErr(e)); break; }
      if (e && e.code === "rate_limited") { await sleep(8000); i -= 40; continue; }
      console.warn("tidy", e);
    }
    done += chunk.length; onProgress(done, need.length);
  }
}
async function runImport(opt) {
  const sel = IMP.review.filter((x) => x.include); IMP.stop = false;
  IMP.step = 4; drawImport();
  const phase = (t) => ($("#imp-phase", IMP.root).textContent = t); const bar = (p) => ($("#imp-bar", IMP.root).style.width = `${Math.round(p * 100)}%`); const det = (t) => ($("#imp-detail", IMP.root).textContent = t);
  const defaults = { tags: opt.tag ? [opt.tag] : [], owner: opt.owner || "", lawfulBasis: opt.lawful, source: IMP.source || "Import", strength: 2 };
  for (const x of sel) { x.c = { ...x.c, tags: uniq([...(x.c.tags || []), ...defaults.tags]) }; for (const k of ["owner", "lawfulBasis", "source"]) if (!x.c[k] && defaults[k]) x.c[k] = defaults[k]; if (!x.c.strength) x.c.strength = 2; }
  if (opt.ai) {
    phase("Tidying with Claude…");
    const pool = sel.map((x) => x.c).filter((c) => !(c.assetClasses || []).length || !c.contactType).slice(0, opt.aiCap);
    await aiTidy(pool, (d, n) => { bar(d / n / 2); det(`${d} of ${n} reviewed`); });
  }
  phase("Saving to UConnect…"); let ok = 0, merged = 0, failed = 0, skipped = 0;
  const work = sel.slice(); let idx = 0;
  const worker = async () => {
    while (idx < work.length && !IMP.stop) {
      const x = work[idx++];
      try {
        if (x.existing) {
          if (opt.dupes === "skip") { skipped++; continue; }
          const base = S.contacts.get(x.existing); if (!base) { skipped++; continue; }
          const m = mergeInto(base, x.c); delete m.id;
          await withRetry(() => S.db.doc(`contacts/${x.existing}`).set(stamp(clean(m), false))); merged++;
        } else {
          const id = uid("c_");
          await withRetry(() => S.db.doc(`contacts/${id}`).set(stamp(clean(x.c), true))); ok++;
        }
      } catch (e) { failed++; if (e && e.code === "quota_exceeded") { IMP.stop = true; toast("Storage is full (5,000 records). Import stopped.", true); } }
      bar((opt.ai ? 0.5 : 0) + ((ok + merged + failed + skipped) / work.length) * (opt.ai ? 0.5 : 1)); det(`${ok} added · ${merged} merged${skipped ? ` · ${skipped} skipped` : ""}${failed ? ` · ${failed} failed` : ""}`);
    }
  };
  await Promise.all([worker(), worker(), worker(), worker()]);
  audit("import", "contacts", "", `Imported from ${IMP.sourceName || IMP.source}: ${ok} added, ${merged} merged${skipped ? `, ${skipped} skipped` : ""}${opt.ai ? " (tidied with Claude)" : ""}`);
  closeModal(); IMP.root = null;
  toast(`Import complete: ${ok} added, ${merged} merged${failed ? `, ${failed} failed` : ""}.`, failed > 0);
  if (opt.tag) { S.ui.contacts.tag = opt.tag; }
  go("#/contacts");
}

/* ---------- Simple imports (deals, pipeline, tasks, intros) ---------- */
const TEMPLATES = {
  contacts: [["First name", "Last name", "Organisation", "Title", "Investor type", "Email", "Phone", "City", "Country", "Asset classes", "Geographic focus", "Sectors", "Ticket size", "AUM", "Relationship strength", "Tier", "Tags", "Notes", "Last contacted"], ["Jane", "Example", "Example Family Office", "CIO", "Single family office", "jane@example.com", "+44 20 0000 0000", "London", "United Kingdom", "Private Equity; Real Estate", "UK & Ireland; Europe", "Healthcare; Logistics", "£2m-£10m", "£400m", "Warm", "A", "Co-invest club", "Prefers minority growth deals", "2026-08-14"]],
  deals: [["Deal", "Type", "Asset class", "Strategy", "Sectors", "Geography", "Target", "Currency", "Min ticket", "Sponsor", "Close date", "Fee %", "Summary"], ["Example Logistics Portfolio", "Placement (fee-based)", "Real Estate", "Logistics", "Logistics", "UK & Ireland", "40000000", "GBP", "1000000", "Example Capital", "2026-12-15", "2", "Six urban logistics assets, 7-year hold"]],
  pipeline: [["Deal", "Contact email", "Contact name", "Stage", "Indicated", "Committed", "Currency", "Next step", "Next step date"], ["Example Logistics Portfolio", "jane@example.com", "Jane Example", "In discussion", "5000000", "", "GBP", "Send IM", "2026-09-20"]],
  tasks: [["Task", "Due", "Priority", "Contact email", "Deal", "Assignee"], ["Call about Q4 allocation", "2026-09-18", "High", "jane@example.com", "Example Logistics Portfolio", "Ada"]],
  intros: [["Date", "Direction", "Introducer", "Introduced contact email", "Deal", "Agreement", "Fee %", "Fee amount", "Currency", "Status", "Notes"], ["2026-07-02", "We introduced (fee receivable)", "Principal", "jane@example.com", "Example Logistics Portfolio", "Signed", "2", "", "GBP", "Not yet due", ""]],
};
ACT.tpl = (el) => { const t = TEMPLATES[el.dataset.k]; saveFile(`uconnect-${el.dataset.k}-template.csv`, t.map((r) => r.map((v) => (/[,"]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v)).join(",")).join("\n")); };
function findContact(email, name) {
  const e = norm(email), n = norm(name);
  for (const c of S.contacts.values()) { if (e && norm(c.email) === e) return c; }
  if (n) for (const c of S.contacts.values()) if (norm(contactName(c)) === n) return c;
  return null;
}
function findDeal(name) { const n = norm(name); return [...S.deals.values()].find((d) => norm(d.name) === n) || null; }
function simpleImport(kind) {
  const labels = { deals: "Import deals", pipeline: "Import pipeline rows", tasks: "Import tasks", intros: "Import introductions" };
  const m = openModal(modalShell(labels[kind], `<label class="drop" for="si-file" id="si-drop" style="margin-top:14px"><strong>Choose a CSV or Excel file</strong><span>Headers are matched automatically. Use the template for a perfect fit.</span></label><input type="file" id="si-file" accept=".csv,.tsv,.txt,.xlsx,.xls" hidden><div class="row" style="margin-top:12px"><button class="btn sm" data-act="tpl" data-k="${kind}">${icon("download")}Download template</button></div><div id="si-out" style="margin-top:14px"></div>`, `<button class="btn ghost" data-act="modal-close">Close</button>`), { size: "" });
  $("#si-file", m).onchange = async (e) => {
    const f = e.target.files[0]; if (!f) return;
    let rows; try { rows = /\.xlsx?$/i.test(f.name) ? await xlsxRows(f) : parseCSV(await readFileText(f)); } catch { toast("That file couldn't be read.", true); return; }
    const hdr = rows[0].map((h) => norm(h)); const data = rows.slice(1);
    const col = (...res) => hdr.findIndex((h) => res.some((re) => re.test(h)));
    const g = (row, i) => (i >= 0 ? String(row[i] ?? "").trim() : "");
    const out = $("#si-out", m); out.innerHTML = `<div class="row"><span class="spin"></span><span class="dim">Importing ${data.length} rows…</span></div>`;
    let n = 0, miss = 0;
    try {
      if (kind === "deals") {
        const I = { name: col(/^deal|^name/), type: col(/type/), ac: col(/asset/), sub: col(/strateg/), sec: col(/sector/), geo: col(/geograph|region/), tgt: col(/target|size|raise/), cur: col(/currenc/), min: col(/min/), sp: col(/sponsor|counterpart|manager/), close: col(/close/), fee: col(/fee/), sum: col(/summary|descr/) };
        for (const r of data) { const name = g(r, I.name); if (!name) continue; const tg = parseMoneyRange(g(r, I.tgt)); const ac = parseAssetClasses(g(r, I.ac))[0] || g(r, I.ac);
          await put("deals", uid("d_"), { name, type: DEAL_TYPES.find((t) => norm(t).startsWith(norm(g(r, I.type)).slice(0, 6))) || DEAL_TYPES[0], assetClass: ASSET_CLASSES[ac] ? ac : "", subStrategy: g(r, I.sub), sectors: parseSectors(g(r, I.sec)), regions: parseRegions(g(r, I.geo)), target: tg.min || null, currency: (g(r, I.cur) || tg.cur || "GBP").toUpperCase(), minTicket: parseMoneyRange(g(r, I.min)).min || null, sponsor: g(r, I.sp), closeDate: parseDateLoose(g(r, I.close)), feePct: parseFloat(g(r, I.fee)) || null, summary: g(r, I.sum), status: "Live", owner: S.me.id, pipeline: {} }, false); n++; }
      } else if (kind === "pipeline") {
        const I = { deal: col(/^deal/), email: col(/email/), name: col(/contact name|^name|^contact$/), stage: col(/stage/), ind: col(/indicat|soft/), com: col(/commit/), cur: col(/currenc/), ns: col(/next step$|^next step|action/), nd: col(/date/) };
        for (const r of data) {
          const d = findDeal(g(r, I.deal)); const c = findContact(g(r, I.email), g(r, I.name)); if (!d || !c) { miss++; continue; }
          const st = ALL_STAGES.find((s) => norm(s) === norm(g(r, I.stage))) || "Prospect";
          await S.db.doc(`deals/${d.id}`).update({ pipeline: { [c.id]: { stage: st, indicated: parseMoneyRange(g(r, I.ind)).min || null, committed: parseMoneyRange(g(r, I.com)).min || null, currency: g(r, I.cur) || d.currency || "GBP", nextStep: g(r, I.ns), nextDate: parseDateLoose(g(r, I.nd)), stageAt: now(), addedAt: now(), addedBy: S.me.id, hist: { [uid("h")]: { stage: st, at: now(), by: S.me.id } } } }, updatedAt: now(), updatedBy: S.me.id }); n++;
        }
      } else if (kind === "tasks") {
        const I = { t: col(/task|title/), due: col(/due|date/), pr: col(/prior/), email: col(/email|contact/), deal: col(/deal/), who: col(/assign|owner/) };
        for (const r of data) { const title = g(r, I.t); if (!title) continue; const c = findContact(g(r, I.email), g(r, I.email)); const d = findDeal(g(r, I.deal)); const u = [...S.users.values()].find((u) => g(r, I.who) && norm(u.name).includes(norm(g(r, I.who))));
          await put("tasks", uid("t_"), { title, due: parseDateLoose(g(r, I.due)), priority: /high|urgent/i.test(g(r, I.pr)) ? "High" : "Normal", contactId: c?.id || "", dealId: d?.id || "", assignee: u?.id || S.me.id, done: false }, false); n++; }
      } else if (kind === "intros") {
        const I = { date: col(/date/), dir: col(/direction/), by: col(/introducer|by/), email: col(/email|contact/), deal: col(/deal/), ag: col(/agreement/), pct: col(/%|percent/), amt: col(/amount/), cur: col(/currenc/), st: col(/status/), notes: col(/note/) };
        for (const r of data) { const c = findContact(g(r, I.email), g(r, I.email)); const d = findDeal(g(r, I.deal)); const intro = findContact("", g(r, I.by));
          await put("intros", uid("i_"), { date: parseDateLoose(g(r, I.date)) || todayISO(), direction: /payable|to us/i.test(g(r, I.dir)) ? INTRO_DIR[1] : INTRO_DIR[0], introducerId: intro?.id || "", introducerText: intro ? "" : g(r, I.by), contactId: c?.id || "", contactText: c ? "" : g(r, I.email), dealId: d?.id || "", agreement: FEE_AGREEMENT.find((a) => norm(a) === norm(g(r, I.ag))) || "None", feePct: parseFloat(g(r, I.pct)) || null, feeAmount: parseMoneyRange(g(r, I.amt)).min || null, currency: g(r, I.cur) || "GBP", status: FEE_STATUS.find((s) => norm(s) === norm(g(r, I.st))) || "Not yet due", notes: g(r, I.notes) }, false); n++; }
      }
      audit("import", kind, "", `Imported ${n} ${kind === "pipeline" ? "pipeline rows" : kind} from ${f.name}`);
      out.innerHTML = `<div class="banner" style="border-style:solid">Imported ${n} row${n === 1 ? "" : "s"}.${miss ? ` ${miss} skipped because the deal or contact wasn't found. Import the deal and contacts first.` : ""}</div>`;
    } catch (e2) { out.innerHTML = `<div class="banner" style="border-color:var(--crit)">Import stopped after ${n} rows. ${esc(e2.message || "")}</div>`; }
  };
}
/* =========================================================
   Deals & pipeline tracker
   ========================================================= */
function dealStats(d) {
  const p = Object.entries(d.pipeline || {}).filter(([, e]) => e);
  const cur = d.currency || "GBP";
  const conv = (e, v) => (e.currency && e.currency !== cur ? toGBP(v, e.currency) / (toGBP(1, cur) || 1) : v);
  const committed = p.filter(([, e]) => e.stage === "Committed" || e.stage === "Closed").reduce((s, [, e]) => s + conv(e, e.committed || e.indicated || 0), 0);
  const indicated = p.filter(([, e]) => e.stage === "In discussion" || e.stage === "Diligence").reduce((s, [, e]) => s + conv(e, e.indicated || 0), 0);
  const counts = Object.fromEntries(ALL_STAGES.map((s) => [s, p.filter(([, e]) => e.stage === s).length]));
  return { committed, indicated, counts, total: p.length };
}
VIEWS.deals = () => {
  const f = S.ui.deals; const q = norm(f.q);
  let deals = [...S.deals.values()].filter((d) => (!f.status || d.status === f.status) && (!q || norm([d.name, d.sponsor, d.assetClass, d.subStrategy, (d.regions || []).join(" ")].join(" ")).includes(q)));
  deals.sort((a, b) => (a.closeDate || "9999").localeCompare(b.closeDate || "9999"));
  const head = `${exampleBanner()}<div class="pagehead"><div><h1 class="h-display">Deals</h1><div class="sub">Where every investor stands on every deal.</div></div>
    <div class="row"><button class="btn" data-act="deal-import">${icon("upload")}Import</button><button class="btn" data-act="deal-from-doc">${icon("sparkle")}New deal from a document</button><button class="btn primary" data-act="deal-new">${icon("plus")}New deal</button></div></div>
    <div class="toolbar"><div class="seg">${[["cards", "By deal"], ["table", "All investors"]].map(([k, l]) => `<button class="${f.view === k ? "on" : ""}" data-act="deals-view" data-k="${k}">${l}</button>`).join("")}</div>
    <input class="input" id="d-q" style="max-width:260px" placeholder="Search deals…" value="${esc(f.q)}" data-in="d-filter" data-k="q">
    <select class="input" id="d-status" style="width:auto" data-on="d-filter" data-k="status"><option value="">All statuses</option>${DEAL_STATUS.map((s) => `<option ${f.status === s ? "selected" : ""}>${s}</option>`).join("")}</select>
    ${f.view === "table" ? `<select class="input" id="d-stage" style="width:auto" data-on="d-filter" data-k="stage"><option value="">${f.stages && !f.stage ? "Discussion + diligence" : "All stages"}</option>${ALL_STAGES.map((s) => `<option ${f.stage === s ? "selected" : ""}>${s}</option>`).join("")}</select>` : ""}</div>`;
  if (!S.deals.size) return head + `<div class="panel"><div class="empty"><div class="h-section">No deals yet.</div><div style="max-width:440px">Create a deal, then add investors from Contacts or let the AI Matcher shortlist them. Each investor moves through Prospect → In discussion → Diligence → Committed → Closed.</div><div class="row"><button class="btn primary" data-act="deal-new">${icon("plus")}New deal</button><button class="btn" data-act="deal-import">Import from spreadsheet</button></div></div></div>`;
  if (f.view === "table") {
    const rows = allEngagements().filter((x) => x.c && deals.includes(x.deal) && (f.stage ? x.e.stage === f.stage : f.stages ? f.stages.includes(x.e.stage) : true)).sort((a, b) => ALL_STAGES.indexOf(b.e.stage) - ALL_STAGES.indexOf(a.e.stage) || toTime(a.e.nextDate || "9999") - toTime(b.e.nextDate || "9999"));
    return head + (rows.length ? `<div class="tablewrap"><table class="t"><thead><tr><th>Investor</th><th>Deal</th><th>Stage</th><th class="r">Indicated</th><th class="r">Committed</th><th>In stage</th><th>Next step</th><th>Owner</th></tr></thead><tbody>
      ${rows.map(({ deal, cid, e, c }) => { const d = daysSince(e.stageAt || e.addedAt); const stalled = ACTIVE_STAGES.includes(e.stage) && e.stage !== "Committed" && d > (S.settings.stallDays || 21); const late = e.nextDate && e.nextDate < todayISO(); return `<tr data-act="eng-open" data-deal="${deal.id}" data-cid="${cid}"><td><div class="name">${esc(contactName(c))}</div><div class="org">${esc(c.organisation || "")}</div></td><td class="dim">${esc(deal.name)}</td><td><span class="row" style="gap:8px;flex-wrap:nowrap"><span class="stage-dot" style="background:${STAGE_COLOR[e.stage]}"></span>${esc(e.stage)}</span></td><td class="r num sens">${e.indicated ? esc(money(e.indicated, e.currency || deal.currency)) : "—"}</td><td class="r num sens">${e.committed ? esc(money(e.committed, e.currency || deal.currency)) : "—"}</td><td>${stalled ? `<span class="chip warn">${d}d</span>` : `<span class="muted">${d === Infinity ? "—" : d + "d"}</span>`}</td><td>${esc(e.nextStep || "")}${e.nextDate ? `<div class="org" style="${late ? "color:#EBA597" : ""}">${late ? "Overdue · " : ""}${esc(fmtDate(e.nextDate))}</div>` : ""}</td><td class="dim">${esc(userName(c.owner) || "—")}</td></tr>`; }).join("")}
      </tbody></table></div>` : `<div class="panel"><div class="empty"><div class="hint">No investors match these filters.</div></div></div>`);
  }
  return head + `<div class="deal-grid">${deals.map((d) => { const s = dealStats(d); const tgt = d.target || 0; const pc = tgt ? Math.min(100, (s.committed / tgt) * 100) : 0; const pi = tgt ? Math.min(100 - pc, (s.indicated / tgt) * 100) : 0;
    return `<article class="deal-card" data-act="go" data-href="#/deals/${encodeURIComponent(d.id)}"><div class="row between" style="align-items:flex-start;flex-wrap:nowrap"><div style="min-width:0"><div class="eyebrow">${esc(d.type || "")}</div><div class="dn" style="margin-top:6px">${esc(d.name)}</div><div class="hint" style="margin-top:4px">${esc([d.assetClass, d.subStrategy, (d.regions || []).join(", ")].filter(Boolean).join(" · "))}</div></div><span class="chip ${d.status === "Live" ? "gold" : d.status === "Closed" ? "good" : ""}">${esc(d.status || "Live")}</span></div>
    ${d.example ? '<span class="chip ex" style="align-self:flex-start">Example</span>' : ""}
    <div class="figs"><div><div class="eyebrow">Target</div><div class="v num sens">${tgt ? esc(money(tgt, d.currency)) : "—"}</div></div><div><div class="eyebrow">Committed</div><div class="v num sens">${esc(money(s.committed, d.currency))}</div></div><div><div class="eyebrow">Indicated</div><div class="v num sens">${esc(money(s.indicated, d.currency))}</div></div></div>
    <div class="progress" title="${Math.round(pc)}% committed, ${Math.round(pi)}% indicated"><i class="c" style="width:${pc}%"></i><i class="s" style="width:${pi}%"></i></div>
    <div class="stagecount">${STAGES.map((st) => `<span style="border-top-color:${s.counts[st] ? STAGE_COLOR[st] : "var(--line-2)"}"><b>${s.counts[st]}</b>${st === "In discussion" ? "Discussion" : st}</span>`).join("")}</div>
    <div class="row between hint"><span>${d.closeDate ? "Target close " + esc(fmtDate(d.closeDate)) : "No close date"}</span><span>${esc(userName(d.owner) || "")}</span></div></article>`; }).join("")}</div>`;
};
Object.assign(ACT, {
  "deals-view": (el) => { S.ui.deals.view = el.dataset.k; S.ui.deals.stages = null; render(); },
  "d-filter": (el) => { S.ui.deals[el.dataset.k] = el.value; if (el.dataset.k === "stage") S.ui.deals.stages = null; render(); },
  "deal-new": () => dealForm(),
  "deal-from-doc": () => { dealForm(); setTimeout(() => $("#df-file")?.click(), 0); },
  "deal-import": (el) => showMenu(el, [{ label: "Deals (one row per deal)", run: () => simpleImport("deals") }, { label: "Pipeline (one row per investor per deal)", run: () => simpleImport("pipeline") }]),
});

/* ---------------- Deal detail ---------------- */
VIEWS.deal = () => {
  const d = S.deals.get(S.route.id);
  if (!d) return S.loaded.deals ? `<div class="empty"><div class="h-section">Deal not found.</div><a href="#/deals">Back to deals</a></div>` : `<div class="empty"><span class="spin"></span></div>`;
  const s = dealStats(d); const tab = S.ui.dealTab; const tgt = d.target || 0;
  const docs = [...S.docs.values()].filter((x) => x.dealId === d.id);
  const intros = [...S.intros.values()].filter((i) => i.dealId === d.id);
  const head = `<div class="pagehead"><div style="min-width:0"><a href="#/deals" class="hint" style="display:inline-flex;align-items:center;gap:4px">${icon("back", "").replace("<svg", '<svg style="width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.5"')}All deals</a>
    <h1 class="h-display" style="margin-top:8px">${esc(d.name)}</h1><div class="sub">${esc([d.type, d.assetClass, d.subStrategy, (d.regions || []).join(", "), d.sponsor ? "Sponsor: " + d.sponsor : ""].filter(Boolean).join(" · "))}</div></div>
    <div class="row"><span class="chip ${d.status === "Live" ? "gold" : ""}">${esc(d.status)}</span><button class="btn" data-act="deal-edit" data-id="${d.id}">${icon("edit")}Edit</button><button class="btn" data-act="deal-match" data-id="${d.id}">${icon("sparkle")}Find investors</button><button class="btn primary" data-act="deal-add" data-id="${d.id}">${icon("plus")}Add investors</button></div></div>
    <div class="kpis" style="grid-template-columns:repeat(4,minmax(0,1fr))">
      <div class="kpi"><span class="eyebrow">Target</span><span class="v num sens">${tgt ? esc(money(tgt, d.currency)) : "—"}</span><span class="s sens">${d.minTicket ? "Min ticket " + esc(money(d.minTicket, d.currency)) : "No minimum set"}</span></div>
      <div class="kpi"><span class="eyebrow">Committed</span><span class="v num sens">${esc(money(s.committed, d.currency))}</span><span class="s">${tgt ? Math.round((s.committed / tgt) * 100) + "% of target" : ""}</span></div>
      <div class="kpi"><span class="eyebrow">Indicated</span><span class="v num sens">${esc(money(s.indicated, d.currency))}</span><span class="s">In discussion + diligence</span></div>
      <div class="kpi"><span class="eyebrow">Investors</span><span class="v num">${s.total}</span><span class="s">${s.counts.Passed} passed</span></div></div>
    <div class="tabs">${[["pipeline", "Pipeline"], ["details", "Details"], ["note", `Investor note${d.note ? "" : " ✦"}`], ["docs", `Documents (${docs.length})`], ["fees", `Intros & fees (${intros.length})`]].map(([k, l]) => `<button class="${tab === k ? "on" : ""}" data-act="deal-tab" data-k="${k}">${l}</button>`).join("")}</div>`;
  if (tab === "pipeline") {
    const entries = Object.entries(d.pipeline || {}).filter(([, e]) => e).map(([cid, e]) => ({ cid, e, c: S.contacts.get(cid) })).filter((x) => x.c);
    const col = (st) => { const items = entries.filter((x) => x.e.stage === st).sort((a, b) => (b.e.stageAt || 0) - (a.e.stageAt || 0)); const sum = items.reduce((t, x) => t + (st === "Committed" || st === "Closed" ? x.e.committed || x.e.indicated || 0 : x.e.indicated || 0), 0);
      return `<section class="col ${st === "Passed" ? "passed" : ""}" data-stage="${esc(st)}" data-deal="${d.id}"><div class="col-h"><div class="row"><span class="nm"><span class="stage-dot" style="background:${STAGE_COLOR[st]}"></span>${esc(st)}</span><span class="badge">${items.length}</span></div><span class="hint num sens">${sum ? esc(money(sum, d.currency)) : "&nbsp;"}</span></div>
      <div class="col-b">${items.map(({ cid, e, c }) => { const days = daysSince(e.stageAt || e.addedAt); const stalled = ACTIVE_STAGES.includes(st) && st !== "Committed" && days > (S.settings.stallDays || 21); const late = e.nextDate && e.nextDate < todayISO();
        return `<div class="ecard" draggable="true" data-act="eng-open" data-deal="${d.id}" data-cid="${cid}"><div class="row between" style="flex-wrap:nowrap"><span class="n">${esc(contactName(c))}</span>${strengthDots(c.strength)}</div><span class="o">${esc(c.organisation || c.contactType || "")}</span>${e.indicated || e.committed ? `<span class="num sens" style="font-size:13px">${esc(money(e.committed || e.indicated, e.currency || d.currency))}${e.committed ? "" : ' <span class="muted">indicated</span>'}</span>` : ""}${e.nextStep ? `<span class="ns">→ ${esc(e.nextStep)}</span>` : ""}<div class="foot"><span class="${late ? "" : "muted"}" style="${late ? "color:#EBA597" : ""}">${e.nextDate ? (late ? "Overdue " : "") + esc(fmtDate(e.nextDate, { day: "numeric", month: "short" })) : ""}</span>${stalled ? `<span class="chip warn">${days}d in stage</span>` : `<span class="muted">${days === Infinity ? "" : days + "d"}</span>`}</div></div>`; }).join("") || `<div class="hint" style="padding:8px 4px">${st === "Prospect" ? "Add investors to start." : "Drag investors here."}</div>`}</div></section>`; };
    return head + `<div class="board" id="board">${ALL_STAGES.map(col).join("")}</div><div class="hint show-m" style="margin-top:8px">Tap an investor to change their stage.</div>`;
  }
  if (tab === "details") {
    const kv = (pairs) => `<dl class="kv">${pairs.filter(([, v]) => v).map(([k, v]) => `<dt>${esc(k)}</dt><dd>${v}</dd>`).join("")}</dl>`;
    return head + `<div class="grid g2"><section class="panel"><div class="panel-h"><h2 class="h-card">Terms</h2></div><div class="panel-b">${kv([["Type", esc(d.type)], ["Asset class", esc(d.assetClass)], ["Strategy", esc(d.subStrategy)], ["Instrument", esc(d.instrument)], ["Sectors", esc((d.sectors || []).join(", "))], ["Asset location", esc((d.regions || []).join(", "))], ["Target raise", tgt ? `<span class="sens">${esc(money(tgt, d.currency, { compact: false }))}</span>` : ""], ["Minimum ticket", d.minTicket ? `<span class="sens">${esc(money(d.minTicket, d.currency, { compact: false }))}</span>` : ""], ["Target close", esc(fmtDate(d.closeDate))], ["Sponsor / counterparty", esc(d.sponsor)], ["Fee", d.feePct ? `${esc(d.feePct)}% ${esc(d.feeBasis || "of capital placed")}` : ""], ["Constraints", esc((d.constraints || []).join(", "))], ["Owner", esc(userName(d.owner))]])}</div></section>
      <section class="panel"><div class="panel-h"><h2 class="h-card">Summary</h2></div><div class="panel-b stack"><div style="white-space:pre-wrap">${esc(d.summary || "No summary yet.")}</div>${d.highlights ? `<div class="divider"></div><div class="eyebrow">Highlights</div><div style="white-space:pre-wrap" class="dim">${esc(d.highlights)}</div>` : ""}</div></section></div>`;
  }
  if (tab === "note") return head + noteTab(d);
  if (tab === "docs") return head + vaultTable(docs, { dealId: d.id });
  if (tab === "fees") return head + introsTable(intros, { dealId: d.id });
  return head;
};
Object.assign(ACT, {
  "deal-tab": (el) => { S.ui.dealTab = el.dataset.k; render(); },
  "deal-edit": (el) => dealForm(el.dataset.id),
  "deal-add": (el) => { const d = S.deals.get(el.dataset.id); pickContact(`Add investors to ${d.name}`, (list) => addToPipeline(d.id, list.map((c) => c.id)), { multi: true, exclude: Object.keys(d.pipeline || {}).filter((k) => d.pipeline[k]) }); },
  "deal-match": (el) => { const d = S.deals.get(el.dataset.id); MATCH.prefill = dealToCriteria(d); MATCH.dealId = d.id; go("#/match"); },
  "eng-open": (el) => engagementForm(el.dataset.deal, el.dataset.cid),
});
async function addToPipeline(dealId, ids) {
  const d = S.deals.get(dealId); if (!d) return;
  const add = {}; let n = 0;
  for (const id of ids) { if ((d.pipeline || {})[id]) continue; add[id] = { stage: "Prospect", currency: d.currency || "GBP", addedAt: now(), addedBy: S.me.id, stageAt: now(), hist: { [uid("h")]: { stage: "Prospect", at: now(), by: S.me.id } } }; n++; }
  if (!n) { toast("Those investors are already on this deal."); return; }
  await patch("deals", dealId, { pipeline: add }, `Added ${plural(n, "investor")} to ${d.name}`);
  toast(`${plural(n, "investor")} added to ${d.name}.`);
}
function dealToCriteria(d) { return { assetClass: d.assetClass || "", subStrategy: d.subStrategy || "", sectors: d.sectors || [], regions: d.regions || [], size: d.target || "", currency: d.currency || "GBP", minTicket: d.minTicket || "", instrument: d.instrument || "", constraints: d.constraints || [], brief: [d.name, d.summary, d.highlights].filter(Boolean).join("\n\n") }; }

/* Drag and drop between stages */
let dragInfo = null;
document.addEventListener("dragstart", (e) => { const c = e.target.closest?.(".ecard"); if (!c) return; dragInfo = { deal: c.dataset.deal, cid: c.dataset.cid }; c.classList.add("dragging"); e.dataTransfer.effectAllowed = "move"; try { e.dataTransfer.setData("text/plain", c.dataset.cid); } catch {} });
document.addEventListener("dragend", (e) => { e.target.closest?.(".ecard")?.classList.remove("dragging"); $$(".col.over").forEach((x) => x.classList.remove("over")); });
document.addEventListener("dragover", (e) => { const col = e.target.closest?.(".col"); if (!col || !dragInfo) return; e.preventDefault(); $$(".col.over").forEach((x) => x !== col && x.classList.remove("over")); col.classList.add("over"); });
document.addEventListener("drop", async (e) => {
  const col = e.target.closest?.(".col"); if (!col || !dragInfo) return; e.preventDefault(); col.classList.remove("over");
  const { deal, cid } = dragInfo; dragInfo = null; const st = col.dataset.stage;
  const d = S.deals.get(deal); const cur = d?.pipeline?.[cid]; if (!cur || cur.stage === st) return;
  if (st === "Passed") return engagementForm(deal, cid, { stage: "Passed" });
  if ((st === "Committed" || st === "Closed") && !cur.committed) return engagementForm(deal, cid, { stage: st });
  await setStage(deal, cid, st);
});
async function setStage(dealId, cid, stage, extra = {}) {
  const d = S.deals.get(dealId); const c = S.contacts.get(cid);
  await patch("deals", dealId, { pipeline: { [cid]: { ...extra, stage, stageAt: now(), hist: { [uid("h")]: { stage, at: now(), by: S.me.id } } } } }, `${contactName(c)} moved to ${stage} on ${d.name}`);
  if (c && toTime(todayISO()) > toTime(c.lastContacted)) S.db.doc(`contacts/${cid}`).update({ lastContacted: todayISO() }).catch(() => {});
}
function engagementForm(dealId, cid, preset = {}) {
  const d = S.deals.get(dealId); const c = S.contacts.get(cid); const e = { ...(d.pipeline || {})[cid], ...preset };
  const hist = Object.values(e.hist || {}).filter(Boolean).sort((a, b) => b.at - a.at);
  const m = openModal(modalShell(esc(contactName(c)), `
    <div class="fgrid" style="margin-top:14px">
      ${fSelect("en-stage", "Stage", ALL_STAGES, e.stage, { blank: null })}${fSelect("en-cur", "Currency", CURRENCIES, e.currency || d.currency || "GBP", { blank: null })}
      ${fInput("en-ind", "Indicated amount", e.indicated ? String(e.indicated) : "", { ph: "e.g. 5m" })}${fInput("en-com", "Committed amount", e.committed ? String(e.committed) : "", { ph: "e.g. 4.5m" })}
      ${fInput("en-ns", "Next step", e.nextStep, { ph: "e.g. Send IM, book site visit" })}${fInput("en-nd", "Next step date", e.nextDate, { type: "date" })}
      <div class="span2" id="en-pass-wrap" ${e.stage === "Passed" ? "" : "hidden"}>${fSelect("en-pass", "Reason for passing", ["Outside mandate", "Ticket size", "Timing", "Valuation / terms", "Sponsor / team", "Already exposed", "No response", "Other"], e.passReason)}</div>
      ${fText("en-notes", "Notes on this investor for this deal", e.notes, { cls: "span2", rows: 3, ph: "Appetite, conditions, questions raised…" })}
    </div>
    ${hist.length ? `<div class="stack" style="margin-top:20px"><div class="eyebrow">Stage history</div><div class="timeline">${hist.map((h) => `<div class="tl"><span class="pt" style="border-color:${STAGE_COLOR[h.stage]}"></span><div><div class="body">${esc(h.stage)}</div><div class="meta">${esc(fmtTime(h.at))} · ${esc(userName(h.by))}</div></div></div>`).join("")}</div></div>` : ""}`,
    `<button class="btn ghost" id="en-remove" style="margin-right:auto">Remove from deal</button><button class="btn ghost" id="en-profile">Open profile</button><button class="btn primary" id="en-save">Save</button>`,
    `${esc(d.name)} · ${esc(c.organisation || "")}`), { size: "" });
  $("#en-stage", m).onchange = (ev) => ($("#en-pass-wrap", m).hidden = ev.target.value !== "Passed");
  $("#en-profile", m).onclick = () => { closeModal(); openDrawer("contact", cid); };
  $("#en-remove", m).onclick = async () => { if (!(await confirmBox("Remove from this deal?", `${esc(contactName(c))} will be taken off the ${esc(d.name)} pipeline. Their history on it is lost.`, "Remove", true))) return; closeModal(); await patch("deals", dealId, { pipeline: { [cid]: null } }, `Removed ${contactName(c)} from ${d.name}`); };
  $("#en-save", m).onclick = async () => {
    const stage = val(m, "en-stage"); const prev = (d.pipeline || {})[cid] || {};
    const upd = { currency: val(m, "en-cur"), indicated: numVal(m, "en-ind"), committed: numVal(m, "en-com"), nextStep: val(m, "en-ns"), nextDate: val(m, "en-nd"), notes: val(m, "en-notes"), passReason: stage === "Passed" ? val(m, "en-pass") : "" };
    closeModal();
    if (stage !== prev.stage) await setStage(dealId, cid, stage, upd);
    else await patch("deals", dealId, { pipeline: { [cid]: upd } }, `Updated ${contactName(c)} on ${d.name}`);
    toast("Saved.");
  };
}

/* ---------------- Deal form ---------------- */
function dealForm(id, preset = {}) {
  const d = id ? { ...S.deals.get(id) } : { status: "Live", currency: "GBP", type: DEAL_TYPES[0], owner: S.me.id, ...preset };
  const subs = ASSET_CLASSES[d.assetClass] || [];
  const m = openModal(modalShell(id ? "Edit deal" : "New deal", `
    <div class="fsec" style="gap:10px">
      <label class="drop" for="df-file" id="df-drop" style="padding:18px"><strong>${icon("sparkle")} ${id ? "Update from a newer document" : "Autofill from a pitch deck, teaser or IM"}</strong><span>PDF, PowerPoint, Word, Excel or an image. ${S.sample ? "Claude reads it and fills in the deal for you to check." : "Basic details are filled in; turn on AI for full extraction."}${id ? " Only empty fields are filled." : ""}</span></label>
      <input type="file" id="df-file" hidden accept=".pdf,.pptx,.docx,.xlsx,.xls,.txt,.md,.png,.jpg,.jpeg,.webp">
      <div class="hint" id="df-auto-status" aria-live="polite"></div>
      <label class="check" id="df-keep-wrap" hidden><input type="checkbox" id="df-keep" checked> Save the document to this deal's Vault</label>
      <label class="check" id="df-note-wrap" hidden><input type="checkbox" id="df-note" checked> Also write the investor note from this document</label>
    </div>
    <div class="fsec"><div class="fgrid">
      ${fInput("df-name", "Deal name", d.name, { cls: "span2", ph: "e.g. Project Atlas · UK logistics portfolio" })}
      ${fSelect("df-type", "Deal type", DEAL_TYPES, d.type, { blank: null })}${fSelect("df-status", "Status", DEAL_STATUS, d.status, { blank: null })}
      ${fSelect("df-ac", "Asset class", ASSET_KEYS, d.assetClass)}<div id="df-sub-wrap">${fSelect("df-sub", "Strategy", subs, d.subStrategy)}</div>
      ${fSelect("df-inst", "Instrument", INSTRUMENTS, d.instrument)}${fInput("df-sponsor", "Sponsor / counterparty", d.sponsor)}
    </div>
    ${fPick("df-regions", "Asset location", REGIONS, d.regions)}${fPick("df-sectors", "Sectors", SECTORS, d.sectors)}${fPick("df-cons", "Investor constraints this deal satisfies", CONSTRAINTS, d.constraints)}</div>
    <div class="fsec"><div class="fgrid">
      ${fInput("df-target", "Target raise", d.target ? String(d.target) : "", { ph: "e.g. 40m" })}${fSelect("df-cur", "Currency", CURRENCIES, d.currency, { blank: null })}
      ${fInput("df-min", "Minimum ticket", d.minTicket ? String(d.minTicket) : "", { ph: "e.g. 1m" })}${fInput("df-close", "Target close", d.closeDate, { type: "date" })}
      ${fInput("df-fee", "Fee %", d.feePct ?? "", { type: "number", attrs: 'step="0.05" min="0"' })}${fInput("df-feeb", "Fee basis", d.feeBasis, { ph: "e.g. of capital placed, paid at close" })}
      ${fSelect("df-owner", "Deal owner", userOpts(), d.owner, { blank: "Unassigned" })}
    </div></div>
    <div class="fsec">${fText("df-sum", "Summary", d.summary, { rows: 4, ph: "The investment case in a few lines" })}${fText("df-hi", "Highlights", d.highlights, { rows: 3, ph: "Returns, yield, hold period, sponsor track record…" })}</div>`,
    `${id && isAdmin() ? `<button class="btn danger" id="df-del" style="margin-right:auto">Delete deal</button>` : ""}<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="df-save">${id ? "Save" : "Create deal"}</button>`), { size: "wide" });
  $("#df-ac", m).onchange = (e) => { $("#df-sub-wrap", m).innerHTML = fSelect("df-sub", "Strategy", ASSET_CLASSES[e.target.value] || [], ""); };
  $("#df-file", m).onchange = (e) => { const f = e.target.files[0]; if (f) autofillDeal(m, f, !!id); e.target.value = ""; };
  const ddrop = $("#df-drop", m);
  ["dragover", "dragenter"].forEach((ev) => ddrop.addEventListener(ev, (e) => { e.preventDefault(); ddrop.classList.add("over"); }));
  ["dragleave", "drop"].forEach((ev) => ddrop.addEventListener(ev, (e) => { e.preventDefault(); ddrop.classList.remove("over"); }));
  ddrop.addEventListener("drop", (e) => { const f = e.dataTransfer.files[0]; if (f) autofillDeal(m, f, !!id); });
  m.addEventListener("input", (e) => e.target.classList?.remove("filled"));
  if ($("#df-del", m)) $("#df-del", m).onclick = async () => { if (!(await confirmBox("Delete this deal?", "The deal, its pipeline and its stage history are removed permanently. Documents stay in the Vault.", "Delete deal", true))) return; closeModal(); await remove("deals", id, `Deleted deal ${d.name}`); go("#/deals"); };
  $("#df-save", m).onclick = async () => {
    const name = val(m, "df-name"); if (!name) { toast("Give the deal a name.", true); return; }
    const data = { ...(id ? S.deals.get(id) : { pipeline: {} }), ...preset, name, type: val(m, "df-type"), status: val(m, "df-status"), assetClass: val(m, "df-ac"), subStrategy: val(m, "df-sub"), instrument: val(m, "df-inst"), sponsor: val(m, "df-sponsor"), regions: pickVal(m, "df-regions"), sectors: pickVal(m, "df-sectors"), constraints: pickVal(m, "df-cons"), target: numVal(m, "df-target"), currency: val(m, "df-cur"), minTicket: numVal(m, "df-min"), closeDate: val(m, "df-close"), feePct: parseFloat(val(m, "df-fee")) || null, feeBasis: val(m, "df-feeb"), owner: val(m, "df-owner"), summary: val(m, "df-sum"), highlights: val(m, "df-hi") };
    delete data.id; delete data.pipelineAdd;
    if (!id && preset.pipelineAdd) { data.pipeline = {}; for (const cid of preset.pipelineAdd) data.pipeline[cid] = { stage: "Prospect", currency: data.currency || "GBP", addedAt: now(), addedBy: S.me.id, stageAt: now(), hist: { [uid("h")]: { stage: "Prospect", at: now(), by: S.me.id } } }; }
    const nid = id || uid("d_");
    try {
      if (id) { const body = clean(stamp(data, false)); delete body.pipeline; await withRetry(() => S.db.doc(`deals/${id}`).update(body)); audit("update", "deals", id, `Updated deal ${name}`); }
      else await put("deals", nid, data, `Created deal ${name}`);
      const noteFile = m._autoFile && S.sample && $("#df-note", m)?.checked && !$("#df-note-wrap", m).hidden ? m._autoFile : null;
      const keepFile = m._autoFile && S.assets && $("#df-keep", m)?.checked && !$("#df-keep-wrap", m).hidden ? m._autoFile : null;
      closeModal(); toast(id ? "Deal saved." : "Deal created.");
      if (noteFile) { S.ui.dealTab = "note"; setTimeout(() => generateNote(nid, { file: noteFile, onStep: (t) => { const el = $("#note-step"); if (el) el.textContent = t; } }).then(() => render()), 50); }
      if (keepFile) {
        S.assets.upload(keepFile).then((r) => put("docs", uid("x_"), { name: keepFile.name, category: docCategoryFor(keepFile.name), dealId: nid, contactId: "", notes: "Used to create this deal", assetId: r.id, contentType: r.contentType, size: r.sizeBytes, uploadedAt: now(), uploadedBy: S.me.id, sentTo: {} }, `Uploaded ${keepFile.name} to the vault`)).then(() => toast(`${keepFile.name} saved to the Vault.`)).catch(() => toast("The document couldn't be saved to the Vault. Upload it there manually.", true));
      }
      go(`#/deals/${nid}`);
    } catch {}
  };
}

/* ---------------- Autofill a deal from a pitch deck, teaser, IM or term sheet ---------------- */
const shortNum = (v) => { v = Number(v) || 0; if (!v) return ""; if (v >= 1e9) return +(v / 1e9).toFixed(2) + "bn"; if (v >= 1e6) return +(v / 1e6).toFixed(2) + "m"; if (v >= 1e3) return +(v / 1e3).toFixed(1) + "k"; return String(v); };
function docCategoryFor(name) { const n = name.toLowerCase(); if (/nda|non.?disclosure|confidentiality/.test(n)) return "NDA"; if (/term.?sheet|\bts\b/.test(n)) return "Term sheet"; if (/\bim\b|memorandum|\bcim\b/.test(n)) return "Information memorandum"; if (/teaser|one.?pager/.test(n)) return "Teaser"; if (/model|\.xlsx?$/.test(n)) return "Financial model"; return "Pitch deck"; }
async function extractDeal(file, onStep) {
  const canImg = S.sample ? !!(await S.sample.limits().catch(() => null))?.images : false;
  onStep("Reading the document…");
  const { text, images } = await readDocument(file, { wantImages: canImg });
  if (!text && !images.length) throw Object.assign(new Error("empty"), { code: "empty" });
  if (!S.sample) {
    // Without AI: take what simple rules can find.
    const size = (text.match(/(?:rais\w*|target\w*|seeking|round|size)[^.\n]{0,60}?([£$€]\s?\d[\d.,]*\s?(?:bn|m|k|million|billion)?)/i) || [])[1];
    const min = (text.match(/min\w*\s*(?:ticket|investment|commitment)[^.\n]{0,30}?([£$€]\s?\d[\d.,]*\s?(?:bn|m|k|million)?)/i) || [])[1];
    const t = parseMoneyRange(size), mt = parseMoneyRange(min);
    return { fields: { name: file.name.replace(/\.[^.]+$/, "").replace(/[_-]+/g, " ").trim(), assetClass: parseAssetClasses(text.slice(0, 5000))[0] || "", sectors: parseSectors(text.slice(0, 5000)), regions: parseRegions(text.slice(0, 5000)), target: t.min, currency: t.cur || mt.cur, minTicket: mt.min, summary: text.slice(0, 1200) }, ai: false };
  }
  onStep(images.length && !text ? "Claude is reading the pages…" : "Claude is reading the document…");
  const prompt = `You are reading an investment document (pitch deck, teaser, information memorandum or term sheet) for a principal who places, co-invests and allocates private capital. Extract the deal so it can be registered in a CRM. Use only what the document supports and leave a key out when it isn't there. Never invent figures.
Return one JSON object with these keys:
name (short project or company name),
type (one of: ${DEAL_TYPES.join(" | ")}; "Placement (fee-based)" when a company or sponsor is raising from outside investors, "Co-invest / Syndication" when a lead investor invites co-investors, "Direct allocation" when it is an opportunity for the principal alone),
assetClass (one of: ${ASSET_KEYS.join(" | ")}),
subStrategy (one of the strategies for that asset class: ${ASSET_KEYS.map((a) => `${a}: ${ASSET_CLASSES[a].join(", ")}`).join("; ")}),
instrument (one of: ${INSTRUMENTS.join(" | ")}),
sponsor (the company, GP or sponsor raising),
regions (where the asset or company is; array from: ${REGIONS.join(" | ")}),
sectors (array from: ${SECTORS.join(" | ")}),
constraints (array, only if the document explicitly says the deal is: ${CONSTRAINTS.slice(0, 3).join(" | ")}),
target (total raise as a number in full units), currency (ISO code), minTicket (number in full units),
closeDate (YYYY-MM-DD, only if a close date or deadline is stated),
feePct (number, only if a placement or arrangement fee is stated),
summary (3 to 5 sentences: what it is, the opportunity, use of funds, stage),
highlights (4 to 8 short lines separated by newlines: returns or IRR or multiple, yield, valuation, revenue or EBITDA, growth, hold period, track record, security),
missing (array of important items the document does not state, for example "minimum ticket", "valuation", "close date"),
confidence ("high" | "medium" | "low").
Write in UK English and do not use em dashes.
${images.length ? `The document's pages are attached as images.${text ? " Its extracted text follows too." : ""}` : ""}
DOCUMENT (${file.name}):
"""${text.slice(0, 45000)}"""`;
  const out = await S.sample.json(prompt, { modelTier: "default", ...(images.length ? { images } : {}) });
  audit("ai", "deals", "", `Read ${file.name} to autofill a new deal`);
  return { fields: out && typeof out === "object" ? out : {}, ai: true };
}
function applyDealFields(m, f, onlyEmpty) {
  let n = 0;
  const mark = (el) => { el.classList.add("filled"); n++; };
  const setVal = (id, v) => { const el = $("#" + id, m); if (!el || v === undefined || v === null || v === "") return; if (onlyEmpty && el.value) return; if (el.tagName === "SELECT" && ![...el.options].some((o) => o.value === String(v))) return; el.value = String(v); mark(el); };
  const setPick = (id, list, allowed) => { const box = $("#" + id, m); if (!box || !Array.isArray(list)) return; const want = new Set(list.filter((x) => allowed.includes(x))); if (!want.size) return; if (onlyEmpty && $$("button.on", box).length) return; $$("button", box).forEach((b) => { const on = want.has(b.dataset.v); b.classList.toggle("on", on); b.setAttribute("aria-pressed", on); }); mark(box); };
  setVal("df-name", f.name); setVal("df-type", DEAL_TYPES.includes(f.type) ? f.type : ""); setVal("df-inst", INSTRUMENTS.includes(f.instrument) ? f.instrument : ""); setVal("df-sponsor", f.sponsor);
  if (ASSET_CLASSES[f.assetClass] && !(onlyEmpty && val(m, "df-ac"))) { $("#df-ac", m).value = f.assetClass; mark($("#df-ac", m)); $("#df-sub-wrap", m).innerHTML = fSelect("df-sub", "Strategy", ASSET_CLASSES[f.assetClass], ""); }
  setVal("df-sub", (ASSET_CLASSES[val(m, "df-ac")] || []).includes(f.subStrategy) ? f.subStrategy : "");
  setPick("df-regions", f.regions, REGIONS); setPick("df-sectors", f.sectors, SECTORS); setPick("df-cons", f.constraints, CONSTRAINTS);
  setVal("df-target", shortNum(f.target)); setVal("df-cur", CURRENCIES.includes(f.currency) ? f.currency : ""); setVal("df-min", shortNum(f.minTicket));
  setVal("df-close", /^\d{4}-\d{2}-\d{2}$/.test(f.closeDate || "") ? f.closeDate : ""); setVal("df-fee", Number(f.feePct) > 0 ? f.feePct : "");
  setVal("df-sum", f.summary); setVal("df-hi", Array.isArray(f.highlights) ? f.highlights.join("\n") : f.highlights);
  return n;
}
async function autofillDeal(m, file, onlyEmpty) {
  const st = $("#df-auto-status", m); const drop = $("#df-drop", m);
  const step = (t) => { st.innerHTML = `<span class="spin"></span> ${esc(t)}`; };
  drop.classList.add("over");
  try {
    const { fields, ai } = await extractDeal(file, step);
    const n = applyDealFields(m, fields, onlyEmpty);
    m._autoFile = file;
    const missing = Array.isArray(fields.missing) && fields.missing.length ? ` Not in the document: ${fields.missing.slice(0, 5).join(", ")}.` : "";
    st.innerHTML = n ? `${icon("sparkle")} Filled ${n} field${n === 1 ? "" : "s"} from <strong>${esc(file.name)}</strong>${ai ? "" : " using simple rules (AI is off)"}. Check the highlighted fields before saving.${esc(missing)}${fields.confidence === "low" ? " Claude wasn't confident about this one." : ""}` : onlyEmpty ? `Nothing new in ${esc(file.name)}: the details it covers are already filled in.` : `Couldn't find deal details in ${esc(file.name)}. Fill the fields by hand.`;
    const keep = $("#df-keep-wrap", m); if (keep && S.assets) keep.hidden = false;
    const nw = $("#df-note-wrap", m); if (nw && S.sample) nw.hidden = false;
  } catch (e) {
    st.textContent = e && e.code === "unsupported" ? "That file type can't be read. Use PDF, PowerPoint (.pptx), Word (.docx), Excel, text or an image." : e && e.code === "empty" ? "No readable text was found in that document." : S.sample && e && e.code ? aiErr(e) : "That document couldn't be read. Try a PDF export.";
  } finally { drop.classList.remove("over"); }
}
/* =========================================================
   AI Matcher: deal brief → most relevant investors
   ========================================================= */
const MATCH = { criteria: { brief: "", assetClass: "", subStrategy: "", sectors: [], regions: [], investorBase: [], size: "", currency: "GBP", minTicket: "", instrument: "", constraints: [], name: "" }, results: null, running: false, steps: null, sel: new Set(), dealId: "", prefill: null, mode: "" };
const STOP = new Set("the and for with that this from into their over under about which will have been are was were has had its our your they them than then also only more most such very into onto upon deal fund capital investment investors investor opportunity company business target raise million billion return returns year years".split(" "));
function keywords(text) { return uniq(norm(text).split(" ").filter((w) => w.length > 4 && !STOP.has(w))).slice(0, 60); }

function scoreContact(c, cr, kw) {
  if (c.archived) return null;
  let s = 0; const why = [], warn = [];
  const assets = c.assetClasses || [];
  if (cr.assetClass) { if (assets.includes(cr.assetClass)) { s += 30; why.push(`Invests in ${cr.assetClass}`); } else if (!assets.length) s += 6; else s -= 4; }
  if (cr.subStrategy && (c.subStrategies || []).includes(cr.subStrategy)) { s += 10; why.push(`${cr.subStrategy} focus`); }
  const secHit = (cr.sectors || []).filter((x) => (c.sectors || []).includes(x));
  if (secHit.length) { s += Math.min(12, 6 * secHit.length); why.push(`Sector fit: ${secHit.join(", ")}`); } else if ((c.sectors || []).includes("Generalist")) s += 4;
  const cReg = c.regions || []; const home = regionOf(c.country);
  if ((cr.regions || []).length) {
    const hit = cr.regions.filter((r) => cReg.includes(r));
    if (hit.length) { s += 18; why.push(`Invests in ${hit.join(", ")}`); } else if (cReg.includes("Global")) { s += 14; why.push("Global mandate"); } else if (home && cr.regions.includes(home)) { s += 10; why.push(`Based in ${home}`); } else if (!cReg.length) s += 4; else s -= 4;
  }
  if ((cr.investorBase || []).length && home && cr.investorBase.includes(home)) { s += 8; why.push(`Located in ${home}`); }
  const dMin = toGBP(numOrRange(cr.minTicket), cr.currency), dSize = toGBP(numOrRange(cr.size), cr.currency);
  const cMin = toGBP(c.ticketMin, c.ticketCurrency), cMax = toGBP(c.ticketMax || c.ticketMin, c.ticketCurrency);
  if (cMin || cMax) {
    if (dMin && cMax && cMax < dMin) { s -= 12; warn.push(`Usual ticket (${ticketText(c)}) is below the minimum`); }
    else if (dSize && cMin && cMin > dSize) { s -= 8; warn.push(`Usual ticket (${ticketText(c)}) is larger than the raise`); }
    else if (dMin || dSize) { s += 15; why.push(`Ticket ${ticketText(c)} fits`); }
  } else s += 3;
  const cons = c.constraints || []; const dc = cr.constraints || [];
  if (cons.includes("Sharia-compliant only") && !dc.includes("Sharia-compliant only")) { s -= 25; warn.push("Requires Sharia-compliant structures"); }
  if (cons.includes("ESG / impact mandate") && !dc.includes("ESG / impact mandate")) { s -= 6; warn.push("Has an ESG / impact mandate"); }
  if (cons.includes("No leverage") && /debt|credit|mezz|lever/i.test(cr.instrument + " " + cr.brief)) { s -= 6; warn.push("Avoids leverage"); }
  const st = c.structures || [];
  if (/fund/i.test(cr.instrument) && st.includes("Fund commitments")) { s += 6; why.push("Makes fund commitments"); }
  else if (st.includes("Co-investments") || st.includes("Direct deals") || st.includes("Club deals")) { s += 5; }
  s += ((c.strength || 2) - 2) * 2;
  if (kw.length) { const hay = norm([c.notes, c.mandateNotes, (c.tags || []).join(" "), c.organisation].join(" ")); const hits = kw.filter((w) => hay.includes(w)); if (hits.length) { s += Math.min(8, hits.length * 2); why.push(`Notes mention ${hits.slice(0, 3).join(", ")}`); } }
  if (c.doNotContact) { s -= 40; warn.push("Marked do not contact"); }
  return { id: c.id, score: clamp(Math.round(s), 0, 99), why, warn };
}
function numOrRange(v) { if (!v) return 0; if (typeof v === "number") return v; return parseMoneyRange(v).min || 0; }
function localMatch(cr) {
  const kw = keywords(cr.brief);
  return [...S.contacts.values()].map((c) => scoreContact(c, cr, kw)).filter((x) => x && x.score > 0).sort((a, b) => b.score - a.score);
}

VIEWS.match = () => {
  if (MATCH.prefill) { MATCH.criteria = { ...MATCH.criteria, sectors: [], regions: [], investorBase: [], constraints: [], ...MATCH.prefill }; MATCH.prefill = null; MATCH.results = null; }
  const cr = MATCH.criteria; const aiOn = !!S.sample; const subs = ASSET_CLASSES[cr.assetClass] || [];
  const recent = [...S.matches.values()].sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)).slice(0, 6);
  const deal = S.deals.get(MATCH.dealId);
  return `<div class="pagehead"><div><h1 class="h-display">AI Matcher</h1><div class="sub">Describe a deal. UConnect scores every contact on mandate, geography, ticket and constraints, then Claude ranks the best and explains why.</div></div></div>
  <div class="match-grid">
    <section class="panel match-brief">
      <div class="panel-h"><h2 class="h-card">The deal</h2><div class="row" style="gap:6px"><label class="btn sm" for="m-file" title="Upload a teaser or IM">${icon("upload")}Teaser</label><input type="file" id="m-file" accept=".pdf,.pptx,.docx,.xlsx,.txt,.md" hidden data-on="m-file"><button class="btn sm ghost" data-act="m-reset">Clear</button></div></div>
      <div class="panel-b stack">
        ${fSelect("m-deal", "Match for an existing deal (optional)", dealOpts(false), MATCH.dealId, { blank: "A new opportunity", attrs: 'data-on="m-deal"' })}
        <div class="field"><label for="m-brief">Deal details</label><textarea class="input" id="m-brief" rows="7" data-in="m-set" data-k="brief" placeholder="Paste the teaser or describe it: what, where, size, minimum ticket, structure, returns, timing.">${esc(cr.brief)}</textarea></div>
        ${aiOn ? `<button class="btn sm" data-act="m-parse" ${MATCH.running ? "disabled" : ""}>${icon("sparkle")}Read the brief and fill the fields</button>` : ""}
        <div class="fgrid">
          ${fSelect("m-ac", "Asset class", ASSET_KEYS, cr.assetClass, { attrs: 'data-on="m-set" data-k="assetClass"' })}
          ${fSelect("m-sub", "Strategy", subs, cr.subStrategy, { attrs: 'data-on="m-set" data-k="subStrategy"' })}
          ${fInput("m-size", "Raise size", cr.size ? String(cr.size) : "", { ph: "e.g. 40m", attrs: 'data-in="m-set" data-k="size"' })}
          ${fSelect("m-cur", "Currency", CURRENCIES, cr.currency, { blank: null, attrs: 'data-on="m-set" data-k="currency"' })}
          ${fInput("m-min", "Minimum ticket", cr.minTicket ? String(cr.minTicket) : "", { ph: "e.g. 1m", attrs: 'data-in="m-set" data-k="minTicket"' })}
          ${fSelect("m-inst", "Instrument", INSTRUMENTS, cr.instrument, { attrs: 'data-on="m-set" data-k="instrument"' })}
        </div>
        ${fPick("m-regions", "Asset location", REGIONS, cr.regions)}
        ${fPick("m-sectors", "Sectors", SECTORS, cr.sectors)}
        ${fPick("m-base", "Prefer investors based in (optional)", REGIONS.filter((r) => r !== "Global"), cr.investorBase)}
        ${fPick("m-cons", "This deal is…", CONSTRAINTS.slice(0, 3), cr.constraints)}
        <div class="row">${aiOn ? `<button class="btn primary" data-act="m-run" style="flex:1" ${MATCH.running ? "disabled" : ""}>${icon("sparkle")}Find investors</button>` : ""}<button class="btn ${aiOn ? "" : "primary"}" data-act="m-quick" ${MATCH.running ? "disabled" : ""} title="Instant rules-based match, no AI">Quick match</button></div>
        ${!aiOn ? `<div class="hint">Claude isn't available in this view, so matching uses UConnect's scoring rules only.</div>` : ""}
      </div>
    </section>
    <section class="stack" style="gap:18px;min-width:0">${matchResultsHTML(deal)}
      ${recent.length ? `<section class="panel"><div class="panel-h"><h2 class="h-card">Recent searches</h2></div><div class="alerts">${recent.map((r) => `<div class="alert" data-act="m-load" data-id="${r.id}"><span class="sev good"></span><div style="min-width:0"><div class="t1">${esc(r.title || "Untitled search")}</div><div class="t2">${esc(userName(r.createdBy))} · ${esc(ago(r.createdAt))} · ${plural((r.results || []).length, "match", "matches")}</div></div><span class="hint">${r.ai ? "AI ranked" : "Quick"}</span></div>`).join("")}</div></section>` : ""}
    </section>
  </div>`;
};
function matchResultsHTML(deal) {
  if (MATCH.running) return `<section class="panel"><div class="panel-h"><h2 class="h-card">Working</h2><button class="btn sm ghost" data-act="m-stop">Stop</button></div><div class="steps">${(MATCH.steps || []).map((s) => `<div class="step ${s.state}"><span class="b">${s.state === "done" ? "" : ""}</span><span>${esc(s.label)}</span></div>`).join("")}</div></section>`;
  if (!MATCH.results) return `<section class="panel"><div class="empty" style="padding:48px 24px"><div class="h-section">Who should see this deal?</div><div style="max-width:480px">Paste a teaser or fill in the fields. Matching looks at asset class and strategy, sector, where they invest, where they're based, ticket size versus your minimum, structures they use, and constraints like Sharia or ESG mandates.</div>${S.contacts.size ? `<div class="hint">${S.contacts.size.toLocaleString("en-GB")} contacts will be considered.</div>` : `<button class="btn" data-act="import">${icon("upload")}Import contacts first</button>`}</div></section>`;
  const res = MATCH.results; const sel = MATCH.sel;
  const ring = (n) => { const r = 24, C = 2 * Math.PI * r; return `<div class="score" title="Match score ${n} of 100"><svg viewBox="0 0 56 56"><circle cx="28" cy="28" r="${r}" fill="none" stroke="var(--raised-2)" stroke-width="3"/><circle cx="28" cy="28" r="${r}" fill="none" stroke="${n >= 75 ? "var(--gold)" : n >= 55 ? "var(--gold-mid)" : "var(--ink-4)"}" stroke-width="3" stroke-linecap="round" stroke-dasharray="${(C * n) / 100} ${C}"/></svg><b>${n}</b></div>`; };
  return `<section class="panel"><div class="panel-h"><div><h2 class="h-card">${res.length ? `${res.length} investors to approach` : "No strong matches"}</h2><div class="hint" style="margin-top:4px">${MATCH.mode === "ai" ? "Scored by UConnect, ranked and explained by Claude" : "Scored by UConnect's matching rules"}${deal ? ` · for ${esc(deal.name)}` : ""}</div></div>
    <div class="row" style="gap:6px">${sel.size ? `<button class="btn sm primary" data-act="m-add-sel">${deal ? `Add ${sel.size} to ${esc(deal.name.slice(0, 24))}` : `Add ${sel.size} to a deal`}</button>` : `<button class="btn sm" data-act="m-selall">Select all</button>`}<button class="btn sm" data-act="m-export">${icon("download")}CSV</button></div></div>
    ${res.length ? res.map((r, i) => { const c = S.contacts.get(r.id); if (!c) return ""; const inDeal = deal && (deal.pipeline || {})[c.id];
      return `<div class="result"><div class="rank">${i + 1}</div>${ring(r.score)}<div style="min-width:0"><div class="row" style="gap:8px"><label class="check"><input type="checkbox" data-on="m-sel" data-id="${c.id}" ${sel.has(c.id) ? "checked" : ""} aria-label="Select ${esc(contactName(c))}"></label><a data-act="contact-open" data-id="${c.id}" class="rn" style="color:var(--ink);cursor:pointer">${esc(contactName(c))}</a>${r.fit ? `<span class="chip ${r.fit === "Strong" ? "gold" : ""}">${esc(r.fit)} fit</span>` : ""}${inDeal ? `<span class="chip info">On deal · ${esc(inDeal.stage)}</span>` : ""}</div>
        <div class="hint" style="margin-top:2px">${esc([c.title, c.organisation, c.contactType].filter(Boolean).join(" · "))} · ${strengthDots(c.strength)} ${esc(STRENGTH[c.strength] || "")} · ${c.owner ? "Owner: " + esc(userName(c.owner)) : "No owner"} · touched ${esc(ago(lastTouch(c)))}</div>
        <ul>${(r.why || []).slice(0, 4).map((w) => `<li>${esc(w)}</li>`).join("")}</ul>
        ${(r.warn || []).length ? `<div class="caution">Watch: ${esc(r.warn.join("; "))}</div>` : ""}
        ${r.approach ? `<div class="approach">“${esc(r.approach)}”</div>` : ""}</div>
        <div class="acts">${inDeal ? "" : `<button class="btn sm" data-act="m-add" data-id="${c.id}">Add to deal</button>`}${S.sample ? `<button class="btn sm ghost" data-act="m-draft" data-id="${c.id}">${icon("mail")}Draft note</button>` : ""}</div></div>`; }).join("") : `<div class="empty"><div>Nobody in the network clearly fits these criteria.</div><div class="hint">Try widening geography or sectors, or add mandates to more contacts.</div></div>`}</section>`;
}
Object.assign(ACT, {
  "m-set": (el) => { MATCH.criteria[el.dataset.k] = el.value; if (el.dataset.k === "assetClass") { MATCH.criteria.subStrategy = ""; render(); } },
  "m-deal": (el) => { MATCH.dealId = el.value; if (el.value) { MATCH.criteria = { ...MATCH.criteria, ...dealToCriteria(S.deals.get(el.value)) }; } MATCH.results = null; render(); },
  "m-reset": () => { MATCH.criteria = { brief: "", assetClass: "", subStrategy: "", sectors: [], regions: [], investorBase: [], size: "", currency: "GBP", minTicket: "", instrument: "", constraints: [], name: "" }; MATCH.results = null; MATCH.dealId = ""; MATCH.sel.clear(); render(); },
  "m-file": async (el) => {
    const f = el.files[0]; if (!f) return;
    try { toast("Reading document…"); const { text } = await readDocument(f); if (!text) { toast("No readable text in that document. Paste the key details instead.", true); return; } MATCH.criteria.brief = text.slice(0, 20000); audit("ai", "", "", `Loaded teaser ${f.name} into the matcher`); render(); if (S.sample) ACT["m-parse"](); else toast("Teaser loaded. Fill in the fields, then match."); }
    catch (e) { console.warn(e); toast("That document couldn't be read. Try copying the text in instead.", true); }
  },
  "m-parse": async () => { await parseBrief(); render(); },
  "m-quick": () => runMatch(false),
  "m-run": () => runMatch(true),
  "m-stop": () => { MATCH.ctl?.abort(); },
  "m-sel": (el) => { el.checked ? MATCH.sel.add(el.dataset.id) : MATCH.sel.delete(el.dataset.id); render(); },
  "m-selall": () => { (MATCH.results || []).forEach((r) => MATCH.sel.add(r.id)); render(); },
  "m-add": (el) => matchAdd([el.dataset.id]),
  "m-add-sel": () => matchAdd([...MATCH.sel]),
  "m-load": (el) => { const r = S.matches.get(el.dataset.id); if (!r) return; MATCH.criteria = { ...MATCH.criteria, ...(r.criteria || {}) }; MATCH.results = r.results || []; MATCH.mode = r.ai ? "ai" : "rules"; MATCH.dealId = r.dealId || ""; MATCH.sel.clear(); render(); window.scrollTo(0, 0); },
  "m-export": () => { const rows = (MATCH.results || []).map((r, i) => ({ ...r, rank: i + 1, c: S.contacts.get(r.id) })).filter((r) => r.c); saveFile(`uconnect-matches-${todayISO()}.csv`, toCSV(rows, [["Rank", "rank"], ["Score", "score"], ["Fit", "fit"], ["Name", (r) => contactName(r.c)], ["Organisation", (r) => r.c.organisation], ["Type", (r) => r.c.contactType], ["Email", (r) => r.c.email], ["Why", (r) => (r.why || []).join("; ")], ["Watch", (r) => (r.warn || []).join("; ")], ["Approach", "approach"], ["Owner", (r) => userName(r.c.owner)]])); },
  "m-draft": (el) => draftNote(el.dataset.id),
});
document.addEventListener("pickchange", (e) => {
  const map = { "m-regions": "regions", "m-sectors": "sectors", "m-base": "investorBase", "m-cons": "constraints" };
  const k = map[e.target.id]; if (k) MATCH.criteria[k] = pickVal(document, e.target.id);
});
function matchAdd(ids) {
  if (!ids.length) return;
  if (MATCH.dealId && S.deals.get(MATCH.dealId)) { addToPipeline(MATCH.dealId, ids).then(() => { MATCH.sel.clear(); render(); }); return; }
  const cr = MATCH.criteria;
  const m = openModal(modalShell("Add to a deal", `<div class="stack" style="margin-top:14px">${S.deals.size ? fSelect("ma-d", "Existing deal", dealOpts(true), "", { blank: "Choose a live deal" }) : ""}<div class="hint">Or create a new deal from this brief and add ${plural(ids.length, "investor")} as Prospects.</div></div>`, `<button class="btn" id="ma-new">Create new deal</button>${S.deals.size ? `<button class="btn primary" id="ma-ok">Add to deal</button>` : ""}`), { size: "narrow" });
  $("#ma-new", m).onclick = () => { closeModal(); dealForm(null, { name: cr.name || "", assetClass: cr.assetClass, subStrategy: cr.subStrategy, sectors: cr.sectors, regions: cr.regions, target: numOrRange(cr.size) || null, currency: cr.currency, minTicket: numOrRange(cr.minTicket) || null, instrument: cr.instrument, constraints: cr.constraints, summary: cr.brief.slice(0, 3000), pipelineAdd: ids }); };
  if ($("#ma-ok", m)) $("#ma-ok", m).onclick = async () => { const d = val(m, "ma-d"); if (!d) return; closeModal(); MATCH.dealId = d; await addToPipeline(d, ids); MATCH.sel.clear(); render(); };
}
async function parseBrief() {
  const cr = MATCH.criteria; if (!S.sample || !cr.brief.trim()) { if (!cr.brief.trim()) toast("Paste the deal details first."); return false; }
  MATCH.running = true; MATCH.steps = [{ label: "Reading the brief", state: "on" }]; render();
  const prompt = `Extract the investable deal terms from this brief for an investor-matching engine. Use only what the text supports.
Return one JSON object with keys:
name (short deal name), assetClass (one of: ${ASSET_KEYS.join(" | ")}), subStrategy (one of the strategies for that asset class: ${ASSET_KEYS.map((a) => `${a}: ${ASSET_CLASSES[a].join(", ")}`).join("; ")}),
sectors (array from: ${SECTORS.join(" | ")}), regions (where the asset or company is; array from: ${REGIONS.join(" | ")}),
size (total raise, number in full units), currency (ISO code), minTicket (number), instrument (one of: ${INSTRUMENTS.join(" | ")}),
constraints (array, only if the deal is explicitly: ${CONSTRAINTS.slice(0, 3).join(" | ")}).
Reply with only the JSON object.

BRIEF:
"""${cr.brief.slice(0, 20000)}"""`;
  try {
    MATCH.ctl = new AbortController();
    const o = await S.sample.json(prompt, { modelTier: "quick", signal: MATCH.ctl.signal });
    if (o && typeof o === "object") {
      if (ASSET_CLASSES[o.assetClass]) cr.assetClass = o.assetClass;
      if (o.subStrategy && (ASSET_CLASSES[cr.assetClass] || []).includes(o.subStrategy)) cr.subStrategy = o.subStrategy;
      if (Array.isArray(o.sectors)) cr.sectors = o.sectors.filter((s) => SECTORS.includes(s));
      if (Array.isArray(o.regions)) cr.regions = o.regions.filter((s) => REGIONS.includes(s));
      if (Array.isArray(o.constraints)) cr.constraints = o.constraints.filter((s) => CONSTRAINTS.includes(s));
      if (o.size) cr.size = String(o.size); if (o.minTicket) cr.minTicket = String(o.minTicket);
      if (CURRENCIES.includes(o.currency)) cr.currency = o.currency; if (INSTRUMENTS.includes(o.instrument)) cr.instrument = o.instrument;
      if (o.name) cr.name = String(o.name).slice(0, 80);
    }
    return true;
  } catch (e) { toast(aiErr(e), e.code !== "cancelled"); return false; }
  finally { MATCH.running = false; }
}
async function runMatch(useAI) {
  const cr = MATCH.criteria;
  if (!S.contacts.size) { toast("Import contacts before matching.", true); return; }
  if (!cr.assetClass && !cr.brief.trim() && !cr.regions.length && !cr.sectors.length) { toast("Describe the deal or pick an asset class first.", true); return; }
  MATCH.sel.clear();
  if (useAI && S.sample && cr.brief.trim() && !cr.assetClass) { const ok = await parseBrief(); if (!ok) { render(); return; } }
  const scored = localMatch(cr);
  if (!useAI || !S.sample) {
    MATCH.results = scored.slice(0, 25).map((x) => ({ ...x, fit: x.score >= 70 ? "Strong" : x.score >= 50 ? "Good" : "Possible" })); MATCH.mode = "rules";
    saveMatch(false); render(); return;
  }
  MATCH.running = true; MATCH.steps = [{ label: "Reading the brief", state: "done" }, { label: `Scoring ${S.contacts.size.toLocaleString("en-GB")} contacts on mandate, geography and ticket`, state: "done" }, { label: "Claude is ranking the shortlist", state: "on" }]; render();
  const short = scored.slice(0, 60);
  const lines = short.map((x) => { const c = S.contacts.get(x.id); return { id: x.id, name: contactName(c), org: c.organisation || "", type: c.contactType || "", assets: c.assetClasses || [], strategies: c.subStrategies || [], sectors: c.sectors || [], investsIn: c.regions || [], basedIn: [c.city, c.country].filter(Boolean).join(", "), ticket: ticketText(c), aum: c.aum ? money(c.aum, c.aumCurrency) : "", structures: c.structures || [], constraints: c.constraints || [], lead: c.leadPref || "", strength: STRENGTH[c.strength] || "", lastTouch: lastTouch(c) ? ago(lastTouch(c)) : "never", notes: [c.mandateNotes, c.notes].filter(Boolean).join(" ").slice(0, 280), ruleScore: x.score }; });
  const prompt = `You advise a principal who allocates, co-invests and places private capital (earning placement fees). Rank which of these contacts he should approach for the deal below.
Judge real mandate fit first (asset class, strategy, sector, where they invest, ticket versus minimum and raise size, structures, constraints such as Sharia or ESG), then practical likelihood (relationship strength, recency, notes). Penalise clear mismatches. Do not invent facts about people beyond the data given.

DEAL
${JSON.stringify({ assetClass: cr.assetClass, strategy: cr.subStrategy, sectors: cr.sectors, assetLocation: cr.regions, preferInvestorsBasedIn: cr.investorBase, raise: cr.size ? `${cr.currency} ${cr.size}` : "", minimumTicket: cr.minTicket ? `${cr.currency} ${cr.minTicket}` : "", instrument: cr.instrument, dealIs: cr.constraints })}
Brief: """${cr.brief.slice(0, 6000)}"""

CANDIDATES (ruleScore is a rough pre-score)
${JSON.stringify(lines)}

Return only a JSON array of at most 15 of the best candidates, best first, each:
{"id":"<id>","score":0-100,"fit":"Strong"|"Good"|"Possible","reasons":["2-3 short specific reasons"],"caution":"one short risk or empty string","approach":"one sentence on how to pitch it to them, UK English, no em dashes"}
Leave out anyone who is a poor fit.`;
  try {
    MATCH.ctl = new AbortController();
    const out = await S.sample.json(prompt, { modelTier: "default", signal: MATCH.ctl.signal });
    const byId = new Map(short.map((x) => [x.id, x]));
    MATCH.results = (Array.isArray(out) ? out : []).filter((o) => o && byId.has(o.id)).map((o) => ({ id: o.id, score: clamp(Math.round(Number(o.score) || byId.get(o.id).score), 0, 100), fit: ["Strong", "Good", "Possible"].includes(o.fit) ? o.fit : "", why: arr(o.reasons).map(String).slice(0, 4), warn: o.caution ? [String(o.caution)] : byId.get(o.id).warn, approach: o.approach ? String(o.approach) : "" }));
    MATCH.mode = "ai";
    audit("ai", "", "", `Ran AI match${cr.name ? " for " + cr.name : ""}: ${MATCH.results.length} investors shortlisted`);
    saveMatch(true);
  } catch (e) {
    if (e.code !== "cancelled") toast(aiErr(e) + " Showing rules-based results instead.", true);
    MATCH.results = scored.slice(0, 25).map((x) => ({ ...x, fit: x.score >= 70 ? "Strong" : x.score >= 50 ? "Good" : "Possible" })); MATCH.mode = "rules";
  } finally { MATCH.running = false; render(); }
}
function saveMatch(ai) {
  const cr = MATCH.criteria; const id = uid("m_");
  const title = cr.name || [cr.assetClass, cr.subStrategy, (cr.regions || []).join("/")].filter(Boolean).join(" · ") || "Deal search";
  S.db.doc(`matches/${id}`).set({ title, ai, dealId: MATCH.dealId || "", criteria: { ...cr, brief: cr.brief.slice(0, 4000) }, results: (MATCH.results || []).slice(0, 25), createdAt: now(), createdBy: S.me.id }).catch(() => {});
  const old = [...S.matches.values()].sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0)).slice(40);
  old.forEach((o) => S.db.doc(`matches/${o.id}`).delete().catch(() => {}));
}
function draftNote(cid) {
  const c = S.contacts.get(cid); const cr = MATCH.criteria; const r = (MATCH.results || []).find((x) => x.id === cid) || {};
  const m = openModal(modalShell(`Draft note to ${esc(contactName(c))}`, `<div class="hint" style="margin-top:12px">Written for you to edit and send yourself. UConnect doesn't send email.</div><textarea class="input" id="dn-text" rows="14" style="margin-top:10px">Thinking…</textarea>`, `<button class="btn ghost" id="dn-stop">Stop</button><button class="btn" id="dn-again">Try another</button><button class="btn primary" id="dn-copy">Copy</button>`), { size: "" });
  let ctl;
  const run = async () => {
    ctl = new AbortController(); const box = $("#dn-text", m); box.value = "Thinking…";
    const prompt = `Write a short, discreet first-approach email from a principal who places and co-invests private capital, to ${contactName(c)}${c.title ? ", " + c.title : ""}${c.organisation ? " at " + c.organisation : ""}. Relationship: ${STRENGTH[c.strength] || "unknown"}; last spoke ${lastTouch(c) ? ago(lastTouch(c)) : "not recorded"}. Their mandate: ${[(c.assetClasses || []).join(", "), (c.subStrategies || []).join(", "), (c.sectors || []).join(", "), (c.regions || []).join(", "), ticketText(c)].filter(Boolean).join("; ")}. ${c.notes ? "Context: " + c.notes.slice(0, 300) : ""}
The opportunity: ${[cr.name, cr.assetClass, cr.subStrategy, (cr.regions || []).join(", "), cr.size ? cr.currency + " " + cr.size + " raise" : "", cr.minTicket ? "minimum " + cr.currency + " " + cr.minTicket : ""].filter(Boolean).join(", ")}. ${cr.brief ? "Brief: " + cr.brief.slice(0, 1500) : ""}
Why it suits them: ${(r.why || []).join("; ")}.
Requirements: UK English, warm and concise (under 150 words), a subject line first, no confidential figures beyond those given, offer an NDA and a short call, sign off as [Your name]. Do not use em dashes.`;
    try { await S.sample(prompt, { signal: ctl.signal, cache: false, onText: ({ text }) => { box.value = text; } }); audit("ai", "contacts", cid, `Drafted an outreach note to ${contactName(c)}`); }
    catch (e) { if (e.code !== "cancelled") box.value = (e.text || "") + "\n\n" + aiErr(e); }
  };
  run();
  $("#dn-stop", m).onclick = () => ctl?.abort();
  $("#dn-again", m).onclick = () => { ctl?.abort(); run(); };
  $("#dn-copy", m).onclick = async () => { try { await navigator.clipboard.writeText($("#dn-text", m).value); toast("Copied."); } catch { $("#dn-text", m).select(); toast("Select all and copy."); } };
  const top = modalStack[modalStack.length - 1]; const oc = top.onClose; top.onClose = () => { ctl?.abort(); oc && oc(); };
}
/* =========================================================
   Intros & fees · Tasks · Vault · Security · Settings
   ========================================================= */

/* ---------------- Intros & fees ---------------- */
function introFee(i) {
  if (i.feeAmount) return toGBP(i.feeAmount, i.currency);
  if (i.feePct && i.dealId && i.contactId) { const d = S.deals.get(i.dealId); const e = d && (d.pipeline || {})[i.contactId]; if (e && (e.committed || e.indicated)) return toGBP(((e.committed || e.indicated) * i.feePct) / 100, e.currency || d.currency); }
  return 0;
}
function feeTotals() {
  let expected = 0, received = 0, invoiced = 0, payable = 0;
  for (const i of S.intros.values()) {
    const f = introFee(i); if (!f || i.status === "Waived") continue;
    if (i.direction === INTRO_DIR[1]) { if (i.status !== "Received") payable += f; continue; }
    if (i.status === "Received") received += f; else { expected += f; if (i.status === "Invoiced") invoiced += f; }
  }
  return { expected, received, invoiced, payable };
}
function introTitle(i) {
  const who = contactName(S.contacts.get(i.contactId)) || i.contactText || "someone";
  const by = contactName(S.contacts.get(i.introducerId)) || i.introducerText || "us";
  const d = S.deals.get(i.dealId);
  return i.direction === INTRO_DIR[1] ? `${by} introduced ${who} to us${d ? " for " + d.name : ""}` : `We introduced ${who}${d ? " to " + d.name : ""}`;
}
function introsTable(list, ctx = {}) {
  list = list.slice().sort((a, b) => (b.date || "").localeCompare(a.date || ""));
  return `<div class="toolbar"><span class="spacer"></span><button class="btn" data-act="intro-import">${icon("upload")}Import</button><button class="btn primary" data-act="intro-new" data-deal="${ctx.dealId || ""}">${icon("plus")}Record introduction</button></div>
  ${list.length ? `<div class="tablewrap"><table class="t"><thead><tr><th>Date</th><th>Introduction</th><th>Agreement</th><th class="r">Fee</th><th>Status</th></tr></thead><tbody>${list.map((i) => { const f = introFee(i); return `<tr data-act="intro-edit" data-id="${i.id}"><td class="dim num">${esc(fmtDate(i.date))}</td><td><div class="name">${esc(introTitle(i))}</div><div class="org">${i.direction === INTRO_DIR[1] ? "Fee payable by us" : "Fee receivable"}${i.feePct ? ` · ${esc(i.feePct)}%` : ""}${i.example ? " · Example" : ""}</div></td><td><span class="chip ${i.agreement === "Signed" ? "good" : i.agreement === "None" || !i.agreement ? "crit" : "warn"}">${esc(i.agreement || "None")}</span></td><td class="r num sens">${f ? esc(money(f)) : '<span class="muted">—</span>'}</td><td><span class="chip ${i.status === "Received" ? "good" : i.status === "Invoiced" ? "gold" : i.status === "Disputed" ? "crit" : ""}">${esc(i.status || "Not yet due")}</span></td></tr>`; }).join("")}</tbody></table></div>` : `<div class="panel"><div class="empty"><div class="h-section">No introductions recorded.</div><div style="max-width:460px">Record every introduction, who made it and the fee agreed. Unsigned fee agreements are flagged so nothing is left on the table.</div></div></div>`}`;
}
VIEWS.intros = () => {
  const t = feeTotals(); const list = [...S.intros.values()];
  const unsigned = list.filter((i) => i.direction !== INTRO_DIR[1] && (!i.agreement || i.agreement === "None" || i.agreement === "Verbal")).length;
  return `${exampleBanner()}<div class="pagehead"><div><h1 class="h-display">Intros &amp; Fees</h1><div class="sub">Every introduction, both directions, with the economics attached. Amounts in GBP.</div></div></div>
  <div class="kpis" style="grid-template-columns:repeat(4,minmax(0,1fr))"><div class="kpi"><span class="eyebrow">Receivable</span><span class="v num sens">${money(t.expected)}</span><span class="s sens">${money(t.invoiced)} invoiced</span></div><div class="kpi"><span class="eyebrow">Received</span><span class="v num sens">${money(t.received)}</span><span class="s">All time</span></div><div class="kpi"><span class="eyebrow">Payable by us</span><span class="v num sens">${money(t.payable)}</span><span class="s">Finder's fees owed</span></div><div class="kpi"><span class="eyebrow">Unprotected</span><span class="v num">${unsigned}</span><span class="s">${unsigned ? "No signed fee agreement" : "All agreements in writing"}</span></div></div>
  ${introsTable(list)}`;
};
Object.assign(ACT, {
  "intro-new": (el) => introForm(null, { dealId: el?.dataset?.deal || "" }),
  "intro-edit": (el) => introForm(el.dataset.id),
  "intro-import": () => simpleImport("intros"),
});
function introForm(id, preset = {}) {
  const i = id ? { ...S.intros.get(id) } : { date: todayISO(), direction: INTRO_DIR[0], agreement: "None", status: "Not yet due", currency: "GBP", ...preset };
  const m = openModal(modalShell(id ? "Introduction" : "Record an introduction", `<div class="fgrid" style="margin-top:14px">
    ${fSelect("in-dir", "Direction", INTRO_DIR, i.direction, { blank: null, cls: "span2" })}
    ${fSelect("in-by", "Introducer", contactOpts(), i.introducerId, { blank: "Us / the principal" })}${fInput("in-bytxt", "…or introducer not in UConnect", i.introducerText)}
    ${fSelect("in-who", "Person introduced", contactOpts(), i.contactId, { blank: "—" })}${fInput("in-whotxt", "…or person not in UConnect", i.contactText)}
    ${fSelect("in-deal", "Deal", dealOpts(false), i.dealId, { blank: "Not deal-specific" })}${fInput("in-date", "Date", i.date, { type: "date" })}
    ${fSelect("in-ag", "Fee agreement", FEE_AGREEMENT, i.agreement, { blank: null })}${fSelect("in-st", "Fee status", FEE_STATUS, i.status, { blank: null })}
    ${fInput("in-pct", "Fee %", i.feePct ?? "", { type: "number", attrs: 'step="0.05" min="0"' })}${fInput("in-amt", "Fixed fee amount", i.feeAmount ? String(i.feeAmount) : "", { ph: "Leave blank to calculate from %" })}
    ${fSelect("in-cur", "Currency", CURRENCIES, i.currency, { blank: null })}${fInput("in-inv", "Invoice / reference", i.invoiceRef)}
    ${fText("in-notes", "Notes", i.notes, { cls: "span2", rows: 2 })}</div>
    <div class="hint" style="margin-top:10px">With a fee % and a deal, the fee is calculated from the investor's committed amount (or indicated, until they commit).</div>`,
    `${id && isAdmin() ? `<button class="btn danger" id="in-del" style="margin-right:auto">Delete</button>` : ""}<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="in-save">Save</button>`), { size: "" });
  if ($("#in-del", m)) $("#in-del", m).onclick = async () => { if (!(await confirmBox("Delete this introduction?", "The fee record goes with it.", "Delete", true))) return; closeModal(); await remove("intros", id, "Deleted an introduction"); };
  $("#in-save", m).onclick = async () => {
    const data = { ...(id ? S.intros.get(id) : {}), direction: val(m, "in-dir"), introducerId: val(m, "in-by"), introducerText: val(m, "in-bytxt"), contactId: val(m, "in-who"), contactText: val(m, "in-whotxt"), dealId: val(m, "in-deal"), date: val(m, "in-date"), agreement: val(m, "in-ag"), status: val(m, "in-st"), feePct: parseFloat(val(m, "in-pct")) || null, feeAmount: numVal(m, "in-amt"), currency: val(m, "in-cur"), invoiceRef: val(m, "in-inv"), notes: val(m, "in-notes") };
    delete data.id;
    try { await put("intros", id || uid("i_"), data, `${id ? "Updated" : "Recorded"} introduction: ${introTitle(data)}`); closeModal(); toast("Saved."); } catch {}
  };
}

/* ---------------- Tasks ---------------- */
VIEWS.tasks = () => {
  const who = S.ui.tasks.who; const today = todayISO(); const wk = new Date(Date.now() + 7 * DAY).toISOString().slice(0, 10);
  const all = [...S.tasks.values()].filter((t) => who === "all" || t.assignee === S.me.id);
  const open = all.filter((t) => !t.done).sort((a, b) => (a.due || "9999").localeCompare(b.due || "9999"));
  const groups = [["Overdue", open.filter((t) => t.due && t.due < today), "crit"], ["Today", open.filter((t) => t.due === today), "warn"], ["This week", open.filter((t) => t.due > today && t.due <= wk), "info"], ["Later", open.filter((t) => !t.due || t.due > wk), ""]];
  const done = all.filter((t) => t.done).sort((a, b) => (b.doneAt || 0) - (a.doneAt || 0)).slice(0, 20);
  const row = (t) => `<div class="alert" style="grid-template-columns:auto minmax(0,1fr) auto" data-act="task-edit" data-id="${t.id}"><label class="check" data-stop><input type="checkbox" data-on="task-toggle" data-id="${t.id}" ${t.done ? "checked" : ""} aria-label="Complete"></label><div style="min-width:0"><div class="t1" style="${t.done ? "text-decoration:line-through;opacity:.55" : ""}">${t.priority === "High" ? '<span class="chip warn" style="margin-right:6px">High</span>' : ""}${esc(t.title)}</div><div class="t2">${esc([t.contactId ? contactName(S.contacts.get(t.contactId)) : "", t.dealId ? S.deals.get(t.dealId)?.name : "", userName(t.assignee)].filter(Boolean).join(" · "))}</div></div><span class="hint num">${esc(t.due ? fmtDate(t.due, { day: "numeric", month: "short" }) : "No date")}</span></div>`;
  return `<div class="pagehead"><div><h1 class="h-display">Tasks</h1><div class="sub">${plural(open.length, "open task")}</div></div><div class="row"><div class="seg">${[["all", "Everyone"], ["me", "Mine"]].map(([k, l]) => `<button class="${who === k ? "on" : ""}" data-act="tasks-who" data-k="${k}">${l}</button>`).join("")}</div><button class="btn" data-act="task-import">${icon("upload")}Import</button><button class="btn primary" data-act="task-new">${icon("plus")}New task</button></div></div>
  <form class="panel" id="tq-form" style="margin-bottom:18px;padding:12px 14px"><div class="row" style="flex-wrap:wrap"><input class="input" id="tq-title" placeholder="Quick add: e.g. Send NDA to Halden FO" style="flex:1;min-width:220px"><input class="input" type="date" id="tq-due" style="width:auto" value="${today}"><button class="btn primary" type="submit">Add</button></div></form>
  <div class="stack" style="gap:18px">${groups.filter(([, l]) => l.length).map(([name, l, sev]) => `<section class="panel"><div class="panel-h"><h2 class="h-card">${name}</h2><span class="badge ${sev === "crit" ? "crit" : ""}">${l.length}</span></div><div class="alerts">${l.map(row).join("")}</div></section>`).join("") || `<div class="panel"><div class="empty"><div class="h-section">Nothing outstanding.</div></div></div>`}
  ${done.length ? `<details class="panel"><summary class="panel-h" style="cursor:pointer;list-style:none"><h2 class="h-card">Recently completed</h2><span class="badge">${done.length}</span></summary><div class="alerts">${done.map(row).join("")}</div></details>` : ""}</div>`;
};
afterHooks.push(() => { const f = $("#tq-form"); if (f) f.onsubmit = async (e) => { e.preventDefault(); const t = val(document, "tq-title"); if (!t) return; await put("tasks", uid("t_"), { title: t, due: val(document, "tq-due"), assignee: S.me.id, priority: "Normal", done: false }, `Added task “${t}”`); $("#tq-title").value = ""; }; });
Object.assign(ACT, {
  "tasks-who": (el) => { S.ui.tasks.who = el.dataset.k; render(); },
  "task-new": (el) => taskForm(null, { contactId: el?.dataset?.cid || "" }),
  "task-edit": (el, e) => { if (e.target.closest("[data-stop]")) return; taskForm(el.dataset.id); },
  "task-import": () => simpleImport("tasks"),
  "task-toggle": async (el) => { const t = S.tasks.get(el.dataset.id); if (!t) return; await patch("tasks", t.id, { done: el.checked, doneAt: el.checked ? now() : null }, `${el.checked ? "Completed" : "Reopened"} task “${t.title}”`); },
});
function taskForm(id, preset = {}) {
  const t = id ? { ...S.tasks.get(id) } : { assignee: S.me.id, priority: "Normal", due: todayISO(), ...preset };
  const m = openModal(modalShell(id ? "Task" : "New task", `<div class="fgrid" style="margin-top:14px">${fInput("tf-title", "Task", t.title, { cls: "span2" })}${fInput("tf-due", "Due", t.due, { type: "date" })}${fSelect("tf-pr", "Priority", ["Normal", "High"], t.priority, { blank: null })}${fSelect("tf-as", "Assigned to", userOpts(), t.assignee, { blank: "Unassigned" })}${fSelect("tf-c", "Contact", contactOpts(), t.contactId, { blank: "—" })}${fSelect("tf-d", "Deal", dealOpts(false), t.dealId, { blank: "—" })}${fText("tf-n", "Notes", t.notes, { cls: "span2", rows: 2 })}</div>`,
    `${id ? `<button class="btn danger" id="tf-del" style="margin-right:auto">Delete</button>` : ""}<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="tf-save">Save</button>`), { size: "" });
  if ($("#tf-del", m)) $("#tf-del", m).onclick = async () => { closeModal(); await remove("tasks", id, `Deleted task “${t.title}”`); };
  $("#tf-save", m).onclick = async () => { const title = val(m, "tf-title"); if (!title) return; const data = { ...(id ? S.tasks.get(id) : { done: false }), title, due: val(m, "tf-due"), priority: val(m, "tf-pr"), assignee: val(m, "tf-as"), contactId: val(m, "tf-c"), dealId: val(m, "tf-d"), notes: val(m, "tf-n") }; delete data.id; try { await put("tasks", id || uid("t_"), data, `${id ? "Updated" : "Added"} task “${title}”`); closeModal(); } catch {} };
}

/* ---------------- Vault ---------------- */
function vaultTable(list, ctx = {}) {
  list = list.slice().sort((a, b) => (b.uploadedAt || 0) - (a.uploadedAt || 0));
  return `<div class="toolbar"><span class="spacer"></span>${S.assets ? `<button class="btn primary" data-act="doc-upload" data-deal="${ctx.dealId || ""}">${icon("upload")}Upload documents</button>` : `<span class="hint">Uploading needs edit access to this workspace.</span>`}</div>
  ${list.length ? `<div class="tablewrap"><table class="t"><thead><tr><th>Document</th><th>Category</th><th>Deal</th><th>Sent to</th><th>Uploaded</th><th></th></tr></thead><tbody>${list.map((x) => { const sent = Object.entries(x.sentTo || {}).filter(([, v]) => v); return `<tr class="nohover"><td><a href="${esc("/_blob/" + x.assetId)}" target="_blank" rel="noopener" data-act="doc-open" data-id="${x.id}" class="name" style="color:var(--ink)">${esc(x.name)}</a><div class="org">${esc(((x.size || 0) / 1024 / 1024).toFixed(2))} MB${x.notes ? " · " + esc(x.notes) : ""}</div></td><td><span class="chip">${esc(x.category || "Other")}</span></td><td class="dim">${esc(S.deals.get(x.dealId)?.name || "—")}</td><td class="dim">${sent.length ? esc(sent.slice(0, 2).map(([cid]) => contactName(S.contacts.get(cid))).filter(Boolean).join(", ")) + (sent.length > 2 ? ` +${sent.length - 2}` : "") : "—"}</td><td class="dim">${esc(fmtDate(x.uploadedAt))}<div class="org">${esc(userName(x.uploadedBy))}</div></td><td class="r"><button class="btn sm ghost" data-act="doc-more" data-id="${x.id}" aria-label="More">${icon("more")}</button></td></tr>`; }).join("")}</tbody></table></div>` : `<div class="panel"><div class="empty"><div class="h-section">The vault is empty.</div><div style="max-width:460px">Keep NDAs, teasers, IMs, term sheets and KYC files with the deal they belong to, and record who received each one. ${window.UC_STANDALONE ? "PDF, Office files, images, CSV and text are accepted." : "PDFs, images, CSV and text files are accepted; save Word or PowerPoint files as PDF first."}</div></div></div>`}`;
}
VIEWS.vault = () => `<div class="pagehead"><div><h1 class="h-display">Vault</h1><div class="sub">${plural(S.docs.size, "document")} · every open and upload is logged</div></div></div>${vaultTable([...S.docs.values()])}`;
Object.assign(ACT, {
  "doc-open": (el) => { const x = S.docs.get(el.dataset.id); if (x) audit("view", "docs", x.id, `Opened document ${x.name}`); },
  "doc-upload": (el) => {
    const m = openModal(modalShell("Upload to the vault", `<div class="fgrid" style="margin-top:14px">${fSelect("du-cat", "Category", DOC_CATS, "", { blank: null })}${fSelect("du-deal", "Deal", dealOpts(false), el.dataset.deal || "", { blank: "Not deal-specific" })}${fSelect("du-c", "Related contact", contactOpts(), "", { blank: "—" })}${fInput("du-notes", "Note", "", { ph: "e.g. v3, signed copy" })}</div>
      <label class="drop" for="du-file" id="du-drop" style="margin-top:16px"><strong>Choose files or drop them here</strong><span>${window.UC_STANDALONE ? "PDF, Word, Excel, PowerPoint, images, CSV, text. Up to 25 MB each." : "PDF, images, CSV, text. Up to 20 MB each."}</span></label><input type="file" id="du-file" multiple hidden accept="${window.UC_STANDALONE ? ".pdf,.png,.jpg,.jpeg,.webp,.gif,.csv,.txt,.md,.json,.docx,.xlsx,.pptx,.doc,.xls,.ppt,.zip" : ".pdf,.png,.jpg,.jpeg,.webp,.gif,.csv,.txt,.md,.json"}"><div id="du-out" class="stack" style="margin-top:12px;gap:6px"></div>`, `<button class="btn ghost" data-act="modal-close">Done</button>`), { size: "" });
    const handle = async (files) => {
      for (const f of files) {
        const line = document.createElement("div"); line.className = "row hint"; line.innerHTML = `<span class="spin"></span>${esc(f.name)}`; $("#du-out", m).appendChild(line);
        try {
          const r = await S.assets.upload(f);
          await put("docs", uid("x_"), { name: f.name, category: val(m, "du-cat") || "Other", dealId: val(m, "du-deal"), contactId: val(m, "du-c"), notes: val(m, "du-notes"), assetId: r.id, contentType: r.contentType, size: r.sizeBytes, uploadedAt: now(), uploadedBy: S.me.id, sentTo: {} }, `Uploaded ${f.name}`);
          line.innerHTML = `${icon("file")}<span style="color:var(--ink)">${esc(f.name)}</span> uploaded`;
        } catch (e) { const code = e && e.code; line.innerHTML = `<span style="color:#EBA597">${esc(f.name)}: ${code === "too_large" ? "larger than 20 MB" : code === "unsupported_type" ? "this file type isn't accepted (save as PDF)" : code === "quota_exceeded" ? "storage is full" : "upload failed"}</span>`; }
      }
    };
    $("#du-file", m).onchange = (e) => handle([...e.target.files]);
    const drop = $("#du-drop", m); ["dragover", "dragenter"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); })); ["dragleave", "drop"].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.remove("over"); })); drop.addEventListener("drop", (e) => handle([...e.dataTransfer.files]));
  },
  "doc-more": (el) => {
    const x = S.docs.get(el.dataset.id);
    showMenu(el, [
      { label: "Record as sent to…", run: () => pickContact(`Who received ${x.name}?`, (list) => { const add = {}; list.forEach((c) => (add[c.id] = todayISO())); patch("docs", x.id, { sentTo: add }, `Recorded ${x.name} as sent to ${list.map(contactName).join(", ")}`); }, { multi: true }) },
      { label: "Edit details", run: () => { const m = openModal(modalShell("Document details", `<div class="fgrid" style="margin-top:14px">${fInput("de-name", "Name", x.name, { cls: "span2" })}${fSelect("de-cat", "Category", DOC_CATS, x.category, { blank: null })}${fSelect("de-deal", "Deal", dealOpts(false), x.dealId, { blank: "—" })}${fInput("de-notes", "Note", x.notes, { cls: "span2" })}</div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="de-ok">Save</button>`), { size: "narrow" }); $("#de-ok", m).onclick = async () => { await patch("docs", x.id, { name: val(m, "de-name"), category: val(m, "de-cat"), dealId: val(m, "de-deal"), notes: val(m, "de-notes") }); closeModal(); }; } },
      ...(isAdmin() && S.assets ? ["-", { label: "Delete permanently", run: async () => { if (!(await confirmBox("Delete this document?", `${esc(x.name)} will be removed from storage for everyone.`, "Delete", true))) return; try { await S.assets.delete(x.assetId); } catch {} await remove("docs", x.id, `Deleted document ${x.name}`); } }] : []),
    ]);
  },
});

/* ---------------- Security (super admin) ---------------- */
const ACTION_LABEL = { login: "Sign in", logout: "Sign out", lock: "Lock", unlock: "Unlock", security: "Security", create: "Create", update: "Edit", delete: "Delete", erase: "Erasure", import: "Import", export: "Export", view: "View", ai: "AI" };
VIEWS.security = () => {
  if (!isAdmin()) return `<div class="empty">Super admins only.</div>`;
  const tab = S.ui.security.tab;
  const tabs = `<div class="tabs">${[["log", "Access log"], ["users", "Users"], ["policy", "Access code & policies"], ["data", "Data & backups"]].map(([k, l]) => `<button class="${tab === k ? "on" : ""}" data-act="sec-tab" data-k="${k}">${l}</button>`).join("")}</div>`;
  const head = `<div class="pagehead"><div><h1 class="h-display">Security</h1><div class="sub">Who came in, when, from where, and what they changed.</div></div></div>`;
  if (tab === "log") {
    const f = S.ui.security; const q = norm(f.q);
    const ev = auditEvents(3000).filter((e) => (!f.user || e.u === f.user || (f.user === "__unknown" && !e.u)) && (!f.kind || (f.kind === "access" ? ["login", "logout", "lock", "unlock", "security"].includes(e.a) : e.a === f.kind)) && (!q || norm(e.s + " " + e.n + " " + e.d).includes(q)));
    const fails = auditEvents(3000).filter((e) => e.a === "security" && /Failed|locked out/i.test(e.s) && now() - e.t < 7 * DAY).length;
    const online = new Map(); S.peers.forEach((p) => online.set(p.presence.uid, p.presence));
    return head + tabs + `<div class="kpis" style="grid-template-columns:repeat(3,minmax(0,1fr))"><div class="kpi"><span class="eyebrow">Online now</span><span class="v num">${online.size || 1}</span><span class="s">${esc([...online.values()].map((p) => p.name).join(", ") || S.me.name)}</span></div><div class="kpi"><span class="eyebrow">Sign-ins, last 7 days</span><span class="v num">${auditEvents(3000).filter((e) => e.a === "login" && now() - e.t < 7 * DAY).length}</span><span class="s">Across all users</span></div><div class="kpi"><span class="eyebrow">Failed attempts, 7 days</span><span class="v num" style="${fails ? "color:#EBA597" : ""}">${fails}</span><span class="s">${fails ? "Review below" : "None"}</span></div></div>
    <div class="toolbar"><input class="input" id="sl-q" style="max-width:260px" placeholder="Search the log…" value="${esc(f.q)}" data-in="sec-f" data-k="q"><select class="input" id="sl-u" style="width:auto" data-on="sec-f" data-k="user"><option value="">All users</option>${[...S.users.values()].map((u) => `<option value="${u.id}" ${f.user === u.id ? "selected" : ""}>${esc(u.name)}</option>`).join("")}<option value="__unknown" ${f.user === "__unknown" ? "selected" : ""}>Unidentified</option></select><select class="input" id="sl-k" style="width:auto" data-on="sec-f" data-k="kind"><option value="">All events</option><option value="access" ${f.kind === "access" ? "selected" : ""}>Access only (sign in/out, locks, failures)</option>${Object.entries(ACTION_LABEL).map(([k, l]) => `<option value="${k}" ${f.kind === k ? "selected" : ""}>${l}</option>`).join("")}</select><span class="spacer"></span><button class="btn" data-act="sec-export">${icon("download")}Export log</button></div>
    <div class="tablewrap"><table class="t"><thead><tr><th>When</th><th>Who</th><th>Event</th><th>Detail</th><th>Device</th></tr></thead><tbody>${ev.slice(0, 400).map((e) => `<tr class="nohover"><td class="num dim" style="white-space:nowrap">${esc(fmtTime(e.t))}</td><td>${esc(e.n)}</td><td><span class="chip ${e.a === "security" ? "crit" : e.a === "login" ? "good" : e.a === "logout" || e.a === "lock" ? "" : e.a === "delete" || e.a === "erase" ? "warn" : "info"}">${esc(ACTION_LABEL[e.a] || e.a)}</span></td><td style="min-width:260px">${esc(e.s)}</td><td class="mono muted" style="white-space:nowrap">${esc(e.d || "")}</td></tr>`).join("") || `<tr class="nohover"><td colspan="5" class="muted">No events match.</td></tr>`}</tbody></table></div>
    <div class="hint" style="margin-top:8px">Showing the last 60 days held live. Export the log regularly to keep a permanent record.</div>`;
  }
  if (tab === "users") {
    const users = [...S.users.values()].sort((a, b) => (b.role === "superadmin") - (a.role === "superadmin") || a.name.localeCompare(b.name));
    return head + tabs + `<div class="toolbar"><span class="spacer"></span><button class="btn primary" data-act="user-new">${icon("plus")}Add user</button></div><div class="tablewrap"><table class="t"><thead><tr><th>Name</th><th>Role</th><th>Status</th><th>Last sign-in</th><th>Device</th><th></th></tr></thead><tbody>${users.map((u) => `<tr class="nohover"><td class="name">${esc(u.name)}${u.id === S.me.id ? ' <span class="muted">(you)</span>' : ""}</td><td>${u.role === "superadmin" ? '<span class="chip gold">Super admin</span>' : '<span class="chip">Member</span>'}</td><td>${u.active === false ? '<span class="chip crit">Deactivated</span>' : '<span class="chip good">Active</span>'}</td><td class="dim">${u.lastLoginAt ? esc(fmtTime(u.lastLoginAt)) : "Never"}</td><td class="mono muted">${esc(u.lastLoginDevice || "")}</td><td class="r"><button class="btn sm ghost" data-act="user-more" data-id="${u.id}">${icon("more")}</button></td></tr>`).join("")}</tbody></table></div>
    <div class="hint" style="margin-top:10px">Members can view and edit everything except Security and permanent deletion. Deactivating a user signs them out immediately on every device.</div>`;
  }
  if (tab === "policy") {
    const p = S.security || {};
    return head + tabs + `<div class="grid g2"><section class="panel"><div class="panel-h"><h2 class="h-card">Access code</h2></div><div class="panel-b stack"><div class="dim">The shared code is stored only as a salted PBKDF2 hash, never in readable form. ${p.codeRotatedAt ? `Last changed ${esc(fmtTime(p.codeRotatedAt))} by ${esc(userName(p.codeRotatedBy) || "setup")}.` : "Set at launch."}</div><div class="hint">Change it whenever someone leaves or the code may have been shared. People already signed in stay signed in until their session ends.</div><div><button class="btn primary" data-act="code-rotate">Change access code</button></div></div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Session policies</h2></div><div class="panel-b"><form id="pol-form" class="fgrid">${fInput("pol-idle", "Lock after inactivity (minutes)", p.idleMinutes || 15, { type: "number", attrs: 'min="2" max="240"' })}${fInput("pol-max", "Maximum session (hours)", p.maxSessionHours || 12, { type: "number", attrs: 'min="1" max="72"' })}${fInput("pol-att", "Failed attempts before lockout", p.maxAttempts || 5, { type: "number", attrs: 'min="3" max="20"' })}${fInput("pol-lock", "Lockout duration (minutes)", p.lockMinutes || 15, { type: "number", attrs: 'min="1" max="1440"' })}<div class="span2 row"><button class="btn primary" type="submit">Save policies</button></div></form></div></section></div>`;
  }
  const used = docCount(); const pct = (used / 5000) * 100;
  return head + tabs + `<div class="grid g2"><section class="panel"><div class="panel-h"><h2 class="h-card">Capacity</h2></div><div class="panel-b stack">${window.UC_STANDALONE ? `<div class="row between"><span>${used.toLocaleString("en-GB")} records</span><span class="hint">No fixed limit</span></div>` : `<div class="row between"><span>${used.toLocaleString("en-GB")} of 5,000 records</span><span class="hint">${Math.round(pct)}%</span></div><div class="meter"><i class="${pct > 80 ? "warn" : ""}" style="width:${Math.min(100, pct)}%"></i></div>`}<div class="hint">Contacts ${S.contacts.size} · Deals ${S.deals.size} · Tasks ${S.tasks.size} · Intros ${S.intros.size} · Documents ${S.docs.size} · Log days ${S.auditDays.length}.${window.UC_STANDALONE ? " The database and files live on your server's storage volume; back it up regularly." : " Beyond about 4,000 contacts, move to the production version of UConnect."}</div></div></section>
  <section class="panel"><div class="panel-h"><h2 class="h-card">Backups</h2></div><div class="panel-b stack"><div class="dim">Download everything as one JSON file: contacts, deals, pipelines, intros, tasks, document index, users (without PIN hashes) and the live log. Keep it somewhere safe; it contains personal data.</div><div class="row"><button class="btn primary" data-act="backup">${icon("download")}Download full backup</button><label class="btn" for="restore-file">${icon("upload")}Restore from backup</label><input type="file" id="restore-file" accept=".json" hidden data-on="restore"></div><div class="hint">Restore merges: records in the file are written back; nothing else is deleted.</div></div></section></div>`;
};
Object.assign(ACT, {
  "sec-tab": (el) => { S.ui.security.tab = el.dataset.k; render(); },
  "sec-f": (el) => { S.ui.security[el.dataset.k] = el.value; render(); },
  "sec-export": () => { const ev = auditEvents(100000); saveFile(`uconnect-access-log-${todayISO()}.csv`, toCSV(ev, [["Time", (e) => new Date(e.t).toISOString()], ["User", "n"], ["Event", (e) => ACTION_LABEL[e.a] || e.a], ["Detail", "s"], ["Entity", "e"], ["Record", "i"], ["Device", "d"]])); },
  "user-new": () => userForm(),
  "user-more": (el) => {
    const u = S.users.get(el.dataset.id); const admins = [...S.users.values()].filter((x) => x.role === "superadmin" && x.active !== false);
    const lastAdmin = u.role === "superadmin" && admins.length <= 1;
    showMenu(el, [
      { label: "Rename", run: () => userForm(u.id) },
      { label: "Reset PIN", run: () => pinForm(u.id) },
      ...(u.id !== S.me.id && !lastAdmin ? [{ label: u.role === "superadmin" ? "Make member" : "Make super admin", run: async () => { if (!(await pinPrompt("Confirm role change", `Change ${esc(u.name)}'s role?`))) return; await patch("users", u.id, { role: u.role === "superadmin" ? "member" : "superadmin" }, `Changed ${u.name}'s role to ${u.role === "superadmin" ? "member" : "super admin"}`); } }] : []),
      ...(u.id !== S.me.id && !lastAdmin ? [{ label: u.active === false ? "Reactivate" : "Deactivate", run: async () => { if (!(await pinPrompt("Confirm", `${u.active === false ? "Reactivate" : "Deactivate"} ${esc(u.name)}?`))) return; await patch("users", u.id, { active: u.active === false }, `${u.active === false ? "Reactivated" : "Deactivated"} ${u.name}`); } }] : []),
    ]);
  },
  "code-rotate": () => {
    const m = openModal(modalShell("Change access code", `<div class="stack" style="margin-top:14px"><div class="hint">At least 8 characters with letters and numbers. Share it in person or by phone, not by email.</div>${fInput("cr-a", "New access code", "", { type: "password", attrs: 'autocomplete="new-password"' })}${fInput("cr-b", "Repeat new code", "", { type: "password", attrs: 'autocomplete="new-password"' })}${fInput("cr-pin", "Your PIN", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}<div class="gate-msg" id="cr-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="cr-ok">Change code</button>`), { size: "narrow" });
    $("#cr-ok", m).onclick = async () => {
      const a = $("#cr-a", m).value, b = $("#cr-b", m).value, pin = $("#cr-pin", m).value; const say = (t) => ($("#cr-msg", m).textContent = t);
      if (a.length < 8 || !/[A-Za-z]/.test(a) || !/\d/.test(a)) return say("Use at least 8 characters, mixing letters and numbers.");
      if (a !== b) return say("The two codes don't match.");
      const u = S.users.get(S.me.id); if (!safeEq(await pbkdf2(pin, u.pinSalt, u.iter), u.pinHash)) { audit("security", "", "", "Failed PIN while changing the access code"); return say("Your PIN doesn't match."); }
      const h = await hashSecret(a, 200000);
      try { await S.db.doc("settings/security").update({ codeSalt: h.salt, codeHash: h.hash, iter: h.iter, codeRotatedAt: now(), codeRotatedBy: S.me.id }); audit("security", "settings", "security", "Changed the access code"); closeModal(); toast("Access code changed."); } catch (e) { writeErr(e); }
    };
  },
  backup: () => {
    const strip = (u) => { const { pinHash, pinSalt, iter, ...rest } = u; return rest; };
    const pack = { app: "UConnect", version: 1, exportedAt: new Date().toISOString(), exportedBy: S.me.name, settings: S.settings, contacts: [...S.contacts.values()], deals: [...S.deals.values()], intros: [...S.intros.values()], tasks: [...S.tasks.values()], docs: [...S.docs.values()], users: [...S.users.values()].map(strip), audit: S.auditDays };
    saveFile(`uconnect-backup-${todayISO()}.json`, JSON.stringify(pack));
  },
  restore: async (el) => {
    const f = el.files[0]; if (!f) return;
    let pack; try { pack = JSON.parse(await readFileText(f)); } catch { toast("That isn't a UConnect backup file.", true); return; }
    if (pack.app !== "UConnect") { toast("That isn't a UConnect backup file.", true); return; }
    const counts = ["contacts", "deals", "intros", "tasks", "docs"].map((k) => `${(pack[k] || []).length} ${k}`).join(", ");
    if (!(await confirmBox("Restore this backup?", `From ${esc(pack.exportedAt || "unknown date")}: ${esc(counts)}. Matching records are overwritten with the backup's version.`, "Restore"))) return;
    if (!(await pinPrompt("Confirm restore", "Enter your PIN to restore."))) return;
    let n = 0;
    for (const k of ["contacts", "deals", "intros", "tasks", "docs"]) for (const r of pack[k] || []) { if (!r.id) continue; const { id, ...body } = r; try { await withRetry(() => S.db.doc(`${k}/${id}`).set(body)); n++; } catch {} }
    audit("import", "", "", `Restored ${n} records from backup dated ${pack.exportedAt || "unknown"}`); toast(`Restored ${n} records.`);
  },
});
afterHooks.push(() => { const f = $("#pol-form"); if (f) f.onsubmit = async (e) => { e.preventDefault(); const upd = { idleMinutes: clamp(+val(f, "pol-idle") || 15, 2, 240), maxSessionHours: clamp(+val(f, "pol-max") || 12, 1, 72), maxAttempts: clamp(+val(f, "pol-att") || 5, 3, 20), lockMinutes: clamp(+val(f, "pol-lock") || 15, 1, 1440) }; try { await S.db.doc("settings/security").update(upd); audit("security", "settings", "security", `Updated session policies: lock after ${upd.idleMinutes} min, max session ${upd.maxSessionHours} h, lockout after ${upd.maxAttempts} attempts for ${upd.lockMinutes} min`); toast("Policies saved."); } catch (err) { writeErr(err); } }; });
function userForm(id) {
  const u = id ? S.users.get(id) : null;
  const m = openModal(modalShell(u ? "Rename user" : "Add user", `<div class="stack" style="margin-top:14px">${fInput("uf-name", "Full name", u?.name)}${u ? "" : fSelect("uf-role", "Role", [["member", "Member"], ["superadmin", "Super admin"]], "member", { blank: null }) + fInput("uf-pin", "Starting PIN (6 to 12 digits)", "", { type: "password", attrs: 'inputmode="numeric"' }) + `<div class="hint">Give the PIN to them in person. They can change it in Settings.</div>`}<div class="gate-msg" id="uf-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="uf-ok">${u ? "Save" : "Add user"}</button>`), { size: "narrow" });
  $("#uf-ok", m).onclick = async () => {
    const name = val(m, "uf-name"); if (!name) return;
    if (u) { await patch("users", id, { name }, `Renamed user to ${name}`); closeModal(); return; }
    const pin = $("#uf-pin", m).value; if (!/^\d{6,12}$/.test(pin)) { $("#uf-msg", m).textContent = "PINs are 6 to 12 digits."; return; }
    const h = await hashSecret(pin); const nid = uid("u_");
    await put("users", nid, { name, role: val(m, "uf-role"), pinSalt: h.salt, pinHash: h.hash, iter: h.iter, active: true }, `Added user ${name} (${val(m, "uf-role") === "superadmin" ? "super admin" : "member"})`);
    closeModal(); toast(`${name} added.`);
  };
}
function pinForm(id) {
  const u = S.users.get(id); const self = id === S.me.id;
  const m = openModal(modalShell(self ? "Change your PIN" : `Reset ${esc(u.name)}'s PIN`, `<div class="stack" style="margin-top:14px">${self ? fInput("pf-cur", "Current PIN", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' }) : fInput("pf-cur", "Your PIN (to authorise)", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}${fInput("pf-a", "New PIN (6 to 12 digits)", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}${fInput("pf-b", "Repeat new PIN", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}<div class="gate-msg" id="pf-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="pf-ok">Save PIN</button>`), { size: "narrow" });
  $("#pf-ok", m).onclick = async () => {
    const say = (t) => ($("#pf-msg", m).textContent = t); const me = S.users.get(S.me.id);
    if (!safeEq(await pbkdf2($("#pf-cur", m).value, me.pinSalt, me.iter), me.pinHash)) { audit("security", "users", S.me.id, "Failed PIN while changing a PIN"); return say("That PIN doesn't match."); }
    const a = $("#pf-a", m).value; if (!/^\d{6,12}$/.test(a) || /^(\d)\1+$/.test(a)) return say("Use 6 to 12 digits, not all the same."); if (a !== $("#pf-b", m).value) return say("The new PINs don't match.");
    const h = await hashSecret(a); await patch("users", id, { pinSalt: h.salt, pinHash: h.hash, iter: h.iter }, self ? "Changed own PIN" : `Reset PIN for ${u.name}`); closeModal(); toast("PIN updated.");
  };
}

/* ---------------- Settings ---------------- */
VIEWS.settings = () => {
  const fx = S.settings.fx || DEFAULT_FX; const cad = S.settings.cadence || {}; const adm = isAdmin();
  return `<div class="pagehead"><div><h1 class="h-display">Settings</h1></div></div>
  <div class="grid g2">
    <section class="panel"><div class="panel-h"><h2 class="h-card">You</h2></div><div class="panel-b stack"><div class="me"><span class="av">${esc(initials(S.me.name))}</span><div><div>${esc(S.me.name)}</div><div class="hint">${adm ? "Super admin" : "Member"} · this device: ${esc(deviceLabel())}</div></div></div><div class="row"><button class="btn" data-act="my-pin">Change my PIN</button><button class="btn" data-act="discreet">${S.discreet ? "Turn discreet mode off" : "Turn discreet mode on"}</button><button class="btn" data-act="lock">${icon("lock")}Lock now</button></div></div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Relationship cadence</h2></div><div class="panel-b"><form id="cad-form" class="fgrid">${TIERS.map((t) => fInput("cad-" + t, `Tier ${t}: touch every (days)`, cad[t] || "", { type: "number", attrs: `min="7" max="720" ${adm ? "" : "disabled"}` })).join("")}${fInput("cad-stall", "Flag investors stuck in a stage after (days)", S.settings.stallDays || 21, { type: "number", attrs: `min="3" max="180" ${adm ? "" : "disabled"}` })}${adm ? `<div class="span2"><button class="btn primary" type="submit">Save</button></div>` : `<div class="hint span2">Only super admins can change these.</div>`}</form></div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Currency</h2><span class="hint">Base: GBP</span></div><div class="panel-b"><form id="fx-form" class="stack"><div class="hint">Rates convert every amount into pounds for totals. They're set by hand, so review them monthly. ${S.settings.fxUpdatedAt ? "Last updated " + esc(fmtDate(S.settings.fxUpdatedAt)) + "." : "Starting values are approximate and need checking."}</div><div class="fgrid">${CURRENCIES.filter((c) => c !== "GBP").map((c) => fInput("fx-" + c, `1 ${c} = £`, fx[c], { type: "number", attrs: `step="0.0001" min="0" ${adm ? "" : "disabled"}` })).join("")}</div>${adm ? `<div><button class="btn primary" type="submit">Save rates</button></div>` : ""}</form></div></section>
    <section class="panel"><div class="panel-h"><h2 class="h-card">Templates</h2></div><div class="panel-b stack"><div class="hint">Spreadsheet templates that import perfectly on each page.</div><div class="row">${Object.keys(TEMPLATES).map((k) => `<button class="btn sm" data-act="tpl" data-k="${k}">${icon("download")}${k[0].toUpperCase() + k.slice(1)}</button>`).join("")}</div>
      <div class="divider"></div><div class="h-card">Keyboard</div><div class="hint"><kbd>/</kbd> or <kbd>Ctrl</kbd>+<kbd>K</kbd> search · <kbd>Esc</kbd> close panels</div></div></section>
  </div>`;
};
ACT["my-pin"] = () => pinForm(S.me.id);
afterHooks.push(() => {
  const c = $("#cad-form"); if (c) c.onsubmit = async (e) => { e.preventDefault(); const cadence = {}; TIERS.forEach((t) => (cadence[t] = clamp(+val(c, "cad-" + t) || 60, 7, 720))); await saveSettings({ cadence, stallDays: clamp(+val(c, "cad-stall") || 21, 3, 180) }, "Updated relationship cadence"); };
  const f = $("#fx-form"); if (f) f.onsubmit = async (e) => { e.preventDefault(); const fx = { GBP: 1 }; CURRENCIES.filter((x) => x !== "GBP").forEach((x) => (fx[x] = Number(val(f, "fx-" + x)) || DEFAULT_FX[x])); await saveSettings({ fx, fxUpdatedAt: todayISO() }, "Updated currency rates"); };
});
async function saveSettings(upd, summary) {
  try { const ref = S.db.doc("settings/general"); const snap = await ref.get(); if (snap.exists) await ref.update(upd); else await ref.set({ ...S.settings, ...upd }); audit("update", "settings", "general", summary); toast("Saved."); } catch (e) { writeErr(e); }
}
/* =========================================================
   Investor note: a high level overview written from the deal's
   documents. Two versions from one analysis: an internal one that
   keeps every candid judgement, and an investor-facing one that
   hides the internal-only lines.
   ========================================================= */
const NOTE_UI = { mode: {}, edit: false, running: null };
const SRC_LABEL = { deck: "Deck", ours: "Our view", crm: "UConnect" };
const noteMode = (id) => NOTE_UI.mode[id] || "external";
const noteOf = (d) => (d && d.note ? d.note : null);

async function vaultFile(docId) {
  const x = S.docs.get(docId); if (!x || !x.assetId) throw new Error("missing");
  if (window.UC_FETCH_FILE && window.UC_TRANSPORT === "poll") { const r = await window.UC_FETCH_FILE(x.assetId); return new File([r.blob], x.name, { type: r.meta.type }); }
  const res = await fetch("/_blob/" + x.assetId, { credentials: "same-origin" }); if (!res.ok) throw new Error("fetch");
  return new File([await res.blob()], x.name, { type: x.contentType || "application/octet-stream" });
}
function noteSchemaPrompt(deal, matches, macroNotes, text, hasImages, sourceName) {
  return `You are an investment analyst preparing a high level overview note on one opportunity, for a principal who places and co-invests private capital. The note is read by him, and a version of it is sent to investors and clients.

Be rigorous and sceptical. Separate what the document states from your own assessment. Never invent a figure: if something is not in the document, put it in "gaps" instead. Cover both the micro level (the asset or company itself) and the macro level (market, economy, policy).

Return one JSON object with exactly these keys:
{
 "headline": "the deal in one line",
 "subhead": "asset class, strategy, location and instrument in a short phrase",
 "atAGlance": [{"label":"Raise","value":"GBP 45m","source":"deck|ours"}],            // 6 to 8 rows: raise, minimum ticket, instrument, target return, cash yield, hold period, close date, sponsor
 "opportunity": "3 to 5 sentences: what it is, why it exists now, what the money does",
 "thesis": [{"point":"short claim","evidence":"what supports it","source":"deck|ours","internalOnly":false}],   // 4 to 6
 "watch": [{"risk":"short name","detail":"one or two sentences","impact":"High|Medium|Low","likelihood":"High|Medium|Low","mitigation":"what would reduce or de-risk it","source":"deck|ours","internalOnly":false}],  // 4 to 8
 "micro": [{"topic":"Unit economics|Revenue quality|Concentration|Capital structure|Sponsor and team|Valuation|Exit routes|Operations","finding":"two or three sentences","source":"deck|ours","internalOnly":false}],  // 5 to 8
 "macro": [{"topic":"Demand drivers|Rates and financing|Inflation and costs|Currency|Regulation and policy|Supply and competition|Cycle position","finding":"two or three sentences","asOf":"general knowledge, verify"}],  // 4 to 7
 "terms": [{"label":"Instrument","value":"...","source":"deck|ours"}],              // structure, security, fees, liquidity, governance, use of proceeds, tax notes
 "numbers": {"rows":[{"metric":"Net IRR","base":"15%","downside":"8%","note":"sponsor's own projection","source":"deck|ours"}],"assumptions":["..."]},
 "verdict": {"call":"Pursue|Watch|Pass","conviction":"High|Medium|Low","rationale":"2 to 4 sentences","swingFactor":"the one thing that decides it"},
 "externalSummary": "2 to 3 sentences for the investor-facing version, balanced and free of internal opinion",
 "questions": [{"q":"question for the sponsor","why":"why it matters","priority":"High|Medium|Low"}],   // 6 to 10
 "gaps": ["what the document does not disclose"],
 "suits": {"profile":"the investor this fits, in one or two sentences","assetClasses":["..."],"regions":["..."],"ticket":"e.g. GBP 1m to 5m","riskAppetite":"Low|Moderate|High","notes":"anything else"},
 "nextSteps": [{"step":"...","by":"suggested timing"}],
 "confidence": "High|Medium|Low"
}

Rules:
- "source" is "deck" when the document states it and "ours" when it is your inference or assessment.
- Set "internalOnly": true on any line that is a candid internal judgement: doubts about the sponsor, pricing or negotiating views, anything you would not put in front of the investor. Those lines never appear in the investor version.
- Every macro item carries "asOf": "general knowledge, verify", because you have no live market data. Give the direction and the mechanism rather than precise current figures.
- Impact and likelihood are your assessment, not the document's.
- UK English, plain and concise, no em dashes, no marketing language. Keep currency symbols as the document uses them.
${macroNotes ? `\nCURRENT MARKET NOTES FROM THE TEAM (treat these as current and authoritative, and build the macro section around them):\n"""${macroNotes.slice(0, 4000)}"""\n` : ""}
CRM RECORD FOR THIS DEAL:
${JSON.stringify({ name: deal.name, type: deal.type, assetClass: deal.assetClass, strategy: deal.subStrategy, instrument: deal.instrument, sponsor: deal.sponsor, regions: deal.regions, sectors: deal.sectors, target: deal.target, currency: deal.currency, minTicket: deal.minTicket, closeDate: deal.closeDate, feePct: deal.feePct, summary: deal.summary, highlights: deal.highlights })}
${matches && matches.length ? `\nINVESTORS IN OUR NETWORK THAT THE MATCHER RANKS HIGHEST (use these only to sanity-check the "suits" section; do not name them in the note):\n${JSON.stringify(matches)}\n` : ""}
${hasImages ? `The document's pages are attached as images.${text ? " Extracted text follows too." : ""}` : ""}
DOCUMENT${sourceName ? ` (${sourceName})` : ""}:
"""${(text || "").slice(0, 60000)}"""`;
}
async function generateNote(dealId, { file, macroNotes = "", useMatches = true, onStep = () => {} } = {}) {
  const deal = S.deals.get(dealId); if (!deal) return;
  if (!S.sample) { toast("AI is off, so the note can't be written.", true); return; }
  NOTE_UI.running = dealId; scheduleRender();
  try {
    let text = "", images = [], sourceName = "";
    if (file) {
      onStep("Reading the document…");
      const canImg = !!(await S.sample.limits().catch(() => null))?.images;
      const r = await readDocument(file, { wantImages: canImg });
      text = r.text; images = r.images; sourceName = file.name;
    }
    if (!text && !images.length) { text = [deal.summary, deal.highlights].filter(Boolean).join("\n\n"); sourceName = "the deal record"; }
    if (!text && !images.length) throw Object.assign(new Error("empty"), { code: "empty" });
    const matches = useMatches ? localMatch(dealToCriteria(deal)).slice(0, 5).map((x) => { const c = S.contacts.get(x.id); return { type: c.contactType, assets: c.assetClasses, regions: c.regions, ticket: ticketText(c), score: x.score }; }) : [];
    onStep("Claude is analysing the deal…");
    const out = await S.sample.json(noteSchemaPrompt(deal, matches, macroNotes, text, images.length > 0, sourceName), { modelTier: "complex", ...(images.length ? { images } : {}) });
    if (!out || typeof out !== "object") throw Object.assign(new Error("bad"), { code: "invalid_json" });
    const note = { ...out, generatedAt: now(), generatedBy: S.me.id, source: sourceName, macroNotes: macroNotes || "", version: (deal.note?.version || 0) + 1 };
    await patch("deals", dealId, { note }, `Wrote the investor note for ${deal.name}${sourceName ? " from " + sourceName : ""}`);
    toast("Investor note ready.");
    return note;
  } catch (e) {
    toast(e && e.code === "empty" ? "There was nothing readable to analyse." : aiErr(e), true);
  } finally { NOTE_UI.running = null; scheduleRender(); }
}

/* ---------------- rendering ---------------- */
const srcTag = (s) => (s && SRC_LABEL[s] ? `<span class="src ${esc(s)}">${SRC_LABEL[s]}</span>` : "");
const rk = (v) => ({ High: "crit", Medium: "warn", Low: "" }[v] || "");
function ed(path, value, tag = "span", cls = "") {
  const v = value === undefined || value === null ? "" : String(value);
  return `<${tag} class="${cls} ${NOTE_UI.edit ? "editable" : ""}" ${NOTE_UI.edit ? `contenteditable="plaintext-only" data-np="${esc(path)}"` : ""}>${esc(v)}</${tag}>`;
}
function noteBody(d, n, mode, print) {
  const vis = (arr) => (arr || []).filter((x) => mode === "internal" || !x.internalOnly);
  const hidden = mode === "external" ? ["thesis", "watch", "micro"].reduce((t, k) => t + (n[k] || []).filter((x) => x.internalOnly).length, 0) : 0;
  const glance = (n.atAGlance || []).slice(0, 8);
  const sec = (title, inner, note) => `<section class="nsec"><h3>${esc(title)}</h3>${note ? `<div class="nnote">${esc(note)}</div>` : ""}${inner}</section>`;
  return `
  <header class="nhead">
    <div class="row between" style="align-items:flex-start">
      <div style="min-width:0">
        <div class="eyebrow">${esc(d.type || "Opportunity")}${d.status && d.status !== "Live" ? " · " + esc(d.status) : ""}</div>
        <h2 class="h-display" style="font-size:30px;margin-top:6px">${esc(d.name)}</h2>
        <div class="dim">${ed("subhead", n.subhead)}</div>
      </div>
      <div class="hint" style="text-align:right;white-space:nowrap">${mode === "internal" ? '<span class="chip crit">Internal only</span>' : '<span class="chip">Investor version</span>'}<div style="margin-top:6px">${esc(fmtDate(n.generatedAt))}</div></div>
    </div>
    <p class="nlede">${ed("headline", n.headline)}</p>
  </header>
  ${glance.length ? `<div class="nglance">${glance.map((g, i) => `<div><span class="eyebrow">${ed(`atAGlance.${i}.label`, g.label)}</span><b class="sens">${ed(`atAGlance.${i}.value`, g.value)}</b>${g.source === "ours" ? srcTag(g.source) : ""}</div>`).join("")}</div>` : ""}
  ${mode === "internal" && n.verdict ? sec("Verdict", `<div class="nverdict"><span class="chip ${n.verdict.call === "Pursue" ? "good" : n.verdict.call === "Pass" ? "crit" : "warn"}">${esc(n.verdict.call || "")}</span><span class="chip">Conviction: ${esc(n.verdict.conviction || "")}</span><p>${ed("verdict.rationale", n.verdict.rationale, "span")}</p><p class="nswing"><span class="eyebrow">Swing factor</span> ${ed("verdict.swingFactor", n.verdict.swingFactor, "span")}</p></div>`) : ""}
  ${sec("The opportunity", `<p>${ed(mode === "internal" ? "opportunity" : "opportunity", n.opportunity, "span")}</p>${mode === "external" && n.externalSummary ? `<p class="dim">${ed("externalSummary", n.externalSummary, "span")}</p>` : ""}`)}
  ${vis(n.thesis).length ? sec("The case for it", `<ul class="nlist">${vis(n.thesis).map((t) => { const i = (n.thesis || []).indexOf(t); return `<li><b>${ed(`thesis.${i}.point`, t.point)}</b> ${ed(`thesis.${i}.evidence`, t.evidence)} ${srcTag(t.source)}${t.internalOnly ? '<span class="src int">Internal</span>' : ""}</li>`; }).join("")}</ul>`) : ""}
  ${vis(n.watch).length ? sec("What to watch", `<div class="tablewrap"><table class="t nrisk"><thead><tr><th>Risk</th><th>Impact</th><th>Likelihood</th><th>What would reduce it</th></tr></thead><tbody>${vis(n.watch).map((w) => { const i = (n.watch || []).indexOf(w); return `<tr class="nohover"><td><div class="name">${ed(`watch.${i}.risk`, w.risk)}</div><div class="org">${ed(`watch.${i}.detail`, w.detail)}</div>${w.internalOnly ? '<span class="src int">Internal</span>' : ""}</td><td><span class="chip ${rk(w.impact)}">${esc(w.impact || "")}</span></td><td><span class="chip ${rk(w.likelihood)}">${esc(w.likelihood || "")}</span></td><td>${ed(`watch.${i}.mitigation`, w.mitigation)} ${srcTag(w.source)}</td></tr>`; }).join("")}</tbody></table></div>`) : ""}
  ${vis(n.micro).length ? sec("The asset itself", `<dl class="kv nkv">${vis(n.micro).map((m) => { const i = (n.micro || []).indexOf(m); return `<dt>${ed(`micro.${i}.topic`, m.topic)}</dt><dd>${ed(`micro.${i}.finding`, m.finding)} ${srcTag(m.source)}${m.internalOnly ? '<span class="src int">Internal</span>' : ""}</dd>`; }).join("")}</dl>`) : ""}
  ${(n.macro || []).length ? sec("Market and macro", `<dl class="kv nkv">${(n.macro || []).map((m, i) => `<dt>${ed(`macro.${i}.topic`, m.topic)}</dt><dd>${ed(`macro.${i}.finding`, m.finding)} <span class="src ours">Our view</span></dd>`).join("")}</dl>`, n.macroNotes ? "Built around the market notes supplied by the team." : "General market context, not live data. Check current figures before this note goes out.") : ""}
  ${(n.terms || []).length ? sec("Structure and terms", `<dl class="kv nkv">${(n.terms || []).map((t, i) => `<dt>${ed(`terms.${i}.label`, t.label)}</dt><dd class="sens">${ed(`terms.${i}.value`, t.value)} ${srcTag(t.source)}</dd>`).join("")}</dl>`) : ""}
  ${(n.numbers?.rows || []).length ? sec("The numbers", `<div class="tablewrap"><table class="t"><thead><tr><th>Measure</th><th class="r">Base case</th><th class="r">Downside</th><th>Note</th></tr></thead><tbody>${n.numbers.rows.map((r, i) => `<tr class="nohover"><td class="name">${ed(`numbers.rows.${i}.metric`, r.metric)}</td><td class="r num sens">${ed(`numbers.rows.${i}.base`, r.base)}</td><td class="r num sens">${ed(`numbers.rows.${i}.downside`, r.downside)}</td><td class="dim">${ed(`numbers.rows.${i}.note`, r.note)} ${srcTag(r.source)}</td></tr>`).join("")}</tbody></table></div>${(n.numbers.assumptions || []).length ? `<div class="nnote" style="margin-top:8px"><span class="eyebrow">Assumptions</span> ${esc((n.numbers.assumptions || []).join(" · "))}</div>` : ""}`) : ""}
  ${(n.questions || []).length && mode === "internal" ? sec("Questions for the sponsor", `<ol class="nlist num">${n.questions.map((q, i) => `<li><span class="chip ${rk(q.priority)}">${esc(q.priority || "")}</span> <b>${ed(`questions.${i}.q`, q.q)}</b> <span class="dim">${ed(`questions.${i}.why`, q.why)}</span></li>`).join("")}</ol>`) : ""}
  ${(n.gaps || []).length && mode === "internal" ? sec("Not in the document", `<ul class="nlist">${n.gaps.map((g, i) => `<li>${ed(`gaps.${i}`, g)}</li>`).join("")}</ul>`) : ""}
  ${n.suits ? sec("Who this suits", `<p>${ed("suits.profile", n.suits.profile, "span")}</p><div class="chips" style="margin-top:8px">${[...(n.suits.assetClasses || []), ...(n.suits.regions || [])].map((x) => `<span class="chip">${esc(x)}</span>`).join("")}${n.suits.ticket ? `<span class="chip gold sens">${esc(n.suits.ticket)}</span>` : ""}${n.suits.riskAppetite ? `<span class="chip">${esc(n.suits.riskAppetite)} risk appetite</span>` : ""}</div>${mode === "internal" ? noteMatchesHTML(d) : ""}`) : ""}
  ${(n.nextSteps || []).length ? sec("Next steps", `<ul class="nlist">${n.nextSteps.map((s, i) => `<li>${ed(`nextSteps.${i}.step`, s.step)}${s.by ? ` <span class="dim">${ed(`nextSteps.${i}.by`, s.by)}</span>` : ""}</li>`).join("")}</ul>`) : ""}
  <footer class="nfoot">
    <div>${esc(S.settings.firm || "")} ${n.source ? `Prepared from ${esc(n.source)}` : ""} on ${esc(fmtDate(n.generatedAt))}${n.confidence ? ` · Confidence: ${esc(n.confidence)}` : ""}${hidden ? ` · ${hidden} internal point${hidden === 1 ? "" : "s"} hidden in this version` : ""}</div>
    <div class="ndisc">${ed("disclaimer", n.disclaimer || S.settings.disclaimer || "Private and confidential. Prepared for discussion only. This is not investment advice, nor an offer or invitation to invest. Figures are drawn from the sponsor's own materials and have not been independently verified. Capital is at risk.", "span")}</div>
  </footer>`;
}
function noteMatchesHTML(d) {
  const list = localMatch(dealToCriteria(d)).slice(0, 5).map((x) => ({ x, c: S.contacts.get(x.id) })).filter((r) => r.c);
  if (!list.length) return "";
  return `<div class="nmatch"><span class="eyebrow">Closest investors in your network</span><div class="chips" style="margin-top:6px">${list.map((r) => `<a class="chip gold" data-act="contact-open" data-id="${r.c.id}" style="cursor:pointer">${esc(contactName(r.c))} · ${r.x.score}</a>`).join("")}</div><div class="hint" style="margin-top:4px">From the matcher. Not shown in the investor version.</div></div>`;
}
function noteTab(d) {
  const n = noteOf(d); const mode = noteMode(d.id); const busy = NOTE_UI.running === d.id;
  const dealDocs = [...S.docs.values()].filter((x) => x.dealId === d.id);
  if (busy) return `<section class="panel"><div class="steps" style="padding:28px"><div class="step on"><span class="b"></span><span id="note-step">Working…</span></div><div class="hint">A full note takes up to a minute.</div></div></section>`;
  if (!n) return `<section class="panel"><div class="empty" style="padding:44px 24px"><div class="h-section">Write the investor note</div><div style="max-width:520px">A high level overview of this deal: the case for it, what to watch, the asset itself, market and macro, structure, the numbers, questions for the sponsor and who it suits. You get an internal version and an investor-facing version from the same analysis.</div>
    ${S.sample ? `<div class="row"><button class="btn primary" data-act="note-new" data-id="${d.id}">${icon("sparkle")}Write the note</button></div>` : `<div class="hint">Claude isn't available here, so the note can't be written.</div>`}
    ${dealDocs.length ? `<div class="hint">${plural(dealDocs.length, "document")} in this deal's vault can be used as the source.</div>` : `<div class="hint">Upload the deck to this deal's Vault first, or pick a file when you generate.</div>`}</div></section>`;
  return `<div class="toolbar">
    <div class="seg">${[["external", "Investor version"], ["internal", "Internal version"]].map(([k, l]) => `<button class="${mode === k ? "on" : ""}" data-act="note-mode" data-id="${d.id}" data-k="${k}">${l}</button>`).join("")}</div>
    <span class="spacer"></span>
    <button class="btn sm ${NOTE_UI.edit ? "primary" : ""}" data-act="note-edit">${icon("edit")}${NOTE_UI.edit ? "Done editing" : "Edit"}</button>
    <button class="btn sm" data-act="note-print" data-id="${d.id}">${icon("file")}Export PDF</button>
    ${S.sample ? `<button class="btn sm" data-act="note-new" data-id="${d.id}">${icon("sparkle")}Rewrite</button>` : ""}
  </div>
  <article class="note ${mode}" id="note-${esc(d.id)}">${noteBody(d, n, mode, false)}</article>
  ${NOTE_UI.edit ? `<div class="hint" style="margin-top:10px">Click any text to change it. Edits save when you click away, and apply to both versions.</div>` : ""}`;
}
Object.assign(ACT, {
  "note-mode": (el) => { NOTE_UI.mode[el.dataset.id] = el.dataset.k; render(); },
  "note-edit": () => { NOTE_UI.edit = !NOTE_UI.edit; render(); },
  "note-new": (el) => noteForm(el.dataset.id),
  "note-print": (el) => printNote(el.dataset.id),
});
/* Save an inline edit back into the note */
document.addEventListener("focusout", async (e) => {
  const el = e.target.closest && e.target.closest("[data-np]"); if (!el || !NOTE_UI.edit) return;
  const art = el.closest(".note"); if (!art) return;
  const dealId = art.id.replace(/^note-/, ""); const d = S.deals.get(dealId); if (!d || !d.note) return;
  const path = el.dataset.np.split("."); let obj = d.note; const note = JSON.parse(JSON.stringify(d.note));
  obj = note;
  for (let i = 0; i < path.length - 1; i++) { if (obj[path[i]] === undefined) obj[path[i]] = /^\d+$/.test(path[i + 1]) ? [] : {}; obj = obj[path[i]]; }
  const last = path[path.length - 1]; const val = el.textContent.trim();
  if (String(obj[last] ?? "") === val) return;
  obj[last] = val; note.editedAt = now(); note.editedBy = S.me.id;
  await patch("deals", dealId, { note }, `Edited the investor note for ${d.name}`).catch(() => {});
});
function noteForm(dealId) {
  const d = S.deals.get(dealId); const dealDocs = [...S.docs.values()].filter((x) => x.dealId === dealId);
  const m = openModal(modalShell(d.note ? "Rewrite the investor note" : "Write the investor note", `
    <div class="stack" style="margin-top:14px">
      ${dealDocs.length ? fSelect("nf-doc", "Source document", dealDocs.map((x) => [x.id, `${x.name} (${x.category || "Document"})`]), dealDocs[0].id, { blank: "Use the deal record only" }) : ""}
      <div class="field"><label for="nf-file">…or upload a document now</label><input type="file" class="input" id="nf-file" accept=".pdf,.pptx,.docx,.xlsx,.xls,.txt,.md,.png,.jpg,.jpeg,.webp"></div>
      ${fText("nf-macro", "Your market notes (optional)", "", { rows: 3, ph: "Anything current the analysis should assume: rates, sector sentiment, comparable deals, local market conditions. Claude has no live market data, so what you paste here is treated as current." })}
      <label class="check"><input type="checkbox" id="nf-match" checked> Sanity-check "who this suits" against your contacts</label>
      ${d.note ? `<div class="hint">This replaces the current note, including any edits.</div>` : ""}
    </div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="nf-go">${icon("sparkle")}Write the note</button>`), { size: "" });
  $("#nf-go", m).onclick = async () => {
    const docId = val(m, "nf-doc"); const upload = $("#nf-file", m).files[0];
    const macro = val(m, "nf-macro"); const useMatches = $("#nf-match", m).checked;
    closeModal();
    let file = upload || null;
    if (!file && docId) { try { file = await vaultFile(docId); } catch { toast("That vault document couldn't be read. Upload the file instead.", true); } }
    S.ui.dealTab = "note"; go(`#/deals/${dealId}`);
    await generateNote(dealId, { file, macroNotes: macro, useMatches, onStep: (t) => { const el = $("#note-step"); if (el) el.textContent = t; } });
    render();
  };
}
/* Print view: a light, A4-friendly rendering of the current version */
function printNote(dealId) {
  const d = S.deals.get(dealId); const n = noteOf(d); if (!n) return;
  const mode = noteMode(dealId); const wasEdit = NOTE_UI.edit; NOTE_UI.edit = false;
  const body = noteBody(d, n, mode, true); NOTE_UI.edit = wasEdit;
  const css = `@page{size:A4;margin:18mm 16mm}
  *{box-sizing:border-box}body{font:12px/1.55 "Jost","Segoe UI",system-ui,sans-serif;color:#1a1714;background:#fff;margin:0}
  h2,h3{font-family:"Cormorant Garamond",Garamond,serif;font-weight:600;margin:0}
  .eyebrow{font-size:9px;letter-spacing:.16em;text-transform:uppercase;color:#7a7166}
  .nhead{border-bottom:1.5px solid #b99b62;padding-bottom:10px;margin-bottom:14px}
  .nhead h2{font-size:26px;margin-top:4px}
  .nlede{font-family:"Cormorant Garamond",Garamond,serif;font-size:17px;line-height:1.4;margin:10px 0 0}
  .row.between{display:flex;justify-content:space-between;gap:16px}
  .nglance{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:16px;border:1px solid #e6e0d5;border-radius:3px;padding:12px}
  .nglance div{display:flex;flex-direction:column;gap:2px}.nglance b{font-size:14px;font-weight:600}
  .nsec{margin-bottom:16px;break-inside:avoid}
  .nsec h3{font-size:16px;color:#8a6f3d;border-bottom:1px solid #e6e0d5;padding-bottom:4px;margin-bottom:8px}
  .nnote{font-size:10.5px;color:#7a7166;margin-bottom:6px}
  table{width:100%;border-collapse:collapse;font-size:11px}th{text-align:left;font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#7a7166;border-bottom:1px solid #ddd6c9;padding:5px 6px}
  td{padding:6px;border-bottom:1px solid #eee8dd;vertical-align:top}.r{text-align:right}
  .kv{display:grid;grid-template-columns:150px 1fr;gap:6px 14px;margin:0}dt{color:#7a7166;font-size:11px}dd{margin:0}
  .nlist{margin:0;padding-left:16px}.nlist li{margin-bottom:6px}
  .chip{display:inline-block;border:1px solid #ddd6c9;border-radius:10px;padding:1px 7px;font-size:10px;color:#5c554c;margin-right:4px}
  .chip.crit{border-color:#d9b3aa;color:#9c4a37}.chip.warn{border-color:#e3cda3;color:#8a6423}.chip.good{border-color:#bcd2b8;color:#3f6b39}.chip.gold{border-color:#c9ae80;color:#8a6f3d}
  .src{font-size:8.5px;letter-spacing:.08em;text-transform:uppercase;color:#9a9186;border:1px solid #eee8dd;border-radius:3px;padding:0 4px;margin-left:4px}
  .src.int{color:#9c4a37;border-color:#e7cfc8}
  .nverdict p{margin:6px 0}.nswing .eyebrow{margin-right:6px}
  .nmatch,.hint{display:none}
  .nfoot{margin-top:18px;border-top:1px solid #e6e0d5;padding-top:8px;font-size:9.5px;color:#7a7166}
  .ndisc{margin-top:4px;font-size:9px;color:#8a8277}
  .name{font-weight:600}.org,.dim{color:#6d655b}.org{font-size:10.5px}`;
  const html = `<!doctype html><html lang="en-GB"><head><meta charset="utf-8"><title>${esc(d.name)} — ${mode === "internal" ? "internal note" : "investor note"}</title><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@500;600&family=Jost:wght@300;400;500;600&display=swap"><style>${css}</style></head><body>${body}<script>window.onload=()=>{setTimeout(()=>window.print(),400)}<\/script></body></html>`;
  const w = window.open("", "_blank");
  if (w) { w.document.open(); w.document.write(html); w.document.close(); audit("export", "deals", dealId, `Exported the ${mode} investor note for ${d.name}`); return; }
  saveFile(`${d.name.replace(/[^\w -]/g, "").slice(0, 60)} - ${mode} note.html`, html);
}
/* =========================================================
   Standalone sign-in, sessions and admin flows.
   Every secret check happens on the server; the browser never
   sees the access code hash or anyone's PIN hash.
   ========================================================= */
let lastActivity = now();
let lastPing = 0;
const dev = () => deviceLabel();

async function boot() {
  renderGate();
  let st;
  try { st = await UCAPI("GET", "/api/auth/status"); } catch (e) { gate.bootErr = e || null; gate.step = "nodb"; renderGate(); return; }
  window.UC_AI = !!st.ai; window.UC_TRANSPORT = st.transport || "sse";
  await initCapabilities();
  S.security = st.policy;
  gate.lockUntil = st.lockedOut ? now() + st.lockedOut : 0;
  window.addEventListener("uc-auth", (e) => { if (!S.me) return; if (e.detail.state === "locked") showLock("idle", true); else forceSignOut("You were signed out. Please sign in again."); });
  window.addEventListener("resize", debounce(() => { const c = $("#gate canvas"); if (c && !$("#gate").hidden) drawGuilloche(c); }, 200));
  setInterval(() => { if (S.me && !modalStack.length && !document.hidden) scheduleRender(); }, 60000);
  if (!st.initialised) { gate.step = "uninit"; renderGate(); return; }
  if (st.session) { await beginApp(st.session.user, null, st.session.start); if (st.session.locked) showLock("idle", true); return; }
  if (st.gate) { setGateUsers(st.gate); gate.step = st.hasUsers ? "who" : "setup"; }
  else gate.step = "code";
  renderGate();
}
function gateRefresh() { /* the server drives the gate in standalone mode */ }
function lockoutRemaining() { return gate.lockUntil && gate.lockUntil > now() ? gate.lockUntil - now() : 0; }
function setGateUsers(list) { S.users = new Map((list || []).map((u) => [u.id, { ...u, active: true }])); }
function gateError(e, fallback) {
  if (e && e.code === "locked_out") { const m = /(\d+) min/.exec(e.message || ""); gate.lockUntil = now() + (m ? +m[1] : 15) * 60000; }
  gate.msg = (e && e.message) || fallback; renderGate(true);
}
async function submitCode() {
  if (lockoutRemaining() || gate.busy) return;
  const v = $("#g-code").value.trim(); if (!v) return;
  gate.busy = true; renderGate();
  try {
    const r = await UCAPI("POST", "/api/auth/code", { code: v, device: dev() });
    gate.busy = false; gate.msg = ""; setGateUsers(r.users); gate.step = r.needsSetup ? "setup" : "who"; renderGate();
  } catch (e) { gate.busy = false; gateError(e, "That code isn't recognised."); }
}
async function submitSetup() {
  const n1 = $("#su-n1").value.trim(), p1 = $("#su-p1").value, c1 = $("#su-c1").value;
  const n2 = $("#su-n2").value.trim(), p2 = $("#su-p2").value, c2 = $("#su-c2").value;
  const say = (m) => { gate.msg = m; $("#g-msg").textContent = m; };
  if (!n1) return say("Enter the first super admin's name.");
  if (p1 !== c1) return say("The first PIN and its confirmation don't match.");
  if (n2 && p2 !== c2) return say("The second PIN and its confirmation don't match.");
  try {
    const r = await UCAPI("POST", "/api/auth/setup", { admins: [{ name: n1, pin: p1 }, ...(n2 ? [{ name: n2, pin: p2 }] : [])], device: dev() });
    await beginApp(r.user, null, r.start);
  } catch (e) { if (e.code === "exists" || e.code === "gate") { gate.step = "code"; gate.msg = e.message; renderGate(); } else say(e.message || "Couldn't create the accounts."); }
}
async function submitPin() {
  if (lockoutRemaining()) return;
  const pin = gate.pin; gate.pin = "";
  try {
    const r = await UCAPI("POST", "/api/auth/pin", { userId: gate.userId, pin, device: dev() });
    await beginApp(r.user, r.prevLogin, r.start);
  } catch (e) {
    if (e.code === "gate") { gate.step = "code"; gate.msg = "Please enter the access code again."; renderGate(); return; }
    gateError(e, "Incorrect PIN.");
  }
}
async function beginApp(user, prevLogin, start) {
  gate.step = "loading"; renderGate();
  try { await UC_SYNC.start(); } catch (e) { gate.step = "code"; gate.msg = "Couldn't load your data. Try again."; renderGate(); return; }
  S.me = { ...user, ...(S.users.get(user.id) || {}), id: user.id, name: user.name, role: user.role };
  S.session = { uid: user.id, start: start || now() };
  S.prevLogin = prevLogin || null;
  lastActivity = now();
  if (!S._coreSubbed) { S._coreSubbed = true; subscribeCore(); }
  enterApp();
}
function startSession() {}
function resumeSession() { return false; }
async function signOut(reason = "Signed out") {
  const wasIn = !!S.me;
  if (wasIn) { try { await UCAPI("POST", "/api/auth/logout", { reason, device: dev() }); } catch {} }
  localSignOut();
}
function localSignOut(msg) {
  S.me = null; S.session = null; UC_SYNC.stop();
  closeDrawer(); while (modalStack.length) closeModal(); $(".lockveil")?.remove();
  S.contacts = new Map(); S.deals = new Map(); S.intros = new Map(); S.tasks = new Map(); S.docs = new Map(); S.matches = new Map(); S.auditDays = []; S.users = new Map(); S.peers = [];
  MATCH.results = null; MATCH.criteria.brief = "";
  gate.step = "code"; gate.pin = ""; gate.msg = msg || ""; gate.userId = null;
  renderGate();
}
function forceSignOut(msg) { if (S.me) localSignOut(msg); }

/* Activity, idle lock and keep-alive */
function touchActivity() { lastActivity = now(); if (S.me && !$(".lockveil") && now() - lastPing > 60000) { lastPing = now(); UCAPI("POST", "/api/ping").catch(() => {}); } }
["pointerdown", "keydown", "wheel", "touchstart"].forEach((ev) => window.addEventListener(ev, touchActivity, { passive: true }));
setInterval(() => {
  if (!S.me || $(".lockveil")) return;
  const pol = S.security || {};
  if (now() - (S.session?.start || now()) > (pol.maxSessionHours || 12) * 3600000) { signOut("Signed out (maximum session length reached)"); gate.msg = "Your session reached its time limit. Please sign in again."; return; }
  if (now() - lastActivity > (pol.idleMinutes || 15) * 60000) lockScreen("idle");
}, 15000);
function lockScreen(why) { if (!S.me || $(".lockveil")) return; UCAPI("POST", "/api/auth/lock", { why, device: dev() }).catch(() => {}); showLock(why, false); }
function showLock(why, serverLocked) {
  if (!S.me || $(".lockveil")) return;
  UC_SYNC.pause();
  const v = document.createElement("div"); v.className = "lockveil";
  v.innerHTML = `<div class="gate-card"><div class="wordmark" style="font-size:40px">U<i>Connect</i></div><div class="stack" style="align-items:center;gap:8px"><span class="av" style="width:48px;height:48px;font-size:16px">${esc(initials(S.me.name))}</span><div>${esc(S.me.name)}</div><div class="hint">Locked ${why === "manual" ? "by you" : "after inactivity"}</div></div>
  <form class="gate-form" id="lk-form" autocomplete="off"><label class="gate-label" for="lk-pin">PIN to unlock</label><input class="code-input" id="lk-pin" type="password" inputmode="numeric" autocomplete="off" autofocus><div class="gate-msg" id="lk-msg"></div><button class="btn primary" type="submit">Unlock</button><button class="btn ghost sm" type="button" id="lk-out">Sign out instead</button></form></div>`;
  document.body.appendChild(v);
  setTimeout(() => v.querySelector("#lk-pin")?.focus(), 50);
  v.querySelector("#lk-out").onclick = () => signOut("Signed out from lock screen");
  v.querySelector("#lk-form").onsubmit = async (e) => {
    e.preventDefault();
    try {
      await UCAPI("POST", "/api/auth/unlock", { pin: v.querySelector("#lk-pin").value, device: dev() });
      v.remove(); lastActivity = now(); UC_SYNC.resume();
    } catch (err) {
      if (err.code === "signed_out") { localSignOut(err.message); return; }
      v.querySelector("#lk-msg").textContent = err.message || "Incorrect PIN."; v.querySelector("#lk-pin").value = "";
      const card = v.querySelector(".gate-card"); card.classList.remove("shake"); void card.offsetWidth; card.classList.add("shake");
    }
  };
}

/* Audit: the server stamps who, when and from which IP */
const auditBuf = [];
let auditTimer = null;
function audit(action, entity, id, summary) {
  auditBuf.push({ a: action, e: entity || "", i: id || "", s: String(summary || "").slice(0, 300), d: dev() });
  clearTimeout(auditTimer); auditTimer = setTimeout(flushAudit, 250);
}
async function flushAudit() { if (!auditBuf.length || !S.me) return; const events = auditBuf.splice(0, 50); try { await UCAPI("POST", "/api/audit", { events }); } catch {} if (auditBuf.length) flushAudit(); }

function writeErr(e) {
  const code = e && e.code;
  if (code === "permission_denied") toast("Only a super admin can do that.", true);
  else if (code === "invalid_argument") toast("That record couldn't be saved. Shorten long notes and try again.", true);
  else if (code === "locked" || code === "signed_out") toast("Unlock UConnect to save changes.", true);
  else toast("Couldn't save. Check your connection and try again.", true);
  console.warn(e);
}
function aiErr(e) {
  const c = e && e.code;
  if (c === "not_configured") return "AI isn't set up on the server yet. Add an Anthropic API key to turn it on.";
  if (c === "rate_limited") return "Claude is busy. Wait a moment, then try again.";
  if (c === "prompt_too_large") return "That's too much text for one request. Shorten the brief or import fewer rows at a time.";
  if (c === "invalid_json") return "Claude's answer couldn't be read. Try again.";
  if (c === "cancelled") return "Stopped.";
  return "Claude couldn't complete that request. Try again shortly.";
}

/* PIN confirmation for sensitive actions */
function pinPrompt(title, text) {
  return new Promise((res) => {
    const m = openModal(modalShell(title, `<p class="dim" style="margin-top:14px">${text}</p><div class="field"><label for="pp-pin">Your PIN</label><input class="input" id="pp-pin" type="password" inputmode="numeric" autocomplete="off" maxlength="12" autofocus></div><div class="gate-msg" id="pp-msg"></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="pp-ok">Confirm</button>`), { size: "narrow", onClose: () => res(false) });
    const goOn = async () => {
      try { await UCAPI("POST", "/api/auth/verify-pin", { pin: m.querySelector("#pp-pin").value, purpose: title, device: dev() }); modalStack.pop().wrap.remove(); res(true); }
      catch (e) { m.querySelector("#pp-msg").textContent = e.message || "That PIN doesn't match."; }
    };
    m.querySelector("#pp-ok").onclick = goOn; m.querySelector("#pp-pin").onkeydown = (e) => e.key === "Enter" && goOn();
  });
}
function askPin(title, text, okLabel = "Confirm") {
  return new Promise((res) => {
    const m = openModal(modalShell(title, `<p class="dim" style="margin-top:14px">${text}</p><div class="field"><label for="ap-pin">Your PIN</label><input class="input" id="ap-pin" type="password" inputmode="numeric" autocomplete="off" maxlength="12" autofocus></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="ap-ok">${esc(okLabel)}</button>`), { size: "narrow", onClose: () => res(null) });
    const done = () => { const v = m.querySelector("#ap-pin").value; modalStack.pop().wrap.remove(); res(v); };
    m.querySelector("#ap-ok").onclick = done; m.querySelector("#ap-pin").onkeydown = (e) => e.key === "Enter" && done();
  });
}

/* Admin: users, PINs, access code, policies, sessions */
function userForm(id) {
  const u = id ? S.users.get(id) : null;
  const m = openModal(modalShell(u ? "Rename user" : "Add user", `<div class="stack" style="margin-top:14px">${fInput("uf-name", "Full name", u?.name)}${u ? "" : fSelect("uf-role", "Role", [["member", "Member"], ["superadmin", "Super admin"]], "member", { blank: null }) + fInput("uf-pin", "Starting PIN (6 to 12 digits)", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' }) + fInput("uf-auth", "Your PIN, to authorise", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' }) + `<div class="hint">Give the starting PIN to them in person. They can change it in Settings.</div>`}<div class="gate-msg" id="uf-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="uf-ok">${u ? "Save" : "Add user"}</button>`), { size: "narrow" });
  $("#uf-ok", m).onclick = async () => {
    const name = val(m, "uf-name"); if (!name) return;
    try {
      if (u) await UCAPI("PATCH", "/api/admin/users/" + id, { name, device: dev() });
      else await UCAPI("POST", "/api/admin/users", { name, role: val(m, "uf-role"), pin: $("#uf-pin", m).value, authPin: $("#uf-auth", m).value, device: dev() });
      closeModal(); toast(u ? "Saved." : `${name} added.`);
    } catch (e) { $("#uf-msg", m).textContent = e.message || "Couldn't save."; }
  };
}
function pinForm(id) {
  const u = S.users.get(id); const self = id === S.me.id;
  const m = openModal(modalShell(self ? "Change your PIN" : `Reset ${esc(u.name)}'s PIN`, `<div class="stack" style="margin-top:14px">${fInput("pf-cur", self ? "Current PIN" : "Your PIN, to authorise", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}${fInput("pf-a", "New PIN (6 to 12 digits)", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}${fInput("pf-b", "Repeat new PIN", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}${self ? "" : `<div class="hint">${esc(u.name)} is signed out everywhere and must use the new PIN.</div>`}<div class="gate-msg" id="pf-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="pf-ok">Save PIN</button>`), { size: "narrow" });
  $("#pf-ok", m).onclick = async () => {
    const say = (t) => ($("#pf-msg", m).textContent = t);
    const a = $("#pf-a", m).value; if (a !== $("#pf-b", m).value) return say("The new PINs don't match.");
    try {
      if (self) await UCAPI("POST", "/api/auth/change-pin", { current: $("#pf-cur", m).value, next: a, device: dev() });
      else await UCAPI("PATCH", "/api/admin/users/" + id, { pin: a, authPin: $("#pf-cur", m).value, device: dev() });
      closeModal(); toast("PIN updated.");
    } catch (e) { say(e.message || "Couldn't update the PIN."); }
  };
}
ACT["user-more"] = (el) => {
  const u = S.users.get(el.dataset.id); const admins = [...S.users.values()].filter((x) => x.role === "superadmin" && x.active !== false);
  const lastAdmin = u.role === "superadmin" && admins.length <= 1;
  const act = async (label, body, text) => { const pin = await askPin(label, text); if (pin === null) return; try { await UCAPI("PATCH", "/api/admin/users/" + u.id, { ...body, authPin: pin, device: dev() }); toast("Done."); } catch (e) { toast(e.message || "Couldn't do that.", true); } };
  showMenu(el, [
    { label: "Rename", run: () => userForm(u.id) },
    { label: "Reset PIN", run: () => pinForm(u.id) },
    ...(u.id !== S.me.id && !lastAdmin ? [{ label: u.role === "superadmin" ? "Make member" : "Make super admin", run: () => act("Change role", { role: u.role === "superadmin" ? "member" : "superadmin" }, `Change ${esc(u.name)}'s role?`) }] : []),
    ...(u.id !== S.me.id && !lastAdmin ? [{ label: u.active === false ? "Reactivate" : "Deactivate", run: () => act(u.active === false ? "Reactivate" : "Deactivate", { active: u.active === false }, `${u.active === false ? "Reactivate" : "Deactivate"} ${esc(u.name)}? ${u.active === false ? "" : "They are signed out immediately on every device."}`) }] : []),
    ...(u.id !== S.me.id ? [{ label: "Sign out of every device", run: async () => { const pin = await askPin("Sign out everywhere", `End all of ${esc(u.name)}'s sessions now?`); if (pin === null) return; try { await UCAPI("POST", "/api/admin/sessions/revoke", { userId: u.id, authPin: pin, device: dev() }); toast("Signed out."); S.adminSessions = null; render(); } catch (e) { toast(e.message, true); } } }] : []),
  ]);
};
ACT["code-rotate"] = () => {
  const m = openModal(modalShell("Change access code", `<div class="stack" style="margin-top:14px"><div class="hint">At least 8 characters with letters and numbers. Share it in person or by phone, not by email.</div>${fInput("cr-a", "New access code", "", { type: "password", attrs: 'autocomplete="new-password"' })}${fInput("cr-b", "Repeat new code", "", { type: "password", attrs: 'autocomplete="new-password"' })}${fInput("cr-pin", "Your PIN", "", { type: "password", attrs: 'inputmode="numeric" autocomplete="off"' })}<div class="gate-msg" id="cr-msg"></div></div>`, `<button class="btn ghost" data-act="modal-close">Cancel</button><button class="btn primary" id="cr-ok">Change code</button>`), { size: "narrow" });
  $("#cr-ok", m).onclick = async () => {
    const a = $("#cr-a", m).value; if (a !== $("#cr-b", m).value) { $("#cr-msg", m).textContent = "The two codes don't match."; return; }
    try { await UCAPI("POST", "/api/admin/code", { code: a, authPin: $("#cr-pin", m).value, device: dev() }); closeModal(); toast("Access code changed."); }
    catch (e) { $("#cr-msg", m).textContent = e.message || "Couldn't change the code."; }
  };
};
ACT["sec-export"] = async () => {
  try {
    const r = await UCAPI("GET", "/api/admin/audit", undefined, { headers: { "X-Device": dev() } });
    saveFile(`uconnect-access-log-${todayISO()}.csv`, toCSV(r.events, [["Time", (e) => new Date(e.t).toISOString()], ["User", "n"], ["Event", (e) => ACTION_LABEL[e.a] || e.a], ["Detail", "s"], ["Entity", "e"], ["Record", "i"], ["Device and IP", "d"]]));
  } catch (e) { toast(e.message || "Couldn't export the log.", true); }
};
afterHooks.push(() => {
  const f = $("#pol-form"); if (!f) return;
  f.onsubmit = async (e) => {
    e.preventDefault();
    try { await UCAPI("POST", "/api/admin/policy", { idleMinutes: +val(f, "pol-idle"), maxSessionHours: +val(f, "pol-max"), maxAttempts: +val(f, "pol-att"), lockMinutes: +val(f, "pol-lock"), device: dev() }); toast("Policies saved."); }
    catch (err) { toast(err.message || "Couldn't save policies.", true); }
  };
});
/* Active sessions panel on Security → Users */
const baseSecurityView = VIEWS.security;
VIEWS.security = () => {
  let html = baseSecurityView();
  if (!isAdmin() || S.ui.security.tab !== "users") return html;
  if (!S.adminSessions || now() - S.adminSessions.at > 15000) {
    if (!S._sessLoading) { S._sessLoading = true; UCAPI("GET", "/api/admin/sessions").then((r) => { S.adminSessions = { at: now(), list: r.sessions }; }).catch(() => { S.adminSessions = { at: now(), list: [] }; }).finally(() => { S._sessLoading = false; scheduleRender(); }); }
  }
  const list = S.adminSessions?.list || [];
  html += `<section class="panel" style="margin-top:18px"><div class="panel-h"><h2 class="h-card">Active sessions</h2><span class="hint">Signed-in browsers, with IP address</span></div>
  ${list.length ? `<div class="tablewrap" style="border:0;border-radius:0"><table class="t"><thead><tr><th>User</th><th>Signed in</th><th>Last active</th><th>Status</th><th>Device and IP</th></tr></thead><tbody>${list.map((x) => `<tr class="nohover"><td class="name">${esc(x.name)}</td><td class="dim">${esc(fmtTime(x.created_at))}</td><td class="dim">${esc(ago(x.last_seen))}</td><td>${x.locked ? '<span class="chip">Locked</span>' : '<span class="chip good">Active</span>'}</td><td class="mono muted">${esc([x.device, x.ip].filter(Boolean).join(" · "))}</td></tr>`).join("")}</tbody></table></div>` : `<div class="empty" style="padding:24px"><span class="hint">${S.adminSessions ? "No active sessions." : "Loading…"}</span></div>`}</section>`;
  return html;
};
boot();
