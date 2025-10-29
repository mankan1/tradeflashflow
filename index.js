/* eslint-disable no-console */
import "./instrument-http.js";
import { httpMetrics } from "./instrument-http.js";
import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import WebSocket, { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";

/* ================= CONFIG ================= */
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";
const PORT = process.env.PORT || 8080;

const TRADIER_REST   = process.env.TRADIER_BASE || "https://api.tradier.com";
// const TRADIER_TOKEN  = process.env.TRADIER_TOKEN || "REPLACE_ME";
// const ALPACA_KEY     = process.env.ALPACA_KEY     || "";
// const ALPACA_SECRET  = process.env.ALPACA_SECRET  || "";
const TRADIER_TOKEN  = "DZi4KKhQVv05kjgqXtvJRyiFbEhn"; //process.env.TRADIER_TOKEN; // set me
const ALPACA_KEY     = "AKNND2CVUEIRFCDNVMXL2NYVWD"; // process.env.ALPACA_KEY;    // set me
const ALPACA_SECRET  = "5xBdG2Go1PtWE36wnCrB4vES6mGF6tkusqDL7uSnnCxy"; 
// chain auto-expand
const MAX_STRIKES_AROUND_ATM = 40;   // total (±20)
const MAX_EXPIRY_DAYS        = 30;

// sweep / block thresholds
const SWEEP_WINDOW_MS        = 600;
const SWEEP_MIN_QTY          = 300;
const SWEEP_MIN_NOTIONAL     = 75000;

const BLOCK_MIN_QTY          = 250;
const BLOCK_MIN_NOTIONAL     = 100000;

// fallback timesales polling
const FALLBACK_IDLE_MS       = 3500;
const FALLBACK_POLL_EVERY_MS = 1500;

/* ================= APP / WS ================= */
const app = express();
// CORS — allow your site + local dev, send headers on all responses (incl. 304)
const ALLOWED_ORIGINS = [
  'https://www.tradeflow.lol',
  'https://tradeflow.lol',
  'http://localhost:5173',
  'http://localhost:3000',
  'http://localhost:19006',
];

const corsOptions = {
  origin(origin, cb) {
    // allow same-origin/no-origin (mobile apps, curl) and whitelisted sites
    if (!origin || ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error('Not allowed by CORS'));
  },
  methods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'],
  allowedHeaders: ['Content-Type','Authorization'],
  credentials: false,           // set true only if you use cookies/auth headers across origins
  maxAge: 86400,                // cache preflight
};

// Ensure Vary so proxies don’t reuse wrong CORS headers
app.use((req,res,next) => { res.setHeader('Vary','Origin'); next(); });

// Add CORS before any routes/middleware that send responses
app.use(cors(corsOptions));
// Also handle preflight for all routes
app.options('*', cors(corsOptions));

app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

/* ================= BUFFERS ================= */
const MAX_BUFFER = 400;
const buffers = {
  options_ts: [],
  equity_ts: [],
  sweeps:    [],
  blocks:    [],
  chains:    [],
};
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));
/* ===== EOD OI storage for confirmation ===== */
const eodOiByDateOcc = new Map(); // key: `${date}|${occ}` -> number
const lastEodDateForOcc = new Map(); // occ -> last date string we saw for that occ
/* ================= STATE ================= */
// equities NBBO (bid/ask), options NBBO (by OCC)
const eqQuote  = new Map();
const optNBBO  = new Map();
// OI / VOL baselines per OCC key (UL|YYYY-MM-DD|strike|right)
const optState = new Map();
const dayKey = () => new Date().toISOString().slice(0,10);

// watches
const alpacaSubs = new Set();               // equities
const tradierOptWatch = { set: new Set() }; // option JSON strings

// fallback + sweeps
const optLastPrintTs = new Map(); // occ -> last ts seen
const sweepBuckets   = new Map(); // occ -> burst bucket

// very light “lean memory” of recent opens per OCC
const recentOpenLean = new Map(); // occ -> "BTO" | "STO" | "UNK"
function ymdFromTs(ts) {
  const d = new Date(ts);
  return d.toISOString().slice(0,10);
}

/**
 * Update buffers.* items for a given OCC on a given date (the trade-date)
 * using OI delta between EOD(date-1) and EOD(date).
 * We set:
 *   m.oi_after, m.oi_delta, m.oc_confirm ("OPEN_CONFIRMED" | "CLOSE_CONFIRMED" | "INCONCLUSIVE")
 *   m.oc_confirm_reason
 */
function confirmOccForDate(occ, dateStr) {
  const d0 = new Date(dateStr);
  const prev = new Date(d0.getTime() - 24*3600*1000);
  const prevDate = prev.toISOString().slice(0,10);

  const oiPrev = eodOiByDateOcc.get(`${prevDate}|${occ}`);
  const oiCurr = eodOiByDateOcc.get(`${dateStr}|${occ}`);

  if (!Number.isFinite(oiPrev) || !Number.isFinite(oiCurr)) return 0;
  const delta = oiCurr - oiPrev;

  const confirmOne = (m) => {
    // only annotate prints that belong to that OCC and calendar date
    if (m.occ !== occ) return false;
    const day = ymdFromTs(m.ts || Date.now());
    if (day !== dateStr) return false;

    // Default
    let oc_confirm = "INCONCLUSIVE";
    let reason = `ΔOI=${delta} from ${prevDate}→${dateStr}`;

    // Rules:
    //   ΔOI > 0  → net opens.  If we tagged BTO/STO, mark OPEN_CONFIRMED
    //   ΔOI < 0  → net closes. If we tagged BTC/STC, mark CLOSE_CONFIRMED
    // (Mixed intraday flows can still be inconclusive at single-print level.)
    if (delta > 0 && (m.oc_intent === "BTO" || m.oc_intent === "STO")) {
      oc_confirm = "OPEN_CONFIRMED";
      reason += " (OI increased → opens)";
    } else if (delta < 0 && (m.oc_intent === "BTC" || m.oc_intent === "STC")) {
      oc_confirm = "CLOSE_CONFIRMED";
      reason += " (OI decreased → closes)";
    }

    // annotate
    m.oi_after = oiCurr;
    m.oi_delta = delta;
    m.oc_confirm = oc_confirm;
    m.oc_confirm_reason = reason;
    m.oc_confirm_ts = Date.now();
    return true;
  };

  let touched = 0;
  for (const arrName of ["options_ts", "sweeps", "blocks"]) {
    const arr = buffers[arrName];
    for (const m of arr) if (confirmOne(m)) touched++;
  }
  return touched;
}

/** Record EOD OI rows, return #rows recorded */
function recordEodRows(dateStr, rows) {
  let n = 0;
  for (const r of rows) {
    // allow either explicit occ or UL/exp/right/strike
    let occ = r.occ;
    if (!occ && r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike))) {
      occ = toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike));
    }
    const oi = Number(r.oi);
    if (!occ || !Number.isFinite(oi)) continue;

    eodOiByDateOcc.set(`${dateStr}|${occ}`, oi);
    lastEodDateForOcc.set(occ, dateStr);
    n++;
  }
  return n;
}
/* ================= HELPERS ================= */
function getOptState(key) {
  const today = dayKey();
  const s = optState.get(key) ?? { oi_before: 0, vol_today_before: 0, last_reset: today };
  if (s.last_reset !== today) { s.vol_today_before = 0; s.last_reset = today; }
  optState.set(key, s);
  return s;
}
const setOptOI  = (k, oi) => { const s = getOptState(k); s.oi_before = Number(oi)||0; optState.set(k, s); };
const setOptVol = (k, v)  => { const s = getOptState(k); s.vol_today_before = Math.max(0, Number(v)||0); optState.set(k, s); };
const bumpOptVol = (k, qty) => { const s = getOptState(k); s.vol_today_before += Number(qty)||0; optState.set(k, s); };

function stableId(msg) {
  const key = JSON.stringify({
    type: msg.type, provider: msg.provider,
    symbol: msg.symbol ?? msg.underlying,
    occ: msg.occ, side: msg.side,
    price: msg.price, size: msg.size,
    ts: msg.ts ?? msg.time ?? msg.at
  });
  return createHash("sha1").update(key).digest("hex");
}
function normalizeForFanout(msg) {
  const now = Date.now();
  const withTs = { ts: typeof msg.ts === "number" ? msg.ts : now, ...msg };
  const id = msg.id ?? stableId(withTs) ?? randomUUID();
  return { id, ...withTs };
}
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}
function pushAndFanout(msg) {
  const m = normalizeForFanout(msg);
  const arr = buffers[m.type];
  if (!arr) return;

  const seen = seenIds[m.type] || (seenIds[m.type] = new Set());
  if (seen.has(m.id)) return;

  arr.unshift(m);
  seen.add(m.id);
  while (arr.length > MAX_BUFFER) {
    const dropped = arr.pop();
    if (dropped?.id) seen.delete(dropped.id);
  }
  broadcast(m);
}

function toOcc(ul, expISO, right, strike) {
  const yymmdd = expISO.replaceAll("-", "").slice(2);
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${ul.toUpperCase()}${yymmdd}${cp}${k}`;
}
function parseOcc(occ) {
  const m = /^([A-Z]+)(\d{6})([CP])(\d{8})$/.exec(occ);
  if (!m) return null;
  const [ , ul, yymmdd, cp, k ] = m;
  const exp = `20${yymmdd.slice(0,2)}-${yymmdd.slice(2,4)}-${yymmdd.slice(4,6)}`;
  return { ul, exp, right: cp === "C" ? "CALL" : "PUT", strike: Number(k)/1000 };
}

/* ---- aggressor from NBBO (for options) ---- */
function classifyAggressor(price, nbbo) {
  if (!nbbo) return { side: "UNK", at: "MID" };
  const bid = Number(nbbo.bid ?? 0);
  const ask = Number(nbbo.ask ?? 0);
  if (!Number.isFinite(ask) || ask <= 0) return { side: "UNK", at: "MID" };
  const mid = (bid + ask) / 2;
  const eps = Math.max(0.01, (ask - bid) / 20);
  if (price >= Math.max(ask - eps, bid)) return { side: "BUY",  at: "ASK" };
  if (price <= Math.min(bid + eps, ask)) return { side: "SELL", at: "BID" };
  return { side: price >= mid ? "BUY" : "SELL", at: "MID" };
}

/* ---- opening/closing inference ---- */
function inferIntent(occ, side, qty, price) {
  const s = getOptState(occ);
  const nbbo = optNBBO.get(occ);
  const { side: aggrSide, at } = classifyAggressor(price, nbbo);

  let tag = "UNK"; // "BTO"|"STO"|"BTC"|"STC"|"UNK"
  let conf = 0.35;
  const reasons = [];

  // OPEN if qty > (yday OI + today VOL so far)
  if (qty > (s.oi_before + s.vol_today_before)) {
    tag = side === "BUY" ? "BTO" : "STO";
    conf = 0.8;
    reasons.push("qty > (yday OI + today vol)");
    recentOpenLean.set(occ, tag);
  } else {
    const prior = recentOpenLean.get(occ) || "UNK";
    const qtyWithinOI = qty <= s.oi_before;
    if (qtyWithinOI) reasons.push("qty <= yday OI");

    // “Human lean”:
    if (prior === "BTO" && side === "SELL" && (at === "BID" || aggrSide === "SELL")) {
      tag = "STC"; conf = qtyWithinOI ? 0.7 : 0.55;
      reasons.push("prior=open(BTO)", "sell@bid");
    } else if (prior === "STO" && side === "BUY" && (at === "ASK" || aggrSide === "BUY")) {
      tag = "BTC"; conf = qtyWithinOI ? 0.7 : 0.55;
      reasons.push("prior=open(STO)", "buy@ask");
    } else {
      if (side === "SELL" && at === "BID" && qtyWithinOI) { tag = "STC"; conf = 0.55; reasons.push("sell@bid"); }
      if (side === "BUY"  && at === "ASK" && qtyWithinOI) { tag = "BTC"; conf = 0.55; reasons.push("buy@ask"); }
    }
  }

  return { tag, conf, at, reasons, oi_yday: s.oi_before, vol_before: s.vol_today_before };
}

/* ---- normalized prints ---- */
function normOptionsPrint(provider, p) {
  const now = Date.now();
  const key = `${p.underlying}|${p.option.expiration}|${p.option.strike}|${p.option.right}`;
  if (Number.isFinite(p.oi)) setOptOI(key, Number(p.oi));
  if (Number.isFinite(p.volToday)) setOptVol(key, Number(p.volToday));
  const occ = p.occ ?? toOcc(p.underlying, p.option.expiration, p.option.right, p.option.strike);

  const intent = inferIntent(occ, p.side, p.size, p.price);
  bumpOptVol(key, p.size);

  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "options_ts",
    provider,
    symbol: p.underlying,
    occ,
    option: p.option,
    side: p.side,        // BUY | SELL
    qty: p.size,
    price: p.price,
    oc_intent: intent.tag,       // BTO/STO/BTC/STC/UNK
    intent_conf: intent.conf,    // 0..1
    fill_at: intent.at,          // BID|ASK|MID
    intent_reasons: intent.reasons,
    oi_before: intent.oi_yday,
    vol_before: intent.vol_before,
    venue: p.venue ?? null
  };
}
function normEquityPrint(provider, { symbol, price, size }) {
  const now = Date.now();
  const q = eqQuote.get(symbol);
  const side = (!q?.ask || !q?.bid)
    ? "MID"
    : (price >= q.ask ? "BUY" : (price <= q.bid ? "SELL" : "MID"));
  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "equity_ts",
    provider,
    symbol,
    side, qty: size, price,
    action: "UNK",
    venue: null
  };
}

/* ================= ALPACA (equities) ================= */
let alpacaWS = null;
async function ensureAlpacaWS() {
  if (MOCK) return;
  if (alpacaWS && alpacaWS.readyState === 1) return;
  if (!ALPACA_KEY || !ALPACA_SECRET) { console.warn("Alpaca keys missing"); return; }

  const url = "wss://stream.data.alpaca.markets/v2/iex";
  alpacaWS = new WebSocket(url);

  alpacaWS.onopen = () => {
    try {
      alpacaWS.send(JSON.stringify({ action: "auth", key: ALPACA_KEY, secret: ALPACA_SECRET }));
      if (alpacaSubs.size) {
        const list = Array.from(alpacaSubs);
        alpacaWS.send(JSON.stringify({ action: "subscribe", trades: list, quotes: list }));
      }
    } catch {}
  };
  alpacaWS.onmessage = (e) => {
    try {
      const arr = JSON.parse(String(e.data));
      if (!Array.isArray(arr)) return;
      for (const m of arr) {
        if (m.T === "q") eqQuote.set(m.S, { bid: m.bp, ask: m.ap, ts: Date.parse(m.t) || Date.now() });
        if (m.T === "t") pushAndFanout(normEquityPrint("alpaca", { symbol: m.S, price: m.p, size: m.s || m.z || 0 }));
      }
    } catch {}
  };
  alpacaWS.onclose = () => setTimeout(ensureAlpacaWS, 1200);
  alpacaWS.onerror = () => {};
}
function resubAlpaca() {
  if (!alpacaWS || alpacaWS.readyState !== 1) return;
  const list = Array.from(alpacaSubs);
  try { alpacaWS.send(JSON.stringify({ action: "unsubscribe", trades: ["*"], quotes: ["*"] })); } catch {}
  try { alpacaWS.send(JSON.stringify({ action: "subscribe", trades: list, quotes: list })); } catch {}
}
function pruneOptionsForUnderlyings(uls) {
  const before = tradierOptWatch.set.size;
  for (const entry of Array.from(tradierOptWatch.set)) {
    try {
      const o = JSON.parse(entry);
      if (uls.has(String(o.underlying).toUpperCase())) tradierOptWatch.set.delete(entry);
    } catch {}
  }
  return before - tradierOptWatch.set.size;
}

/* ================= TRADIER (equities + options) ================= */
const tradier = { ws: null, sessionId: null, connecting: false };

function buildTradierWsUrl(sessionId, symbols) {
  const params = new URLSearchParams({
    sessionid: sessionId,
    symbols: symbols.join(","),
    filter: ["quote","trade","timesale"].join(","),
    linebreak: "true",
    validOnly: "true",
  });
  return `wss://ws.tradier.com/v1/markets/events?${params.toString()}`;
}
function buildSymbolList() {
  const eq = Array.from(alpacaSubs);
  const occ = Array.from(tradierOptWatch.set).map(JSON.parse).map(o => toOcc(o.underlying, o.expiration, o.right, o.strike));
  return Array.from(new Set([...eq, ...occ]));
}

async function openTradierWsWith(symbols) {
  if (!TRADIER_TOKEN) { console.warn("[tradier] missing token"); return; }

  const r = await fetch(`${TRADIER_REST}/v1/markets/events/session`, {
    method: "POST",
    headers: { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" }
  });
  const j = await r.json();
  const sid = j?.stream?.sessionid ?? j?.sessionid ?? null;
  if (!sid) { console.warn("[tradier] session id null"); return; }
  tradier.sessionId = sid;

  const url = buildTradierWsUrl(sid, symbols);
  tradier.ws = new WebSocket(url);

  tradier.ws.onopen = () => {
    const payload = {
      sessionid: sid,
      symbols,
      filter: ["quote","trade","timesale"],
      linebreak: true,
      validOnly: true
    };
    try { tradier.ws.send(JSON.stringify(payload)); } catch {}
  };

  tradier.ws.onmessage = (ev) => {
    const text = String(ev.data || "");
    const lines = text.split(/\r?\n/).filter(Boolean);
    for (const ln of lines) {
      let msg; try { msg = JSON.parse(ln); } catch { continue; }
      const t = (msg.type || "").toLowerCase();

      // quotes (equity or OCC option)
      if (t === "quote" && msg.symbol) {
        const sym = String(msg.symbol);
        const bid = Number(msg.bid ?? msg.b ?? msg.bp);
        const ask = Number(msg.ask ?? msg.a ?? msg.ap);
        if (/^[A-Z]+(\d{6})([CP])(\d{8})$/.test(sym)) {
          if (Number.isFinite(bid) && Number.isFinite(ask) && ask > 0) {
            optNBBO.set(sym, { bid, ask, ts: Date.now() });
          }
        } else {
          if (Number.isFinite(bid) || Number.isFinite(ask)) {
            eqQuote.set(sym, { bid, ask, ts: Date.now() });
          }
        }
        continue;
      }

      // trades / timesales
      if ((t === "trade" || t === "timesale") && msg.symbol) {
        const sym = String(msg.symbol);

        // option by OCC
        const mo = parseOcc(sym);
        if (mo) {
          const price = Number(msg.price ?? msg.last ?? msg.p);
          const size  = Number(msg.size  ?? msg.volume ?? msg.s);
          if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
          const side  = String(msg.side || msg.taker_side || "").toUpperCase().includes("SELL") ? "SELL" : "BUY";

          optLastPrintTs.set(sym, Date.now());

          const out = normOptionsPrint("tradier", {
            underlying: mo.ul,
            option: { expiration: mo.exp, strike: mo.strike, right: mo.right },
            price, size, side, venue: msg.exchange || msg.exch, occ: sym
          });
          pushAndFanout(out);

          // blocks
          const notion = price * size * 100;
          if (size >= BLOCK_MIN_QTY || notion >= BLOCK_MIN_NOTIONAL) {
            pushAndFanout({
              type: "blocks",
              provider: "tradier",
              ts: out.ts,
              symbol: mo.ul,
              occ: out.occ,
              option: out.option,
              side: out.side,
              qty: out.qty,
              price: out.price,
              notional: notion,
              oc_intent: out.oc_intent,
              intent_conf: out.intent_conf,
              fill_at: out.fill_at
            });
          }

          // sweeps (per-contract burst)
          const existing = sweepBuckets.get(sym);
          const now = Date.now();
          if (!existing || (now - existing.startTs > SWEEP_WINDOW_MS) || (existing.side !== out.side)) {
            sweepBuckets.set(sym, {
              side: out.side, startTs: now, totalQty: out.qty, notional: notion,
              prints: [{ ts: out.ts, qty: out.qty, price: out.price, venue: out.venue }]
            });
          } else {
            existing.totalQty += out.qty;
            existing.notional += notion;
            existing.prints.push({ ts: out.ts, qty: out.qty, price: out.price, venue: out.venue });
          }
          const bucket = sweepBuckets.get(sym);
          if (bucket && (bucket.totalQty >= SWEEP_MIN_QTY || bucket.notional >= SWEEP_MIN_NOTIONAL)) {
            pushAndFanout({
              type: "sweeps",
              provider: "tradier",
              ts: now,
              symbol: mo.ul,
              occ: out.occ,
              option: out.option,
              side: bucket.side,
              totalQty: bucket.totalQty,
              notional: bucket.notional,
              prints: bucket.prints,
              oc_intent: out.oc_intent,
              intent_conf: out.intent_conf,
              fill_at: out.fill_at
            });
            sweepBuckets.delete(sym);
          }
          continue;
        }

        // equity trade
        const price = Number(msg.price ?? msg.last ?? msg.p);
        const size  = Number(msg.size  ?? msg.volume ?? msg.s);
        if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
        pushAndFanout(normEquityPrint("tradier", { symbol: sym, price, size }));
      }
    }
  };

  tradier.ws.onclose = () => {
    console.warn("[tradier] WS close -> reconnect");
    tradier.ws = null; tradier.sessionId = null;
    setTimeout(ensureTradierWS, 800);
  };
  tradier.ws.onerror = (e) => console.warn("[tradier] WS error", e?.message || e);
}

async function ensureTradierWS(forceResub = false) {
  if (MOCK) return;
  const symbols = buildSymbolList();
  if (!symbols.length) return;

  if (!tradier.ws || tradier.ws.readyState !== 1) {
    if (tradier.connecting) return;
    tradier.connecting = true;
    try { await openTradierWsWith(symbols); }
    finally { tradier.connecting = false; }
    return;
  }
  if (forceResub) {
    const payload = {
      sessionid: tradier.sessionId,
      symbols,
      filter: ["quote","trade","timesale"],
      linebreak: true,
      validOnly: true
    };
    try { tradier.ws.send(JSON.stringify(payload)); } catch (e) {
      try { tradier.ws.close(); } catch {}
      setTimeout(() => ensureTradierWS(false), 250);
    }
  }
}

/* ================= FALLBACK TIMESALES POLLER ================= */
const lastPollCursor = new Map(); // occ -> ISO cursor
async function pollTimesalesOnce() {
  if (MOCK || !TRADIER_TOKEN) return;

  const now = Date.now();
  const occs = Array.from(tradierOptWatch.set).map(JSON.parse)
    .map(o => toOcc(o.underlying, o.expiration, o.right, o.strike));

  for (const occ of occs) {
    const last = optLastPrintTs.get(occ) || 0;
    if (now - last < FALLBACK_IDLE_MS) continue;

    const cursorISO = lastPollCursor.get(occ) || new Date(now - 5000).toISOString();
    const params = new URLSearchParams({ symbol: occ, interval: "tick", start: cursorISO, session_filter: "all" });
    const url = `${TRADIER_REST}/v1/markets/timesales?${params.toString()}`;
    const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${TRADIER_TOKEN}` } });
    if (!r.ok) continue;
    const j = await r.json();
    const prints = j?.series?.data || j?.data || [];
    let maxTs = cursorISO ? Date.parse(cursorISO) : 0;

    for (const p of prints) {
      const price = Number(p.price ?? p.last ?? p.p ?? 0);
      const size  = Number(p.size  ?? p.volume ?? p.s ?? 0);
      const ts    = p.timestamp ? Date.parse(p.timestamp)
                   : (p.time ? Date.parse(p.time) : Date.now());
      if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
      maxTs = Math.max(maxTs, ts);

      const meta = parseOcc(occ);
      if (!meta) continue;
      const side  = (String(p.side || p.taker_side || "").toUpperCase().includes("SELL")) ? "SELL" : "BUY";

      const out = normOptionsPrint("tradier", {
        underlying: meta.ul,
        option: { expiration: meta.exp, strike: meta.strike, right: meta.right },
        price, size, side, venue: (p.exch || p.venue) ?? null, occ
      });
      out.ts = ts;
      pushAndFanout(out);

      const notion = price * size * 100;
      if (size >= BLOCK_MIN_QTY || notion >= BLOCK_MIN_NOTIONAL) {
        pushAndFanout({
          type: "blocks", provider: "tradier", ts,
          symbol: meta.ul, occ, option: out.option, side: out.side,
          qty: out.qty, price: out.price, notional: notion,
          oc_intent: out.oc_intent, intent_conf: out.intent_conf, fill_at: out.fill_at
        });
      }

      const existing = sweepBuckets.get(occ);
      if (!existing || (ts - existing.startTs > SWEEP_WINDOW_MS) || (existing.side !== out.side)) {
        sweepBuckets.set(occ, { side: out.side, startTs: ts, totalQty: size, notional: notion,
          prints: [{ ts, qty: size, price, venue: out.venue }] });
      } else {
        existing.totalQty += size;
        existing.notional += notion;
        existing.prints.push({ ts, qty: size, price, venue: out.venue });
      }
      const bucket = sweepBuckets.get(occ);
      if (bucket && (bucket.totalQty >= SWEEP_MIN_QTY || bucket.notional >= SWEEP_MIN_NOTIONAL)) {
        pushAndFanout({
          type: "sweeps", provider: "tradier", ts,
          symbol: meta.ul, occ, option: out.option, side: bucket.side,
          totalQty: bucket.totalQty, notional: bucket.notional, prints: bucket.prints,
          oc_intent: out.oc_intent, intent_conf: out.intent_conf, fill_at: out.fill_at
        });
        sweepBuckets.delete(occ);
      }
    }
    if (maxTs) {
      lastPollCursor.set(occ, new Date(maxTs + 1).toISOString());
      optLastPrintTs.set(occ, maxTs);
    }
  }
}
setInterval(pollTimesalesOnce, FALLBACK_POLL_EVERY_MS);

/* ================= CHAIN AUTO-EXPAND ================= */
async function getUlPrice(ul) {
  const url = `${TRADIER_REST}/v1/markets/quotes?symbols=${encodeURIComponent(ul)}`;
  const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${TRADIER_TOKEN}` } });
  if (!r.ok) return null;
  const j = await r.json();
  const q = j?.quotes?.quote;
  if (!q) return null;
  const last = Number(q.last ?? q.close ?? q.price);
  return Number.isFinite(last) ? last : null;
}
async function getExpirations(ul) {
  const url = `${TRADIER_REST}/v1/markets/options/expirations?symbol=${encodeURIComponent(ul)}&includeAllRoots=true&strikes=false`;
  const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${TRADIER_TOKEN}` } });
  if (!r.ok) return [];
  const j = await r.json();
  return (j?.expirations?.date || []).map(String);
}
function within30Days(dISO) {
  const t = Date.parse(dISO);
  if (!Number.isFinite(t)) return false;
  const now = Date.now();
  return (t - now) <= MAX_EXPIRY_DAYS * 24 * 3600 * 1000 && (t > now);
}

async function expandAndWatchChain(ul) {
  if (!TRADIER_TOKEN) return;
  const last = await getUlPrice(ul);
  const exps = (await getExpirations(ul)).filter(within30Days);
  if (!exps.length) return;

  for (const exp of exps) {
    const url = `${TRADIER_REST}/v1/markets/options/chains?symbol=${ul}&expiration=${exp}`;
    const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${TRADIER_TOKEN}` } });
    if (!r.ok) continue;
    const j = await r.json();
    const list = j?.options?.option || [];
    if (!list.length) continue;

    // collect strikes + baseline OI
    const strikesSet = new Set();
    for (const c of list) {
      const k = toOcc(
        (c.underlying || c.underlying_symbol || ul).toString(),
        String(c.expiration_date || exp),
        (String(c.option_type || c.right || "").toUpperCase().startsWith("C") ? "CALL" : "PUT"),
        Number(c.strike)
      );
      const oi = Number(c.open_interest || c.oi || 0);
      if (Number.isFinite(oi)) setOptOI(k.replace(/^[A-Z]+/, ul.toUpperCase()), oi);
      const st = Number(c.strike); if (Number.isFinite(st)) strikesSet.add(st);
    }

    const strikes = Array.from(strikesSet).sort((a,b)=>a-b);
    const center = Number.isFinite(last) ? last : (strikes[Math.floor(strikes.length/2)] || 0);
    const best = strikes
      .map(s => ({ s, d: Math.abs(s - center) }))
      .sort((a,b)=>a.d - b.d)
      .slice(0, Math.min(MAX_STRIKES_AROUND_ATM, strikes.length))
      .map(x => x.s)
      .sort((a,b)=>a-b);

    for (const k of best) {
      for (const right of ["CALL","PUT"]) {
        const entry = JSON.stringify({ underlying: ul.toUpperCase(), expiration: exp, strike: k, right });
        tradierOptWatch.set.add(entry);
      }
    }

    pushAndFanout({
      type: "chains", provider: "tradier", ts: Date.now(),
      symbol: ul.toUpperCase(), expiration: exp, strikes: best, strikesCount: best.length
    });
  }

  ensureTradierWS(true);
}

/* ================= WS (client) BOOTSTRAP ================= */
wss.on("connection", (sock) => {
  sock.on("message", (buf) => {
    try {
      const m = JSON.parse(String(buf));
      if (Array.isArray(m.subscribe)) {
        for (const t of m.subscribe) {
          (buffers[t] || []).slice(0, 50).forEach(it => sock.send(JSON.stringify(it)));
        }
      }
    } catch {}
  });
});

/* ================= WATCH ENDPOINTS ================= */
app.post("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, added: 0, watching: { equities: Array.from(alpacaSubs) } });

  let added = 0;
  for (const s of symbols) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }

  if (!MOCK) {
    await ensureAlpacaWS();
    if (TRADIER_TOKEN) { for (const ul of symbols) await expandAndWatchChain(ul); }
    ensureTradierWS(true);
    resubAlpaca();
  }

  res.json({ ok: true, added, watching: { equities: Array.from(alpacaSubs) } });
});

app.delete("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, removed: 0, optsRemoved: 0, watching: { equities: Array.from(alpacaSubs) } });

  let removed = 0;
  for (const s of symbols) if (alpacaSubs.delete(s)) removed++;
  const optsRemoved = pruneOptionsForUnderlyings(symbols);

  if (!MOCK) { resubAlpaca(); ensureTradierWS(true); }

  res.json({ ok: true, removed, optsRemoved, watching: {
    equities: Array.from(alpacaSubs),
    options: Array.from(tradierOptWatch.set).map(JSON.parse)
  } });
});

app.delete("/watch/tradier", (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  let removed = 0;
  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase()
    });
    if (tradierOptWatch.set.delete(entry)) removed++;
  }
  if (!MOCK) ensureTradierWS(true);
  res.json({ ok: true, removed, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

app.delete("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let removed = 0;
  for (const s of equities) if (alpacaSubs.delete(s)) removed++;
  if (!MOCK) { resubAlpaca(); ensureTradierWS(true); }
  res.json({ ok: true, removed, watching: { equities: Array.from(alpacaSubs) } });
});

app.get("/watchlist", (_req, res) => {
  res.json({ equities: Array.from(alpacaSubs), options: Array.from(tradierOptWatch.set).map(JSON.parse) });
});

app.post("/unwatch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, removed: 0, optsRemoved: 0, watching: { equities: Array.from(alpacaSubs) } });

  let removed = 0;
  for (const s of symbols) if (alpacaSubs.delete(s)) removed++;
  const optsRemoved = pruneOptionsForUnderlyings(symbols);
  if (!MOCK) { resubAlpaca(); ensureTradierWS(true); }

  res.json({ ok: true, removed, optsRemoved, watching: {
    equities: Array.from(alpacaSubs),
    options: Array.from(tradierOptWatch.set).map(JSON.parse),
  } });
});

app.post("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let added = 0;
  for (const s of equities) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }
  if (!MOCK) { ensureAlpacaWS(); ensureTradierWS(true); }
  res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) }, added });
});

app.post("/watch/tradier", async (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  let added = 0;

  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase()
    });
    if (!tradierOptWatch.set.has(entry)) { tradierOptWatch.set.add(entry); added++; }
  }

  const uls = new Set();
  for (const o of options) if (o?.underlying) uls.add(String(o.underlying).toUpperCase());
  if (uls.size === 0 && alpacaSubs.size) for (const s of alpacaSubs) uls.add(s);

  if (!MOCK && TRADIER_TOKEN) {
    for (const ul of uls) await expandAndWatchChain(ul);
  }

  if (!MOCK) ensureTradierWS(true);
  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) }, added });
});
/**
 * POST /analytics/oi-snapshot
 * Body: { date: "YYYY-MM-DD", rows: [{ occ, oi } | { underlying, expiration, right, strike, oi }, ...] }
 * Saves EOD OI and immediately runs confirmation for that date where possible.
 */
app.post("/analytics/oi-snapshot", (req, res) => {
  try {
    const dateStr = String(req.body?.date || "").slice(0,10);
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date must be YYYY-MM-DD" });
    }
    const recorded = recordEodRows(dateStr, rows);

    // try confirming each distinct OCC we just recorded
    let confirmedCount = 0;
    const occs = new Set();
    for (const r of rows) {
      const occ = r.occ
        || (r.underlying && r.expiration && r.right && Number.isFinite(Number(r.strike))
            ? toOcc(String(r.underlying).toUpperCase(), String(r.expiration), String(r.right).toUpperCase(), Number(r.strike))
            : null);
      if (occ) occs.add(occ);
    }
    for (const occ of occs) confirmedCount += confirmOccForDate(occ, dateStr);

    res.json({ ok: true, recorded, confirmed: confirmedCount });
  } catch (e) {
    console.error("/analytics/oi-snapshot error", e);
    res.status(500).json({ error: "internal error" });
  }
});

/**
 * POST /analytics/confirm
 * Body: { date: "YYYY-MM-DD", occs?: [ "AAPL251122C00215000", ... ] }
 * If occs omitted, we confirm every OCC that has snapshots for that date and previous date.
 */
app.post("/analytics/confirm", (req, res) => {
  try {
    const dateStr = String(req.body?.date || "").slice(0,10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
      return res.status(400).json({ error: "date must be YYYY-MM-DD" });
    }
    const provided = Array.isArray(req.body?.occs) ? new Set(req.body.occs.map(String)) : null;

    // build candidate set
    const occs = new Set();
    for (const key of eodOiByDateOcc.keys()) {
      const [d, occ] = key.split("|");
      if (d === dateStr && (!provided || provided.has(occ))) occs.add(occ);
    }

    let confirmedCount = 0;
    for (const occ of occs) confirmedCount += confirmOccForDate(occ, dateStr);

    res.json({ ok: true, date: dateStr, occs: Array.from(occs), confirmed: confirmedCount });
  } catch (e) {
    console.error("/analytics/confirm error", e);
    res.status(500).json({ error: "internal error" });
  }
});

/**
 * GET /debug/oi?occ=OCC&date=YYYY-MM-DD
 * Quick peek at stored OI for (date-1, date) and computed delta.
 */
app.get("/debug/oi", (req, res) => {
  const occ = String(req.query?.occ || "");
  const dateStr = String(req.query?.date || "");
  if (!occ || !/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    return res.status(400).json({ error: "need occ and date=YYYY-MM-DD" });
  }
  const prevDate = new Date(dateStr);
  prevDate.setUTCDate(prevDate.getUTCDate() - 1);
  const prev = prevDate.toISOString().slice(0,10);
  const oiPrev = eodOiByDateOcc.get(`${prev}|${occ}`);
  const oiCurr = eodOiByDateOcc.get(`${dateStr}|${occ}`);
  const delta = (Number.isFinite(oiPrev) && Number.isFinite(oiCurr)) ? (oiCurr - oiPrev) : null;
  res.json({ occ, prev, date: dateStr, oiPrev, oiCurr, delta });
});

/* ================= DEBUG / BACKFILL ================= */
app.get("/debug/state", (_req, res) => {
  const occs = Array.from(tradierOptWatch.set).map(JSON.parse)
    .map(o => toOcc(o.underlying, o.expiration, o.right, o.strike));
  res.json({
    mock: MOCK,
    alpacaSubs: Array.from(alpacaSubs),
    watchedOptions: Array.from(tradierOptWatch.set).map(JSON.parse),
    tradierSymbols: Array.from(new Set([...alpacaSubs, ...occs])),
    tradierSessionId: tradier.sessionId,
    tradierWsReady: !!tradier.ws && tradier.ws.readyState === 1
  });
});

app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
app.get("/health",               (_, res) => res.json({ ok: true }));
app.get("/debug/metrics",        (_req, res) => res.json({ mock: MOCK, count: httpMetrics.length, last5: httpMetrics.slice(-5) }));
app.use((req, res) => { res.status(404).json({ error: `Not found: ${req.method} ${req.originalUrl}` }); });
app.use((err, _req, res, _next) => { console.error("Server error:", err); res.status(err.status || 500).json({ error: err.message || "Internal Server Error" }); });

/* ================= START ================= */
server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`));
