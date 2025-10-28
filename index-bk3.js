import "./instrument-http.js";        // must be first
import { httpMetrics } from "./instrument-http.js";
import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import WebSocket, { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";

/* ----------------- CONFIG ----------------- */
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";                 // only on for "1"
const PORT = process.env.PORT || 8080;

const TRADIER_REST = process.env.TRADIER_BASE || "https://api.tradier.com";
const TRADIER_TOKEN = process.env.TRADIER_TOKEN;     // <- set me
const ALPACA_KEY    = process.env.ALPACA_KEY;        // <- set me
const ALPACA_SECRET = process.env.ALPACA_SECRET;     // <- set me

/* ----------------- APP/WS ----------------- */
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

/* ----------------- RING BUFFERS ----------------- */
const MAX_BUFFER = 400;
const buffers = {
  options_ts: [],
  equity_ts: [],
  sweeps: [],
  blocks: [],
  chains: []
};
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));

/* ----------------- STATE ----------------- */
const eqQuote = new Map();   // symbol -> { bid, ask, ts }
const optState = new Map();  // UL|YYYY-MM-DD|strike|right -> { oi_before, vol_today_before, last_reset }
const dayKey = () => new Date().toISOString().slice(0,10);

// watchers
const alpacaSubs = new Set(); // equities (AAPL, NVDA...)
const tradierOptWatch = { set: new Set() }; // JSON of { underlying, expiration, strike, right }

/* ----------------- HELPERS ----------------- */
function getOptState(key) {
  const today = dayKey();
  const s = optState.get(key) ?? { oi_before: 0, vol_today_before: 0, last_reset: today };
  if (s.last_reset !== today) { s.vol_today_before = 0; s.last_reset = today; }
  return s;
}
const setOptOI  = (k, oi) => { const s = getOptState(k); s.oi_before = Number(oi)||0; optState.set(k, s); };
const setOptVol = (k, v)  => { const s = getOptState(k); s.vol_today_before = Math.max(0, Number(v)||0); optState.set(k, s); };
const bumpOptVol = (k, qty) => { const s = getOptState(k); s.vol_today_before += Number(qty)||0; optState.set(k, s); };

function classifyOptionAction(side, qty, oi_before, vol_today_before) {
  if (qty > (oi_before + vol_today_before)) return side === "BUY" ? "BTO" : "STO";
  if (qty <= oi_before)                   return side === "BUY" ? "BTC" : "STC";
  return "UNK";
}
function classifyEquitySide(price, bid, ask) {
  if (Number.isFinite(ask) && price >= ask) return "BUY";
  if (Number.isFinite(bid) && price <= bid) return "SELL";
  return "MID";
}

function stableId(msg) {
  const key = JSON.stringify({
    type: msg.type,
    provider: msg.provider,
    symbol: msg.symbol ?? msg.underlying,
    occ: msg.occ,
    side: msg.side,
    price: msg.price,
    size: msg.size,
    ts: msg.ts ?? msg.time ?? msg.at,
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

function normOptionsPrint(provider, { underlying, option, price, size, side, venue, oi, volToday }) {
  const now = Date.now();
  const key = `${underlying}|${option.expiration}|${option.strike}|${option.right}`;
  if (Number.isFinite(oi)) setOptOI(key, Number(oi));
  if (Number.isFinite(volToday)) setOptVol(key, Number(volToday));
  const s = getOptState(key);
  const action = classifyOptionAction(side, size, s.oi_before, s.vol_today_before);
  bumpOptVol(key, size);
  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "options_ts",
    provider,
    symbol: underlying,
    option,
    side,        // BUY | SELL
    qty: size,
    price,
    action,      // BTO | STO | BTC | STC | UNK
    oi_before: s.oi_before,
    vol_before: s.vol_today_before,
    venue: venue ?? null
  };
}
function normEquityPrint(provider, { symbol, price, size }) {
  const now = Date.now();
  const q = eqQuote.get(symbol);
  const side = q ? classifyEquitySide(price, q.bid, q.ask) : "MID";
  return {
    id: `${now}-${Math.random().toString(36).slice(2,8)}`,
    ts: now,
    type: "equity_ts",
    provider,
    symbol,
    side,          // BUY | SELL | MID
    qty: size,
    price,
    action: "UNK",
    venue: null
  };
}
function toOcc(ul, expISO, right, strike) {
  const yymmdd = expISO.replaceAll("-", "").slice(2);  // 2025-12-19 -> 251219
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${ul.toUpperCase()}${yymmdd}${cp}${k}`;
}

/* ----------------- ALPACA (equities) ----------------- */
let alpacaWS = null;
async function ensureAlpacaWS() {
  if (MOCK) return;
  if (alpacaWS && alpacaWS.readyState === 1) return;
  if (!ALPACA_KEY || !ALPACA_SECRET) { console.warn("Alpaca keys missing"); return; }

  const url = "wss://stream.data.alpaca.markets/v2/iex";
  alpacaWS = new WebSocket(url);

  alpacaWS.onopen = () => {
    alpacaWS.send(JSON.stringify({ action: "auth", key: ALPACA_KEY, secret: ALPACA_SECRET }));
    if (alpacaSubs.size) {
      alpacaWS.send(JSON.stringify({ action: "subscribe",
        trades: Array.from(alpacaSubs),
        quotes: Array.from(alpacaSubs) }));
    }
  };
  alpacaWS.onmessage = (e) => {
    try {
      const arr = JSON.parse(e.data);
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

/* ----------------- TRADIER (equities + options) ----------------- */
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
  // equities
  const eq = Array.from(alpacaSubs);

  // options (to OCC)
  const occ = Array.from(tradierOptWatch.set).map(JSON.parse).map(o =>
    toOcc(o.underlying, o.expiration, o.right, o.strike)
  );

  return Array.from(new Set([...eq, ...occ]));
}

async function openTradierWsWith(symbols) {
  if (!TRADIER_TOKEN) { console.warn("[tradier] missing token"); return; }

  // 1) Create session (REST)
  const r = await fetch(`${TRADIER_REST}/v1/markets/events/session`, {
    method: "POST",
    headers: { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" }
  });
  const j = await r.json();
  const sid = j?.stream?.sessionid ?? j?.sessionid ?? null;
  console.log("[tradier] session create ->", j);
  if (!sid) { console.warn("[tradier] session id null"); return; }
  tradier.sessionId = sid;

  // 2) Open WS with query params
  const url = buildTradierWsUrl(sid, symbols);
  console.log("[tradier] WS open:", url);
  tradier.ws = new WebSocket(url);

  tradier.ws.onopen = () => {
    const payload = {
      sessionid: sid,
      symbols,
      filter: ["quote","trade","timesale"],
      linebreak: true,
      validOnly: true
    };
    console.log("[tradier] subscribe payload ->", payload);
    try { tradier.ws.send(JSON.stringify(payload)); } catch {}
  };

  tradier.ws.onmessage = (ev) => {
    const text = String(ev.data || "");
    const lines = text.split(/\r?\n/).filter(Boolean);
    for (const ln of lines) {
      let msg; try { msg = JSON.parse(ln); } catch { continue; }
      const t = (msg.type || "").toLowerCase();

      // quotes for equities
      if (t === "quote" && msg.symbol && !/^[A-Z]+(\d{6})([CP])(\d{8})$/.test(msg.symbol)) {
        const bid = Number(msg.bid ?? msg.b ?? msg.bp);
        const ask = Number(msg.ask ?? msg.a ?? msg.ap);
        if (Number.isFinite(bid) || Number.isFinite(ask)) {
          eqQuote.set(msg.symbol, { bid, ask, ts: Date.now() });
        }
        continue;
      }

      // trades/timesales
      if ((t === "trade" || t === "timesale") && msg.symbol) {
        const sym = msg.symbol;

        // OCC? -> option print
        const m = /^([A-Z]+)(\d{6})([CP])(\d{8})$/.exec(sym);
        if (m) {
          const [ , ul, yymmdd, cp, k ] = m;
          const exp = `20${yymmdd.slice(0,2)}-${yymmdd.slice(2,4)}-${yymmdd.slice(4,6)}`;
          const strike = Number(k)/1000;
          const right = cp === "C" ? "CALL" : "PUT";

          const price = Number(msg.price ?? msg.last ?? msg.p);
          const size  = Number(msg.size  ?? msg.volume ?? msg.s);
          if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;

          const side  = String(msg.side || msg.taker_side || "").toUpperCase().includes("SELL") ? "SELL" : "BUY";
          pushAndFanout(normOptionsPrint("tradier", {
            underlying: ul,
            option: { expiration: exp, strike, right },
            price, size, side, venue: msg.exchange || msg.exch
          }));
          continue;
        }

        // else equity print
        const price = Number(msg.price ?? msg.last ?? msg.p);
        const size  = Number(msg.size  ?? msg.volume ?? msg.s);
        if (!Number.isFinite(price) || !Number.isFinite(size) || size <= 0) continue;
        pushAndFanout(normEquityPrint("tradier", { symbol: sym, price, size }));
      }
    }
  };

  tradier.ws.onclose = () => {
    console.warn("[tradier] WS close -> reconnect");
    tradier.ws = null;
    tradier.sessionId = null;
    setTimeout(ensureTradierWS, 800);
  };
  tradier.ws.onerror = (e) => console.warn("[tradier] WS error", e?.message || e);
}

async function ensureTradierWS(forceResub = false) {
  if (MOCK) return;
  const symbols = buildSymbolList();
  if (!symbols.length) return;

  // (A) if not open, open
  if (!tradier.ws || tradier.ws.readyState !== 1) {
    if (tradier.connecting) return;
    tradier.connecting = true;
    try { await openTradierWsWith(symbols); }
    finally { tradier.connecting = false; }
    return;
  }

  // (B) already open -> re-subscribe with the *new* list
  if (forceResub) {
    const payload = {
      sessionid: tradier.sessionId,
      symbols,
      filter: ["quote","trade","timesale"],
      linebreak: true,
      validOnly: true
    };
    console.log("[tradier] resubscribe payload ->", payload);
    try { tradier.ws.send(JSON.stringify(payload)); } catch (e) {
      console.warn("[tradier] resubscribe send failed, reopening", e?.message || e);
      try { tradier.ws.close(); } catch {}
      setTimeout(() => ensureTradierWS(false), 250);
    }
  }
}

/* ----------------- WS BOOTSTRAP ----------------- */
wss.on("connection", (sock) => {
  sock.on("message", (buf) => {
    try {
      const m = JSON.parse(String(buf));
      if (Array.isArray(m.subscribe)) {
        for (const t of m.subscribe) {
          (buffers[t] ?? []).slice(0, 50).forEach(it => sock.send(JSON.stringify(it)));
        }
      }
    } catch {}
  });
});

/* ----------------- WATCH ENDPOINTS ----------------- */
app.post("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let added = 0;
  for (const s of equities) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }
  if (MOCK) { /* no-op */ } else { ensureAlpacaWS(); ensureTradierWS(true); }
  res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) }, added });
});

app.post("/watch/tradier", (req, res) => {
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
  if (!MOCK) ensureTradierWS(true);
  res.json({
    ok: true,
    watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) },
    added
  });
});

/* ----------------- DEBUG ----------------- */
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

/* ----------------- BACKFILL REST ----------------- */
app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));

app.get("/health", (_, res) => res.json({ ok: true }));
app.get("/debug/metrics", (_req, res) => res.json({ mock: MOCK, count: httpMetrics.length, last5: httpMetrics.slice(-5) }));

/* ----------------- START ----------------- */
server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`));
