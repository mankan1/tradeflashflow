// server/index.js
import "./instrument-http.js";        // must be first
import { httpMetrics } from "./instrument-http.js";
import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import WebSocket, { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";

// ---- config ----
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";        // only ON when explicitly "1"

const TRADIER_BASE = process.env.TRADIER_BASE || "https://api.tradier.com";
const PORT = process.env.PORT || 8080;

const TRADIER_REST = process.env.TRADIER_BASE || "https://api.tradier.com"; // REST host
const TRADIER_WS   = process.env.TRADIER_WS   || "wss://ws.tradier.com";     // WS host
//const TRADIER_TOKEN = process.env.TRADIER_TOKEN || "DZi4KKhQVv05kjgqXtvJRyiFbEhn";

// ---- express + ws ----
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ---- ring buffers for the 5 streams ----
const MAX_BUFFER = 400;
const buffers = {
  options_ts: [],
  equity_ts: [],
  sweeps: [],
  blocks: [],
  chains: []
};

// ---- quotes cache ----
const eqQuote  = new Map(); // equities: symbol -> { bid, ask, ts }
const optQuote = new Map(); // options:  OCC    -> { bid, ask, ts }

// ---- options OI/vol state to classify BTO/BTC/STO/STC ----
const optState = new Map(); // key: UL|YYYY-MM-DD|strike|right -> { oi_before, vol_today_before, last_reset }
const dayKey = () => new Date().toISOString().slice(0, 10);

const ALPACA_KEY = process.env.ALPACA_KEY || "AKNND2CVUEIRFCDNVMXL2NYVWD";
const ALPACA_SECRET = process.env.ALPACA_SECRET || "5xBdG2Go1PtWE36wnCrB4vES6mGF6tkusqDL7uSnnCxy";
const TRADIER_TOKEN = process.env.TRADIER_TOKEN || "DZi4KKhQVv05kjgqXtvJRyiFbEhn";

// ---- de-dupe ----
// const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));
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
// function normalizeForFanout(msg) {
//   const now = Date.now();
//   const withTs = { ts: typeof msg.ts === "number" ? msg.ts : now, ...msg };
//   const id = msg.id ?? stableId(withTs) ?? randomUUID();
//   return { id, ...withTs };
// }
// function broadcast(obj) {
//   const s = JSON.stringify(obj);
//   for (const c of wss.clients) if (c.readyState === 1) c.send(s);
// }
// function pushAndFanout(msg) {
//   const m = normalizeForFanout(msg);
//   const arr = buffers[m.type];
//   if (!arr) return;

//   const seen = seenIds[m.type] || (seenIds[m.type] = new Set());
//   if (seen.has(m.id)) return;

//   arr.unshift(m);
//   seen.add(m.id);

//   while (arr.length > MAX_BUFFER) {
//     const dropped = arr.pop();
//     if (dropped?.id) seen.delete(dropped.id);
//   }
//   broadcast(m);
// }

// put near top:
let _idCounter = 0;
function uniqueId() {
  // monotonic + uuid to be safe even across processes
  return `${Date.now()}-${(_idCounter++ & 0xffff).toString(36)}-${crypto.randomUUID()}`;
}

// after your `buffers` and `MAX_BUFFER`
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));

function stableHashKey(msg) {
  const key = JSON.stringify({
    type: msg.type,
    provider: msg.provider,
    symbol: msg.symbol ?? msg.underlying,
    occ: msg.occ,
    side: msg.side,
    price: msg.price,
    size: msg.qty ?? msg.size,
    ts: msg.ts ?? msg.time ?? msg.at,
  });
  return createHash("sha1").update(key).digest("hex");
}

function normalizeForFanout(msg) {
  const withTs = { ts: typeof msg.ts === "number" ? msg.ts : Date.now(), ...msg };
  // always give a fresh unique id so React keys never collide,
  // but keep a hash that lets us de-dupe identical payloads arriving twice
  const id = uniqueId();
  const h  = stableHashKey(withTs);
  return { id, _hash: h, ...withTs };
}

// ---- fanout (de-dupe by _hash, not id) ----
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}

function pushAndFanout(msg) {
  const m = normalizeForFanout(msg);
  const arr = buffers[m.type];
  if (!arr) return;

  // de-dupe on the stable hash
  const seen = seenIds[m.type] || (seenIds[m.type] = new Set());
  if (seen.has(m._hash)) return;

  arr.unshift(m);
  seen.add(m._hash);

  while (arr.length > MAX_BUFFER) {
    const dropped = arr.pop();
    if (dropped?._hash) seen.delete(dropped._hash);
  }

  broadcast(m);
}

// ---- helpers ----
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
// function classifyEquitySide(price, bid, ask) {
//   if (Number.isFinite(ask) && price >= ask) return "BUY";
//   if (Number.isFinite(bid) && price <= bid) return "SELL";
//   return "MID";
// }

// function normOptionsPrint(provider, { occ, underlying, option, price, size, side, venue, oi, volToday, bid, ask, ts }) {
//   const key = `${underlying}|${option.expiration}|${option.strike}|${option.right}`;
//   if (Number.isFinite(oi)) setOptOI(key, Number(oi));
//   if (Number.isFinite(volToday)) setOptVol(key, Number(volToday));
//   const s = getOptState(key);
//   const action = classifyOptionAction(side, size, s.oi_before, s.vol_today_before);
//   bumpOptVol(key, size);
//   return {
//     id: `${ts ?? Date.now()}-${Math.random().toString(36).slice(2,8)}`,
//     ts: ts ?? Date.now(),
//     type: "options_ts",
//     provider,
//     symbol: underlying,
//     occ,
//     option,
//     side,        // BUY | SELL | MID
//     qty: size,
//     price,
//     action,      // BTO | STO | BTC | STC | UNK
//     oi_before: s.oi_before,
//     vol_before: s.vol_today_before,
//     bid: bid ?? null,
//     ask: ask ?? null,
//     venue: venue ?? null
//   };
// }
// function normEquityPrint(provider, { symbol, price, size }) {
//   const now = Date.now();
//   const q = eqQuote.get(symbol);
//   const side = q ? classifyEquitySide(price, q.bid, q.ask) : "MID";
//   return {
//     id: `${now}-${Math.random().toString(36).slice(2,8)}`,
//     ts: now,
//     type: "equity_ts",
//     provider,
//     symbol,
//     side,          // BUY | SELL | MID
//     qty: size,
//     price,
//     action: "UNK",
//     venue: null
//   };
// }
function toOcc(ul, expISO, right, strike) {
  const exp = expISO.replaceAll("-", "").slice(2);
  const rt = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike)*1000)).padStart(8, "0");
  return `${ul.toUpperCase()}${exp}${rt}${k}`;
}

function fromOcc(occ) {
  const ul = occ.match(/^[A-Z]{1,6}/)[0];
  const yymmdd = occ.slice(ul.length, ul.length+6);
  const yy = "20" + yymmdd.slice(0,2), mm = yymmdd.slice(2,4), dd = yymmdd.slice(4,6);
  const right = occ.charAt(ul.length+6) === "C" ? "CALL" : "PUT";
  const k1000 = Number(occ.slice(ul.length+7));
  return { ul, exp: `${yy}-${mm}-${dd}`, right, strike: k1000/1000 };
}

function classifyEquitySide(price, bid, ask) {
  if (Number.isFinite(ask) && price >= ask) return "BUY";
  if (Number.isFinite(bid) && price <= bid) return "SELL";
  return "MID";
}

// ---------- Sweep / Block naive detectors ----------
const BLOCK_SHARE_MIN   = 50_000;   // equity shares threshold
const BLOCK_NOTIONAL_MIN= 2_000_000;// equity notional threshold ($)
const OPT_BLOCK_QTY_MIN = 250;      // contracts threshold (tune)
const SWEEP_WINDOW_MS   = 350;      // aggregate prints into a sweep
const sweepBuckets = new Map();     // key -> { t0, side, underlying, right, totalQty, totalPremium, legs: [...] }

function maybeEmitBlockEquity(trade) {
  // needs last quote for notional
  const q = eqQuote.get(trade.symbol);
  const notional = Number.isFinite(trade.price) ? trade.price * trade.qty : 0;
  if (trade.qty >= BLOCK_SHARE_MIN || notional >= BLOCK_NOTIONAL_MIN) {
    pushAndFanout({
      type: "blocks",
      provider: trade.provider,
      ts: trade.ts,
      symbol: trade.symbol,
      side: trade.side,
      shares: trade.qty,
      price: trade.price,
      notional
    });
  }
}

function maybeEmitBlockOption(op) {
  if (op.qty >= OPT_BLOCK_QTY_MIN) {
    pushAndFanout({
      type: "blocks",
      provider: op.provider,
      ts: op.ts,
      underlying: op.symbol,
      occ: toOcc(op.symbol, op.option.expiration, op.option.right, op.option.strike),
      side: op.side,
      contracts: op.qty,
      price: op.price,
      premium: op.qty * op.price * 100
    });
  }
}

function addToSweep(optionPrint) {
  const leg = optionPrint;
  const k = `${leg.symbol}|${leg.option.right}|${leg.side}`;
  const now = leg.ts || Date.now();
  let b = sweepBuckets.get(k);
  if (!b || (now - b.t0) > SWEEP_WINDOW_MS) {
    // emit old first
    if (b && b.legs?.length) {
      pushAndFanout({
        type: "sweeps",
        provider: "tradier",
        ts: b.t0,
        underlying: b.underlying,
        right: b.right,
        side: b.side,
        totalQty: b.totalQty,
        totalPremium: b.totalPremium,
        legs: b.legs
      });
    }
    b = { t0: now, side: leg.side, underlying: leg.symbol, right: leg.option.right,
          totalQty: 0, totalPremium: 0, legs: [] };
    sweepBuckets.set(k, b);
  }
  b.totalQty += leg.qty;
  b.totalPremium += leg.qty * leg.price * 100;
  b.legs.push({
    exp: leg.option.expiration,
    strike: leg.option.strike,
    price: leg.price,
    qty: leg.qty,
    venue: leg.venue || null
  });

  // micro-timer to flush if no more legs come in time
  clearTimeout(b._t);
  b._t = setTimeout(() => {
    const cur = sweepBuckets.get(k);
    if (cur && cur.legs?.length) {
      pushAndFanout({
        type: "sweeps",
        provider: "tradier",
        ts: cur.t0,
        underlying: cur.underlying,
        right: cur.right,
        side: cur.side,
        totalQty: cur.totalQty,
        totalPremium: cur.totalPremium,
        legs: cur.legs
      });
    }
    sweepBuckets.delete(k);
  }, SWEEP_WINDOW_MS + 20);
}

// ---- normalize + handle stream events ----
function normOptionsPrint(provider, { underlying, option, price, size, side, venue, oi, volToday }) {
  const now = Date.now();
  const key = `${underlying}|${option.expiration}|${option.strike}|${option.right}`;
  if (Number.isFinite(oi)) setOptOI(key, Number(oi));
  if (Number.isFinite(volToday)) setOptVol(key, Number(volToday));
  const s = getOptState(key);
  const action = classifyOptionAction(side, size, s.oi_before, s.vol_today_before);
  bumpOptVol(key, size);
  return {
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

function handleTradierEvent(m) {
  // quotes (equities)
  if (m.type === "quote" && m.symbol && !occRegex().test(m.symbol)) {
    const bid = Number(m.bid);
    const ask = Number(m.ask);
    if (Number.isFinite(bid) || Number.isFinite(ask)) {
      eqQuote.set(m.symbol, { bid, ask, ts: Date.now() });
    }
    return;
  }

  // equity trades
  if (m.type === "trade" && m.symbol && !occRegex().test(m.symbol)) {
    const price = Number(m.price ?? m.last);
    const size  = Number(m.size  ?? m.volume ?? 0);
    if (!Number.isFinite(price) || size <= 0) return;
    const out = normEquityPrint("tradier", { symbol: m.symbol, price, size });
    out.ts = m.timestamp ? Date.parse(m.timestamp) : Date.now();
    pushAndFanout(out);
    maybeEmitBlockEquity(out);
    return;
  }

  // options prints (trade / timesale on OCC symbol)
  if ((m.type === "trade" || m.type === "timesale") && m.symbol && occRegex().test(m.symbol)) {
    const { ul, exp, right, strike } = fromOcc(m.symbol);
    const price = Number(m.price ?? m.last ?? 0);
    const size  = Number(m.size  ?? m.volume ?? 0);
    if (!Number.isFinite(price) || size <= 0) return;

    const rawSide = String(m.side || m.taker_side || "").toUpperCase();
    const side = rawSide.includes("SELL") || rawSide === "S" ? "SELL" : "BUY";

    const out = normOptionsPrint("tradier", {
      underlying: ul,
      option: { expiration: exp, strike, right },
      price, size, side, venue: m.exch || m.venue
    });
    out.ts = m.timestamp ? Date.parse(m.timestamp) : Date.now();
    pushAndFanout(out);
    maybeEmitBlockOption(out);
    addToSweep(out);
    return;
  }
}
// function fromOcc(occ) {
//   const ul = occ.match(/^[A-Z]{1,6}/)[0];
//   const yymmdd = occ.slice(ul.length, ul.length+6);
//   const yy = "20" + yymmdd.slice(0,2), mm = yymmdd.slice(2,4), dd = yymmdd.slice(4,6);
//   const right = occ.charAt(ul.length+6) === "C" ? "CALL" : "PUT";
//   const k1000 = Number(occ.slice(ul.length+7));
//   return { ul, exp: `${yy}-${mm}-${dd}`, right, strike: k1000/1000 };
// }
// OCC helpers
// function toOcc(ul, expISO, right, strike) {
//   const exp = expISO.replaceAll("-", "").slice(2); // YYMMDD
//   const rt = String(right).toUpperCase().startsWith("C") ? "C" : "P";
//   const k = String(Math.round(Number(strike)*1000)).padStart(8,"0");
//   return `${ul.toUpperCase()}${exp}${rt}${k}`;
// }
// ---- Tradier streaming (session + websocket) ----
// let tradierWS = null;
// let tradierSessionId = null;

// async function createTradierSession() {
//   const token = TRADIER_TOKEN;
//   const url = `${TRADIER_BASE}/v1/markets/events/session`;
//   const t0 = Date.now();
//   const r = await fetch(url, {
//     method: "POST",
//     headers: {
//       Authorization: `Bearer ${token}`,
//       Accept: "application/json",
//     },
//   });
//   console.log(`[HTTP OUT] POST ${url} -> ${r.status} ${Date.now() - t0}ms`);
//   if (!r.ok) throw new Error(`Tradier session create failed ${r.status}`);
//   const js = await r.json();
//   const sid = js?.stream?.sessionid; // << correct field
//   if (!sid) throw new Error("Tradier sessionid missing in response");
//   return sid;
// }
let tradierWS = null;
let tradierSessionId = null;
let tradierSessionPromise = null;

async function createTradierSession() {
  if (tradierSessionPromise) return tradierSessionPromise;
  tradierSessionPromise = (async () => {
    const url = `${TRADIER_BASE}/v1/markets/events/session`;
    const t0 = Date.now();
    const r = await fetch(url, {
      method: "POST",
      headers: { Authorization: `Bearer ${TRADIER_TOKEN}`, Accept: "application/json" },
    });
    console.log(`[HTTP OUT] POST ${url} -> ${r.status} ${Date.now()-t0}ms`);
    if (!r.ok) throw new Error(`Tradier session create failed ${r.status}`);
    const js = await r.json();
    const sid = js?.stream?.sessionid;
    if (!sid) throw new Error("Tradier sessionid missing in response");
    tradierSessionId = sid;
    return sid;
  })();
  return tradierSessionPromise;
}

async function ensureTradierWS() {
  if (MOCK) return;

  await createTradierSession().catch(e => {
    console.warn(String(e));
    return;
  });
  if (!tradierSessionId) return;

  // Reuse if already OPEN or CONNECTING; just schedule a refresh
  if (tradierWS && (tradierWS.readyState === 0 || tradierWS.readyState === 1)) {
    scheduleSubscriptionRefresh();
    return;
  }

  // (Re)open
  tradierWS = new WebSocket("wss://ws.tradier.com/v1/markets/events");

  // NOTE: use ws event emitter API
  tradierWS.on("open", () => {
    scheduleSubscriptionRefresh(); // will send payload when open
  });

  tradierWS.on("message", (buf) => {
    const text = String(buf || "");
    for (const line of text.split(/\r?\n/)) {
      if (!line.trim()) continue;
      let msg; try { msg = JSON.parse(line); } catch { continue; }
      handleTradierEvent(msg);
    }
  });

  tradierWS.on("close", () => {
    tradierWS = null;
    tradierSessionId = null;
    tradierSessionPromise = null;
    setTimeout(ensureTradierWS, 1500);
  });

  tradierWS.on("error", () => {/* swallow; reconnect on close */});
}
function occRegex() {
  // OCC like AAPL251219C00450000 (UL + YYMMDD + C/P + 8-digit strike*1000)
  return /^[A-Z]{1,6}\d{6}[CP]\d{8}$/;
}

// // Build the symbol list we want to stream: equities (alpacaSubs) + options we watch
// function currentTradierSymbols() {
//   const syms = new Set();
//   // equities you watch via /watch/alpaca
//   for (const s of alpacaSubs) syms.add(String(s).toUpperCase());
//   // option contracts added via /watch/tradier
//   for (const key of tradierOptWatch.set) {
//     const o = JSON.parse(key);
//     syms.add(toOcc(o.underlying, o.expiration, o.right, o.strike));
//   }
//   return Array.from(syms);
// }
function currentTradierSymbols() {
  const syms = new Set();
  for (const s of alpacaSubs) syms.add(String(s).toUpperCase());
  for (const key of tradierOptWatch.set) {
    const o = JSON.parse(key);
    syms.add(toOcc(o.underlying, o.expiration, o.right, o.strike));
  }
  return Array.from(syms);
}

function buildSubscribePayload() {
  return {
    sessionid: tradierSessionId,
    symbols: currentTradierSymbols(),
    filter: ["quote", "trade", "timesale"],
    linebreak: true,
    validOnly: true,
  };
}
function sendWhenOpen(ws, data) {
  const text = typeof data === "string" ? data : JSON.stringify(data);
  if (ws && ws.readyState === 1) { // OPEN
    try { ws.send(text); } catch {}
    return;
  }
  if (!ws) return; // nothing to do yet

  // CONNECTING -> wait for open once
  const onOpen = () => {
    try { ws.send(text); } finally { ws.off?.("open", onOpen); }
  };
  ws.on?.("open", onOpen);
}
let subDirty = false;
let subTimer = null;

function scheduleSubscriptionRefresh() {
  subDirty = true;
  if (subTimer) return;
  subTimer = setTimeout(() => {
    subTimer = null;
    if (!subDirty) return;
    subDirty = false;
    if (!tradierWS) return;
    const payload = buildSubscribePayload();
    // only send after we have a session id
    if (!tradierSessionId) return;
    sendWhenOpen(tradierWS, payload);
  }, 100); // small debounce to absorb bursts
}
// async function ensureTradierWS() {
//   if (MOCK) return; // mock handled elsewhere
//   try {
//     if (!tradierSessionId) tradierSessionId = await createTradierSession();
//   } catch (e) {
//     console.warn(String(e));
//     return;
//   }

//   if (tradierWS && tradierWS.readyState === 1) {
//     // Already connected: refresh subscription if symbols changed
//     const payload = {
//       sessionid: tradierSessionId,
//       symbols: currentTradierSymbols(),
//       filter: ["quote", "trade", "timesale"],
//       linebreak: true,
//       validOnly: true,
//     };
//     try { tradierWS.send(JSON.stringify(payload)); } catch {}
//     return;
//   }

//   const wsUrl = "wss://ws.tradier.com/v1/markets/events";
//   tradierWS = new WebSocket(wsUrl);

//   tradierWS.onopen = () => {
//     const payload = {
//       sessionid: tradierSessionId,
//       symbols: currentTradierSymbols(),
//       filter: ["quote", "trade", "timesale"],
//       linebreak: true,
//       validOnly: true,
//     };
//     console.log(`[tradier] WS open -> send ${JSON.stringify(payload)}`);
//     tradierWS.send(JSON.stringify(payload));
//   };

//   // messages are line-delimited JSON strings when linebreak=true
//   tradierWS.onmessage = (ev) => {
//     const text = String(ev.data || "");
//     for (const line of text.split(/\r?\n/)) {
//       if (!line.trim()) continue;
//       let msg;
//       try { msg = JSON.parse(line); } catch { continue; }
//       handleTradierEvent(msg);
//     }
//   };

//   tradierWS.onclose = () => {
//     tradierWS = null;
//     // session is only valid briefly; create a fresh one on reconnect
//     tradierSessionId = null;
//     setTimeout(ensureTradierWS, 1500);
//   };
//   tradierWS.onerror = () => {};
// }


// // Handle incoming stream events
// function handleTradierEvent(m) {
//   // Common fields: m.type in ['quote','trade','timesale'], m.symbol
//   // Equity quotes → keep bid/ask cache
//   if (m.type === "quote" && m.symbol && !occRegex().test(m.symbol)) {
//     const bid = Number(m.bid ?? m.bidsize ? m.bid : m.bid ?? NaN);
//     const ask = Number(m.ask ?? m.asksize ? m.ask : m.ask ?? NaN);
//     if (Number.isFinite(bid) || Number.isFinite(ask)) {
//       eqQuote.set(m.symbol, { bid, ask, ts: Date.now() });
//     }
//     return;
//   }

//   // Equity trades
//   if (m.type === "trade" && m.symbol && !occRegex().test(m.symbol)) {
//     const price = Number(m.price ?? m.last ?? m.p ?? NaN);
//     const size  = Number(m.size  ?? m.volume ?? m.s ?? 0);
//     if (!Number.isFinite(price) || size <= 0) return;
//     pushAndFanout(
//       normEquityPrint("tradier", { symbol: m.symbol, price, size })
//     );
//     return;
//   }

//   // Option prints (either trade or timesale for OCC symbol)
//   if ((m.type === "trade" || m.type === "timesale") && m.symbol && occRegex().test(m.symbol)) {
//     // Try to recover legs for classification
//     const { ul, exp, right, strike } = fromOcc(m.symbol);
//     const price = Number(m.price ?? m.last ?? m.p ?? NaN);
//     const size  = Number(m.size  ?? m.volume ?? m.s ?? 0);
//     if (!Number.isFinite(price) || size <= 0) return;

//     // Derive BUY/SELL: use taker side if present, else fall back to quote crossing if you wish.
//     const rawSide = String(m.side || m.taker_side || "").toUpperCase();
//     const side = rawSide.includes("SELL") || rawSide === "S" ? "SELL" : "BUY";

//     const out = normOptionsPrint("tradier", {
//       underlying: ul,
//       option: { expiration: exp, strike, right },
//       price, size, side,
//       venue: m.exch || m.venue,
//       // oi/volToday left undefined here; they’re maintained via chains refresh
//       oi: undefined, volToday: undefined,
//     });
//     // Prefer stream timestamp if present
//     if (m.timestamp) out.ts = Date.parse(m.timestamp);
//     pushAndFanout(out);
//     return;
//   }
// }

function parseOcc(occ) {
  // e.g. AAPL251219C00450000
  const m = occ.match(/^([A-Z]+)(\d{2})(\d{2})(\d{2})([CP])(\d{8})$/i);
  if (!m) return null;
  const [, ul, yy, mm, dd, rt, k8] = m;
  const exp = `20${yy}-${mm}-${dd}`;
  const strike = Number(k8)/1000;
  return { underlying: ul.toUpperCase(), expiration: exp, right: rt.toUpperCase(), strike };
}
function occToKey(occ) {
  const p = parseOcc(occ);
  if (!p) return null;
  return `${p.underlying}|${p.expiration}|${p.strike}|${p.right}`;
}

// ---- watchers ----
let alpacaWS = null;
const alpacaSubs = new Set(); // equities
const tradierOptWatch = { set: new Set() }; // watched options (JSON of {underlying, expiration, strike, right})

// ---- Sweeps/Blocks ----
// const SWEEP_WINDOW_MS = 350;
const SWEEP_MIN_NOTIONAL = 75_000;
const BLOCK_MIN_CONTRACTS = 250;
const BLOCK_MIN_NOTIONAL  = 200_000;

// const sweepBuckets = new Map(); // underlying -> { side, start, prints[] }
function dominantOC(prints) {
  const cnt = new Map();
  for (const p of prints) if (p.action) cnt.set(p.action, (cnt.get(p.action)||0)+1);
  let best=null, max=-1;
  for (const [k,v] of cnt) if (v>max) {max=v; best=k;}
  return best || null;
}
function flushSweep(key) {
  const b = sweepBuckets.get(key);
  if (!b || b.prints.length === 0) return;
  const totalContracts = b.prints.reduce((s,x)=>s+x.qty,0);
  const totalNotional  = b.prints.reduce((s,x)=>s+x.price*x.qty*100,0);
  if (totalNotional >= SWEEP_MIN_NOTIONAL && totalContracts > 1) {
    pushAndFanout({
      type: "sweeps",
      id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
      provider: "tradier",
      underlying: key,
      side: b.side,
      oc: dominantOC(b.prints),
      legs: b.prints.length,
      totalContracts,
      totalNotional,
      firstTs: b.start,
      lastTs: b.prints[b.prints.length-1].ts,
      sample: b.prints.slice(-3)
    });
  }
  sweepBuckets.delete(key);
}
function considerSweep(p) {
  if (p.type !== "options_ts" || !p.side) return;
  const key = p.symbol; // underlying
  const now = p.ts;
  const b = sweepBuckets.get(key);
  if (!b || (now - b.start) > SWEEP_WINDOW_MS || (b.side && p.side !== b.side)) {
    if (b) flushSweep(key);
    sweepBuckets.set(key, { side: p.side, start: now, prints: [p] });
    return;
  }
  b.prints.push(p);
  if ((now - b.start) > SWEEP_WINDOW_MS) flushSweep(key);
}
function considerBlock(p) {
  if (p.type !== "options_ts") return;
  const notional = p.price * p.qty * 100;
  if (p.qty >= BLOCK_MIN_CONTRACTS || notional >= BLOCK_MIN_NOTIONAL) {
    pushAndFanout({
      type: "blocks",
      id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
      provider: p.provider,
      symbol: p.occ,
      underlying: p.symbol,
      side: p.side,
      oc: p.action,
      size: p.qty,
      price: p.price,
      notional,
      ts: p.ts
    });
  }
}

// ---- Alpaca equities WS ----
async function ensureAlpacaWS() {
  if (MOCK) { console.warn("Using mock."); return; }
  if (alpacaWS && alpacaWS.readyState === 1) return;

  if (!ALPACA_KEY || !ALPACA_SECRET) { console.warn("Alpaca keys missing; equities will be empty."); return; }
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
const tradierStream = { sessionId: null, ws: null };

async function ensureTradierStream() {
  if (MOCK) { console.warn("[tradier] MOCK on"); return; }
  if (!TRADIER_TOKEN) { console.warn("[tradier] missing TRADIER_TOKEN"); return; }

  // (A) Create/refresh session on the REST host
  if (!tradierStream.sessionId) {
    const sessUrl = `${TRADIER_REST}/v1/markets/events/session`;
    const r = await fetch(sessUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${TRADIER_TOKEN}`,
        Accept: "application/json"
      }
    });
    if (!r.ok) {
      console.warn("[tradier] session create failed", r.status);
      return;
    }
    const { session_id } = await r.json();
    tradierStream.sessionId = session_id;
    console.log("[tradier] session_id", session_id);
  }

  // (B) Build the symbol list (equities + OCC options)
  const occs = Array.from(tradierOptWatch.set).map(s => {
    const o = JSON.parse(s);
    return toOcc(o.underlying, o.expiration, o.right, o.strike);
  });
  const equities = Array.from(alpacaSubs);
  const symbolsList = [...new Set([...equities, ...occs])];
  if (!symbolsList.length) return;

  // Already connected?
  if (tradierStream.ws && tradierStream.ws.readyState === 1) return;

  // (C) Connect the WebSocket on the WS host
  const qs = new URLSearchParams({
    sessionid: tradierStream.sessionId,
    symbols: symbolsList.join(","),
    filter: "quote,trade,timesale",
    linebreak: "true",
    validOnly: "true"
  }).toString();

  const wsUrl = `${TRADIER_WS}/v1/markets/events?${qs}`;
  const ws = new WebSocket(wsUrl, {
    headers: { Authorization: `Bearer ${TRADIER_TOKEN}` }
  });
  tradierStream.ws = ws;

  ws.onopen = () => {
    console.log("[tradier] WS open:", wsUrl);
  };

  ws.onmessage = (ev) => {
    try {
      // Tradier streams line-delimited JSON. We requested linebreak=true so each ev.data is one JSON object.
      const m = JSON.parse(ev.data);
      const sym = String(m.symbol || "");
      // Heuristic: OCC option symbols end with [CP]\d{8} and include YYMMDD before that
      const isOption = /\d{6}[CP]\d{8}$/.test(sym);

      if (m.type === "quote") {
        const bid = Number(m.bid), ask = Number(m.ask);
        const ts  = Number(m.askdate || m.biddate) || Date.now();
        if (isOption) optQuote.set(sym, { bid, ask, ts });
        else eqQuote.set(sym, { bid, ask, ts });
        return;
      }

      if (m.type === "trade" || m.type === "timesale") {
        const last = Number(m.last ?? m.price);
        const size = Number(m.size ?? m.volume ?? 0);
        const ts   = Number(m.date || m.timestamp) || Date.now();
        if (!isFinite(last) || !size) return;

        if (!isOption) {
          const q = eqQuote.get(sym);
          const side = q ? classifyEquitySide(last, q.bid, q.ask) : "MID";
          pushAndFanout({
            type: "equity_ts",
            id: `${ts}-${Math.random().toString(36).slice(2,8)}`,
            provider: "tradier",
            symbol: sym,
            side,
            qty: size,
            price: last,
            ts
          });
          return;
        }

        // option trade
        const q = optQuote.get(sym) || {};
        const side = (Number.isFinite(q.ask) && last >= q.ask) ? "BUY"
                    : (Number.isFinite(q.bid) && last <= q.bid) ? "SELL"
                    : "MID";

        const occ = sym;
        const p = parseOcc(occ);
        if (!p) return;

        const out = normOptionsPrint("tradier", {
          occ,
          underlying: p.underlying,
          option: { expiration: p.expiration, strike: p.strike, right: p.right },
          price: last,
          size,
          side,
          bid: q.bid, ask: q.ask,
          ts
        });

        pushAndFanout(out);
        considerBlock(out);
        considerSweep(out);
        return;
      }
    } catch {
      // ignore bad line
    }
  };

  ws.onclose = () => {
    console.warn("[tradier] WS closed — retrying");
    tradierStream.ws = null;
    tradierStream.sessionId = null; // get a fresh session next time
    setTimeout(ensureTradierStream, 1000);
  };
  ws.onerror = (e) => {
    console.warn("[tradier] WS error", e?.message || "");
  };
}
// ---- Tradier streaming (options + equities events) ----
/**const tradierStream = { sessionId: null, ws: null };

async function ensureTradierStream() {
  if (MOCK) { console.warn("Using mock."); return; }
  if (!TRADIER_TOKEN) { console.warn("TRADIER_TOKEN missing; options will be empty."); return; }

  // Create/refresh session id
  if (!tradierStream.sessionId) {
    const r = await fetch("https://stream.tradier.com/v1/markets/events/session", {
      method: "POST",
      headers: { Authorization: `Bearer ${TRADIER_TOKEN}` }
    });
    if (!r.ok) { console.warn("Tradier session create failed", r.status); return; }
    const { session_id } = await r.json();
    tradierStream.sessionId = session_id;
  }

  // Build symbol list
  const occs = Array.from(tradierOptWatch.set).map(s => {
    const o = JSON.parse(s);
    return toOcc(o.underlying, o.expiration, o.right, o.strike);
  });
  const equities = Array.from(alpacaSubs);
  const symbolsList = [...new Set([...equities, ...occs])];
  if (!symbolsList.length) return;

  // Already connected?
  if (tradierStream.ws && tradierStream.ws.readyState === 1) return;

  const qs = new URLSearchParams({
    sessionid: tradierStream.sessionId,
    symbols: symbolsList.join(","),
    filter: "quote,trade,timesale",
    linebreak: "true",
    validOnly: "true"
  }).toString();

  tradierStream.ws = new WebSocket(`wss://stream.tradier.com/v1/markets/events?${qs}`, {
    headers: { Authorization: `Bearer ${TRADIER_TOKEN}` }
  });

  tradierStream.ws.onmessage = (ev) => {
    try {
      const m = JSON.parse(ev.data);
      const sym = String(m.symbol || "");
      const isOption = /[CP]\d{8}$/.test(sym) && /\d{6}[CP]\d{8}$/.test(sym);

      if (m.type === "quote") {
        const bid = Number(m.bid), ask = Number(m.ask);
        const ts  = Number(m.askdate || m.biddate) || Date.now();
        if (isOption) optQuote.set(sym, { bid, ask, ts });
        else eqQuote.set(sym, { bid, ask, ts });
        return;
      }

      if (m.type === "trade" || m.type === "timesale") {
        const last = Number(m.last ?? m.price);
        const size = Number(m.size ?? m.volume ?? 0);
        const ts   = Number(m.date || m.timestamp) || Date.now();
        if (!isFinite(last) || !size) return;

        if (!isOption) {
          // equity
          const q = eqQuote.get(sym);
          const side = q ? classifyEquitySide(last, q.bid, q.ask) : "MID";
          pushAndFanout({
            type: "equity_ts",
            id: `${ts}-${Math.random().toString(36).slice(2,8)}`,
            provider: "tradier",
            symbol: sym,
            side,
            qty: size,
            price: last,
            ts
          });
          return;
        }

        // option
        const q = optQuote.get(sym) || {};
        const side = (Number.isFinite(q.ask) && last >= q.ask) ? "BUY"
                    : (Number.isFinite(q.bid) && last <= q.bid) ? "SELL"
                    : "MID";

        const occ = sym;
        const p = parseOcc(occ);
        if (!p) return;

        const out = normOptionsPrint("tradier", {
          occ,
          underlying: p.underlying,
          option: { expiration: p.expiration, strike: p.strike, right: p.right },
          price: last,
          size,
          side,
          bid: q.bid, ask: q.ask,
          ts
        });

        pushAndFanout(out);
        considerBlock(out);
        considerSweep(out);
        return;
      }
    } catch {
      // ignore bad line
    }
  };

  tradierStream.ws.onclose = () => {
    tradierStream.ws = null;
    tradierStream.sessionId = null;
    setTimeout(ensureTradierStream, 1000);
  };
  tradierStream.ws.onerror = () => {};
}*/

// ---- Chains refresh (REST JSON) ----
async function refreshChainsNow() {
  if (MOCK || !TRADIER_TOKEN) return;

  // group by UL -> set of expirations
  const byUL = {};
  for (const key of tradierOptWatch.set) {
    const o = JSON.parse(key);
    (byUL[o.underlying] ??= new Set()).add(o.expiration);
  }

  for (const [ul, exps] of Object.entries(byUL)) {
    for (const exp of exps) {
      const url = `${TRADIER_BASE}/v1/markets/options/chains?symbol=${ul}&expiration=${exp}`;
      const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${TRADIER_TOKEN}` } });
      if (!r.ok) continue;
      let j;
      try { j = await r.json(); } catch { continue; }
      const list = j?.options?.option || [];
      const arr = Array.isArray(list) ? list : [list];

      for (const it of arr) {
        // update state cache for later BTO/BTC/STO/STC calc
        const occ = String(it.symbol);
        const k = occToKey(occ);
        if (!k) continue;
        if (Number.isFinite(Number(it.open_interest))) setOptOI(k, Number(it.open_interest));
        if (Number.isFinite(Number(it.volume)))       setOptVol(k, Number(it.volume));
        // keep opt quote hints too
        const bid = Number(it.bid ?? 0), ask = Number(it.ask ?? 0);
        if (Number.isFinite(bid) || Number.isFinite(ask)) {
          optQuote.set(occ, { bid, ask, ts: Date.now() });
        }
      }

      // snapshot for chains screen (lightweight)
      pushAndFanout({
        type: "chains",
        id: `${Date.now()}-${Math.random().toString(36).slice(2,8)}`,
        provider: "tradier",
        underlying: ul,
        expiration: exp,
        count: arr.length,
        ts: Date.now()
      });
    }
  }
}
if (!MOCK) setInterval(() => { refreshChainsNow().catch(()=>{}); }, 15_000);

function startTradierStream() {
  if (MOCK) return startMock();
  ensureTradierWS();
}
// ---- Mock (optional) ----
function startMock() {
  if (startMock._on) return;
  startMock._on = true;
  setInterval(() => {
    for (const sym of alpacaSubs) {
      const q = eqQuote.get(sym) ?? { bid: 100, ask: 101, ts: Date.now() };
      const mid = (q.bid + q.ask)/2;
      const d = (Math.random()-0.5)*0.3;
      const bid = Math.max(0, +(mid + d - 0.05).toFixed(2));
      const ask = Math.max(bid+0.01, +(mid + d + 0.05).toFixed(2));
      eqQuote.set(sym, { bid, ask, ts: Date.now() });
      const price = +(Math.random()<0.5 ? bid : ask).toFixed(2);
      const size = Math.floor(Math.random()*500)+1;
      pushAndFanout(normEquityPrint("alpaca", { symbol: sym, price, size }));
    }
    for (const key of tradierOptWatch.set) {
      const o = JSON.parse(key);
      const size = Math.floor(Math.random()*50)+1;
      const price = +(Math.random()*2 + 0.05).toFixed(2);
      const side = Math.random()<0.5 ? "BUY" : "SELL";
      const occ = toOcc(o.underlying, o.expiration, o.right, o.strike);
      const k = `${o.underlying}|${o.expiration}|${o.strike}|${o.right}`;
      const s = getOptState(k);
      if (Math.random() < 0.3) setOptOI(k, Math.max(0, s.oi_before + Math.floor((Math.random()-0.5)*200)));
      const out = normOptionsPrint("tradier", {
        occ,
        underlying: o.underlying,
        option: { expiration: o.expiration, strike: o.strike, right: o.right },
        price, size, side, bid: null, ask: null, ts: Date.now()
      });
      pushAndFanout(out);
      considerBlock(out);
      considerSweep(out);
    }
  }, 800);
}

// ---- WS connection bootstrap (sends recent on subscribe) ----
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

// ---- /watch endpoints (client initiates) ----
// app.post("/watch/alpaca", (req, res) => {
//   const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
//   for (const s of equities) alpacaSubs.add(s);
//   if (MOCK) startMock(); else { ensureAlpacaWS(); startTradierStream(); }
//   res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) } });
// });

app.post("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  for (const s of equities) alpacaSubs.add(s);
  if (MOCK) startMock(); else ensureAlpacaWS();
  // If you also stream equities via Tradier, refresh that socket’s symbols, too:
  scheduleSubscriptionRefresh();
  res.json({ ok: true, watching: { equities: Array.from(alpacaSubs) } });
});

app.post("/watch/tradier", (req, res) => {
  const options = Array.isArray(req.body?.options) ? req.body.options : [];
  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase()
    });
    tradierOptWatch.set.add(entry);
  }
  if (MOCK) startMock(); else ensureTradierWS();//else startTradierStream();
  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

// app.post("/watch/tradier", (req, res) => {
//   const options = Array.isArray(req.body?.options) ? req.body.options : [];
//   for (const o of options) {
//     const entry = JSON.stringify({
//       underlying: String(o.underlying).toUpperCase(),
//       expiration: String(o.expiration),
//       strike: Number(o.strike),
//       right: String(o.right).toUpperCase()
//     });
//     tradierOptWatch.set.add(entry);
//   }
//   if (MOCK) startMock(); else { ensureTradierStream(); refreshChainsNow().catch(()=>{}); }
//   res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
// });

// ---- debug + REST views ----
app.get("/debug/metrics", (_req, res) => {
  res.json({
    mock: MOCK,
    count: httpMetrics.length,
    last5: httpMetrics.slice(-5),
  });
});

app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
app.get("/health",              (_, res) => res.json({ ok: true }));

server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`));

