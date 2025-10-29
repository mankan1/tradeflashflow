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
const TRADIER_TOKEN  = process.env.TRADIER_TOKEN; // set me
const ALPACA_KEY     = process.env.ALPACA_KEY;    // set me
const ALPACA_SECRET  = process.env.ALPACA_SECRET; // set me

// chain auto-expand
const MAX_STRIKES_AROUND_ATM = 40;   // total (±20)
const MAX_EXPIRY_DAYS        = 30;

// sweep / block thresholds (tune these)
const SWEEP_WINDOW_MS        = 600;  // burst window per contract
const SWEEP_MIN_QTY          = 300;  // total prints qty in window
const SWEEP_MIN_NOTIONAL     = 75000;

const BLOCK_MIN_QTY          = 250;  // single print qty
const BLOCK_MIN_NOTIONAL     = 100000;

// fallback timesales polling
const FALLBACK_IDLE_MS       = 3500; // if no WS prints for a contract this long, poll REST
const FALLBACK_POLL_EVERY_MS = 1500;

/* ================= APP / WS ================= */
const app = express();
app.use(cors());
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

/* ================= STATE ================= */
const eqQuote = new Map(); // symbol -> { bid, ask, ts }
const optState = new Map(); // UL|YYYY-MM-DD|strike|right -> { oi_before, vol_today_before, last_reset }
const dayKey = () => new Date().toISOString().slice(0,10);

const alpacaSubs = new Set();               // equities
const tradierOptWatch = { set: new Set() }; // options watch set (JSON strings)

// for fallback poller and sweeps
const optLastPrintTs = new Map(); // occ -> last ts seen
const sweepBuckets   = new Map(); // occ -> { side, startTs, totalQty, notional, prints: [...] }

/* ================= HELPERS ================= */
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
    occ: toOcc(underlying, option.expiration, option.right, option.strike),
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
    side, qty: size, price,
    action: "UNK",
    venue: null
  };
}
function toOcc(ul, expISO, right, strike) {
  const yymmdd = expISO.replaceAll("-", "").slice(2);
  const cp = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike) * 1000)).padStart(8, "0");
  return `${ul.toUpperCase()}${yymmdd}${cp}${k}`;
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
/* ----- RESUB HELPERS ----- */
function resubAlpaca() {
  if (!alpacaWS || alpacaWS.readyState !== 1) return;
  const list = Array.from(alpacaSubs);
  try {
    alpacaWS.send(JSON.stringify({ action: "unsubscribe", trades: ["*"], quotes: ["*"] }));
  } catch {}
  try {
    alpacaWS.send(JSON.stringify({ action: "subscribe", trades: list, quotes: list }));
  } catch {}
}

function pruneOptionsForUnderlyings(uls /* Set<string> */) {
  const before = tradierOptWatch.set.size;
  for (const entry of Array.from(tradierOptWatch.set)) {
    try {
      const o = JSON.parse(entry);
      if (uls.has(String(o.underlying).toUpperCase())) tradierOptWatch.set.delete(entry);
    } catch {}
  }
  return before - tradierOptWatch.set.size; // removed count
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
  console.log("[tradier] session create ->", j);
  const sid = j?.stream?.sessionid ?? j?.sessionid ?? null;
  if (!sid) { console.warn("[tradier] session id null"); return; }
  tradier.sessionId = sid;

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

      // cache equity quotes
      if (t === "quote" && msg.symbol && !/^[A-Z]+(\d{6})([CP])(\d{8})$/.test(msg.symbol)) {
        const bid = Number(msg.bid ?? msg.b ?? msg.bp);
        const ask = Number(msg.ask ?? msg.a ?? msg.ap);
        if (Number.isFinite(bid) || Number.isFinite(ask)) {
          eqQuote.set(msg.symbol, { bid, ask, ts: Date.now() });
        }
        continue;
      }

      // trades / timesales
      if ((t === "trade" || t === "timesale") && msg.symbol) {
        const sym = msg.symbol;

        // OCC -> option
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

          // mark last-seen for fallback
          const occ = sym;
          optLastPrintTs.set(occ, Date.now());

          // normalize + fanout
          const out = normOptionsPrint("tradier", {
            underlying: ul,
            option: { expiration: exp, strike, right },
            price, size, side, venue: msg.exchange || msg.exch
          });
          out.occ = occ;
          pushAndFanout(out);

          // block detection
          const notion = price * size * 100;
          if (size >= BLOCK_MIN_QTY || notion >= BLOCK_MIN_NOTIONAL) {
            pushAndFanout({
              type: "blocks",
              provider: "tradier",
              ts: out.ts,
              symbol: ul,
              occ,
              option: out.option,
              side: out.side,
              qty: out.qty,
              price: out.price,
              notional: notion
            });
          }

          // sweep aggregation (per-contract burst)
          const existing = sweepBuckets.get(occ);
          const now = Date.now();
          if (!existing || (now - existing.startTs > SWEEP_WINDOW_MS) || (existing.side !== side)) {
            sweepBuckets.set(occ, {
              side, startTs: now, totalQty: size, notional: notion,
              prints: [{ ts: out.ts, qty: size, price, venue: out.venue }]
            });
          } else {
            existing.totalQty += size;
            existing.notional += notion;
            existing.prints.push({ ts: out.ts, qty: size, price, venue: out.venue });
          }
          const bucket = sweepBuckets.get(occ);
          if (bucket && (bucket.totalQty >= SWEEP_MIN_QTY || bucket.notional >= SWEEP_MIN_NOTIONAL)) {
            pushAndFanout({
              type: "sweeps",
              provider: "tradier",
              ts: now,
              symbol: ul,
              occ,
              option: out.option,
              side: bucket.side,
              totalQty: bucket.totalQty,
              notional: bucket.notional,
              prints: bucket.prints
            });
            sweepBuckets.delete(occ); // emit once per burst
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
    console.log("[tradier] resubscribe payload ->", payload);
    try { tradier.ws.send(JSON.stringify(payload)); } catch (e) {
      console.warn("[tradier] resubscribe send failed, reopening", e?.message || e);
      try { tradier.ws.close(); } catch {}
      setTimeout(() => ensureTradierWS(false), 250);
    }
  }
}

/* ================= FALLBACK TIMESALES POLLER ================= */
const lastPollCursor = new Map(); // occ -> ISO cursor (or null)
async function pollTimesalesOnce() {
  if (MOCK || !TRADIER_TOKEN) return;

  const now = Date.now();
  const occs = Array.from(tradierOptWatch.set).map(JSON.parse)
    .map(o => toOcc(o.underlying, o.expiration, o.right, o.strike));

  for (const occ of occs) {
    const last = optLastPrintTs.get(occ) || 0;
    if (now - last < FALLBACK_IDLE_MS) continue; // WS has been active recently

    const cursorISO = lastPollCursor.get(occ) || new Date(now - 5000).toISOString();
    const params = new URLSearchParams({
      symbol: occ,
      interval: "tick",
      start: cursorISO,
      session_filter: "all"
    });
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

      // parse OCC back -> UL/exp/strike/right
      const m = /^([A-Z]+)(\d{6})([CP])(\d{8})$/.exec(occ);
      if (!m) continue;
      const [ , ul, yymmdd, cp, k ] = m;
      const exp = `20${yymmdd.slice(0,2)}-${yymmdd.slice(2,4)}-${yymmdd.slice(4,6)}`;
      const strike = Number(k)/1000;
      const right = cp === "C" ? "CALL" : "PUT";
      const side  = (p.side || p.taker_side || "").toUpperCase().includes("SELL") ? "SELL" : "BUY";

      const out = normOptionsPrint("tradier", {
        underlying: ul,
        option: { expiration: exp, strike, right },
        price, size, side, venue: p.exch || p.venue
      });
      out.ts = ts;
      out.occ = occ;
      pushAndFanout(out);

      // apply block/sweep logic here too
      const notion = price * size * 100;
      if (size >= BLOCK_MIN_QTY || notion >= BLOCK_MIN_NOTIONAL) {
        pushAndFanout({
          type: "blocks",
          provider: "tradier",
          ts: out.ts,
          symbol: ul,
          occ,
          option: out.option,
          side: out.side,
          qty: out.qty,
          price: out.price,
          notional: notion
        });
      }

      const existing = sweepBuckets.get(occ);
      if (!existing || (ts - existing.startTs > SWEEP_WINDOW_MS) || (existing.side !== side)) {
        sweepBuckets.set(occ, {
          side, startTs: ts, totalQty: size, notional: notion,
          prints: [{ ts, qty: size, price, venue: out.venue }]
        });
      } else {
        existing.totalQty += size;
        existing.notional += notion;
        existing.prints.push({ ts, qty: size, price, venue: out.venue });
      }
      const bucket = sweepBuckets.get(occ);
      if (bucket && (bucket.totalQty >= SWEEP_MIN_QTY || bucket.notional >= SWEEP_MIN_NOTIONAL)) {
        pushAndFanout({
          type: "sweeps",
          provider: "tradier",
          ts,
          symbol: ul,
          occ,
          option: out.option,
          side: bucket.side,
          totalQty: bucket.totalQty,
          notional: bucket.notional,
          prints: bucket.prints
        });
        sweepBuckets.delete(occ);
      }
    }
    if (maxTs) {
      lastPollCursor.set(occ, new Date(maxTs + 1).toISOString());
      optLastPrintTs.set(occ, maxTs); // keep it “fresh” so we don't hammer
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

    // pick ATM +/- around last price; if no last, center by median strike
    const strikes = Array.from(new Set(list.map(o => Number(o.strike)))).sort((a,b)=>a-b);
    const center = Number.isFinite(last) ? last : strikes[Math.floor(strikes.length/2)] || 0;
    const best = strikes
      .map(s => ({ s, d: Math.abs(s - center) }))
      .sort((a,b)=>a.d - b.d)
      .slice(0, Math.min(MAX_STRIKES_AROUND_ATM, strikes.length))
      .map(x => x.s)
      .sort((a,b)=>a-b);

    // add both calls & puts for those strikes
    for (const k of best) {
      for (const right of ["CALL","PUT"]) {
        const entry = JSON.stringify({ underlying: ul.toUpperCase(), expiration: exp, strike: k, right });
        tradierOptWatch.set.add(entry);
      }
    }

    // push a chain snapshot for UI
    pushAndFanout({
      type: "chains",
      provider: "tradier",
      ts: Date.now(),
      symbol: ul.toUpperCase(),
      expiration: exp,
      strikes: best,
      strikesCount: best.length
    });
  }

  // refresh WS with expanded list
  ensureTradierWS(true);
}

/* ================= WS BOOTSTRAP ================= */
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
/* Add equities (and auto-expand options for them) */
app.post("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, added: 0, watching: { equities: Array.from(alpacaSubs) } });

  let added = 0;
  for (const s of symbols) if (!alpacaSubs.has(s)) { alpacaSubs.add(s); added++; }

  if (!MOCK) {
    await ensureAlpacaWS();
    // expand & watch option chains for these ULs
    if (TRADIER_TOKEN) {
      for (const ul of symbols) await expandAndWatchChain(ul);
    }
    ensureTradierWS(true);
    resubAlpaca();
  }

  res.json({ ok: true, added, watching: { equities: Array.from(alpacaSubs) } });
});
/* Remove equities (and stop all related option contracts) */
app.delete("/watch/symbols", async (req, res) => {
  const symbols = new Set([...(req.body?.symbols || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  if (!symbols.size) return res.json({ ok: true, removed: 0, optsRemoved: 0, watching: { equities: Array.from(alpacaSubs) } });

  let removed = 0;
  for (const s of symbols) if (alpacaSubs.delete(s)) removed++;

  // remove all watched options whose underlying is now removed
  const optsRemoved = pruneOptionsForUnderlyings(symbols);

  if (!MOCK) {
    // resub Alpaca with current equities
    resubAlpaca();
    // resub Tradier with pruned set
    ensureTradierWS(true);
  }

  res.json({
    ok: true,
    removed,
    optsRemoved,
    watching: {
      equities: Array.from(alpacaSubs),
      options: Array.from(tradierOptWatch.set).map(JSON.parse)
    }
  });
});
/* Remove explicit option contracts */
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

/* Remove equities directly (no options inference) */
app.delete("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  let removed = 0;
  for (const s of equities) if (alpacaSubs.delete(s)) removed++;
  if (!MOCK) { resubAlpaca(); ensureTradierWS(true); }
  res.json({ ok: true, removed, watching: { equities: Array.from(alpacaSubs) } });
});
/* Current watchlist (equities + explicit option contracts) */
app.get("/watchlist", (_req, res) => {
  res.json({
    equities: Array.from(alpacaSubs),
    options: Array.from(tradierOptWatch.set).map(JSON.parse)
  });
});

/* ================= WATCH ENDPOINTS ================= */
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

  // Explicit contracts
  for (const o of options) {
    const entry = JSON.stringify({
      underlying: String(o.underlying).toUpperCase(),
      expiration: String(o.expiration),
      strike: Number(o.strike),
      right: String(o.right).toUpperCase()
    });
    if (!tradierOptWatch.set.has(entry)) { tradierOptWatch.set.add(entry); added++; }
  }

  // Auto-expand full chain window for any underlyings passed in equities or in options list
  const uls = new Set();
  for (const o of options) if (o?.underlying) uls.add(String(o.underlying).toUpperCase());
  // also: if no options provided, read equities being watched and expand those too if desired
  if (uls.size === 0 && alpacaSubs.size) for (const s of alpacaSubs) uls.add(s);

  if (!MOCK && TRADIER_TOKEN) {
    for (const ul of uls) await expandAndWatchChain(ul);
  }

  if (!MOCK) ensureTradierWS(true);
  res.json({
    ok: true,
    watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) },
    added
  });
});

/* ================= DEBUG ================= */
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

/* ================= BACKFILL REST ================= */
app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
app.get("/health",               (_, res) => res.json({ ok: true }));
app.get("/debug/metrics",        (_req, res) => res.json({ mock: MOCK, count: httpMetrics.length, last5: httpMetrics.slice(-5) }));
app.use((req, res) => {
  res.status(404).json({ error: `Not found: ${req.method} ${req.originalUrl}` });
});
app.use((err, req, res, next) => {
  console.error("Server error:", err);
  res.status(err.status || 500).json({ error: err.message || "Internal Server Error" });
});

/* ================= START ================= */
server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`));
