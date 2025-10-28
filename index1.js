// server/index.js  (ESM)

import express from "express";
import cors from "cors";
import WebSocket, { WebSocketServer } from "ws";
import { XMLParser } from "fast-xml-parser";

const app = express();
app.use(cors());
app.use(express.json());

const server = app.listen(process.env.PORT || 8080, () =>
  console.log(`HTTP+WS @ :${server.address().port}  MOCK=${process.env.MOCK === "0" ? "off" : "on"}`)
);
const wss = new WebSocketServer({ server });

// ---------- CONFIG ----------
const MOCK = process.env.MOCK !== "0";
const MAX_BUFFER = 500;
const SWEEP_WINDOW_MS = 350;      // window to aggregate same-side prints into a sweep
const SWEEP_MIN_NOTIONAL = 75000; // $ notional threshold for a sweep
const BLOCK_MIN_CONTRACTS = 250;  // single print contracts threshold OR:
const BLOCK_MIN_NOTIONAL = 200000;// $ notional threshold

// ---------- BUFFERS ----------
const buffers = {
  equity_ts: [],
  options_ts: [],
  sweeps: [],
  blocks: [],
  chains: [] // snapshots of chains for the watched underlyings
};

// ---------- STATE ----------
const alpacaSubs = new Set();                 // equities symbols
const tradierOptWatch = { set: new Set() };  // JSON-serialized { underlying, expiration, right, strike }
const tradierSession = { id: null, ws: null };
const parser = new XMLParser();

// per-symbol quotes to determine buy/sell at print time
const eqQuote = new Map();          // AAPL -> {bid, ask, ts}
const optQuote = new Map();         // OCC -> {bid, ask, ts}

// sweep aggregator: underlying -> pending sweep bucket
const sweepBuckets = new Map();     // key: underlying, value: { side, start, prints[], notional }

// ---------- HELPERS ----------
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}

// unique ids without extra deps
function nextId() {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function pushAndFanout(obj) {
  const arr = buffers[obj.type];
  if (!arr) return;
  arr.unshift(obj);
  if (arr.length > MAX_BUFFER) arr.length = MAX_BUFFER;
  broadcast(obj);
}

function occSymbol({ underlying, expiration, right, strike }) {
  // underlying: "AAPL", expiration: "2025-12-19", right: "C" | "P", strike: number
  const y = expiration.slice(2,4);
  const m = expiration.slice(5,7);
  const d = expiration.slice(8,10);
  const strikeInt = Math.round(Number(strike) * 1000).toString().padStart(8, "0");
  return `${underlying}${y}${m}${d}${right}${strikeInt}`;
}

function inferSideFromQuote({ last, bid, ask }) {
  if (bid == null || ask == null || last == null) return null;
  if (last >= ask) return "BUY";
  if (last <= bid) return "SELL";
  // mid or inside tick — leave null to avoid over-labeling
  return null;
}

// buy/sell-to-open/close tag per your rule-of-thumb
function openCloseTag({ side, size, price, oiBefore, dayVolBefore }) {
  // “qty > oi + volume (prior to the trade) -> open”, otherwise close.
  // Additionally label as buy/sell-to-open/close using the direction.
  const contracts = size; // options “size” is in contracts from stream
  const openish = contracts > (Number(oiBefore||0) + Number(dayVolBefore||0));
  if (side === "BUY")  return openish ? "BUY_TO_OPEN"  : "BUY_TO_CLOSE";
  if (side === "SELL") return openish ? "SELL_TO_OPEN" : "SELL_TO_CLOSE";
  return null;
}

// ---------- WATCH ENDPOINTS ----------
app.post("/watch/alpaca", (req, res) => {
  const equities = new Set([...(req.body?.equities || [])].map(s => String(s).toUpperCase()).filter(Boolean));
  for (const s of equities) alpacaSubs.add(s);
  if (MOCK) startMock(); else ensureAlpacaWS();
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
  if (MOCK) startMock(); else ensureTradierStream(); // << use stream, not REST timesales
  // also kick chains refresh
  if (!MOCK) refreshChainsNow().catch(() => {});
  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

// ---------- FLOW REST ENDPOINTS (unchanged for client) ----------
app.get("/api/flow/equity_ts", (req, res) => res.json({ mock: MOCK, count: buffers.equity_ts.length, last5: buffers.equity_ts.slice(0,5) }));
app.get("/api/flow/options_ts", (req, res) => res.json({ mock: MOCK, count: buffers.options_ts.length, last5: buffers.options_ts.slice(0,5) }));
app.get("/api/flow/sweeps",    (req, res) => res.json({ mock: MOCK, count: buffers.sweeps.length,    last5: buffers.sweeps.slice(0,5) }));
app.get("/api/flow/blocks",    (req, res) => res.json({ mock: MOCK, count: buffers.blocks.length,    last5: buffers.blocks.slice(0,5) }));
app.get("/api/flow/chains",    (req, res) => res.json({ mock: MOCK, count: buffers.chains.length,    last5: buffers.chains.slice(0,5) }));

// ---------------------------------------------------------------
//  Alpaca equities WS (your function kept, just ensure it’s called)
// ---------------------------------------------------------------
let alpacaWS;
async function ensureAlpacaWS() {
  if (MOCK) return;
  if (alpacaWS && alpacaWS.readyState === 1) return;

  const KEY = process.env.ALPACA_KEY, SECRET = process.env.ALPACA_SECRET;
  if (!KEY || !SECRET) { console.warn("Alpaca keys missing; equities will be empty."); return; }

  const url = "wss://stream.data.alpaca.markets/v2/iex";
  alpacaWS = new WebSocket(url);

  alpacaWS.onopen = () => {
    alpacaWS.send(JSON.stringify({ action: "auth", key: KEY, secret: SECRET }));
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
        if (m.T === "t") {
          const side = (() => {
            const q = eqQuote.get(m.S);
            if (!q) return null;
            if (m.p >= q.ask) return "BUY";
            if (m.p <= q.bid) return "SELL";
            return null;
          })();
          pushAndFanout({
            type: "equity_ts",
            id: nextId(),
            provider: "alpaca",
            symbol: m.S,
            price: m.p,
            size: m.s || m.z || 0,
            side,
            ts: Date.now()
          });
        }
      }
    } catch {}
  };
  alpacaWS.onclose = () => setTimeout(ensureAlpacaWS, 1200);
  alpacaWS.onerror = () => {};
}

// ---------------------------------------------------------------
//  Tradier streaming: options + equities events (timesale/quote/trade)
// ---------------------------------------------------------------
async function ensureTradierStream() {
  if (MOCK) return;
  // Create/refresh session
  if (!tradierSession.id) {
    const token = process.env.TRADIER_TOKEN;
    if (!token) { console.warn("TRADIER_TOKEN missing; options will be empty."); return; }

    const r = await fetch("https://stream.tradier.com/v1/markets/events/session", {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!r.ok) { console.warn("Tradier session create failed", r.status); return; }
    const { session_id } = await r.json();
    tradierSession.id = session_id;
  }

  // Connect WS (or HTTP stream). Using WS here:
  if (tradierSession.ws && tradierSession.ws.readyState === 1) return;

  // Build symbols: equities from alpacaSubs + OCC from option watch
  const occs = Array.from(tradierOptWatch.set).map(s => occSymbol(JSON.parse(s)));
  const equities = Array.from(alpacaSubs);
  const symbolsList = [...new Set([...equities, ...occs])];
  if (!symbolsList.length) return;

  const url = `wss://stream.tradier.com/v1/markets/events?sessionid=${tradierSession.id}&symbols=${encodeURIComponent(symbolsList.join(","))}&filter=trade,quote,timesale&linebreak=true&validOnly=true`;
  tradierSession.ws = new WebSocket(url, { headers: { Authorization: `Bearer ${process.env.TRADIER_TOKEN}` } });

  tradierSession.ws.onmessage = (ev) => {
    // one JSON object per line
    try {
      const m = JSON.parse(ev.data);
      // quotes
      if (m.type === "quote") {
        const isOption = /[CP]\d{8}$/.test(m.symbol) || m.symbol.length > 15;
        if (isOption) optQuote.set(m.symbol, { bid: Number(m.bid), ask: Number(m.ask), ts: Number(m.askdate || m.biddate) || Date.now() });
        else eqQuote.set(m.symbol, { bid: Number(m.bid), ask: Number(m.ask), ts: Number(m.askdate || m.biddate) || Date.now() });
      }

      // trades / timesales (both carry last/size)
      if (m.type === "trade" || m.type === "timesale") {
        const isOption = /[CP]\d{8}$/.test(m.symbol) || m.symbol.length > 15;
        const last  = Number(m.last ?? m.price);
        const size  = Number(m.size || 0); // contracts for options; shares for equities
        const bid   = Number(m.bid ?? (isOption ? optQuote.get(m.symbol)?.bid : eqQuote.get(m.symbol)?.bid));
        const ask   = Number(m.ask ?? (isOption ? optQuote.get(m.symbol)?.ask : eqQuote.get(m.symbol)?.ask));
        const side  = inferSideFromQuote({ last, bid, ask });
        const ts    = Number(m.date || Date.now());

        if (!isFinite(last) || !size) return;

        if (!isOption) {
          // equity print
          pushAndFanout({
            type: "equity_ts", id: nextId(), provider: "tradier",
            symbol: m.symbol, price: last, size, side, ts
          });
        } else {
          // option print
          // derive underlying from OCC:
          const und = m.symbol.replace(/(\d{6}[CP]\d{8})$/, ""); // crude but works on OCC format like AAPL251219C00450000
          // fetch OI + day vol we know locally (chain cache below)
          const chInfo = latestChainInfoFor(m.symbol);
          const ocTag = openCloseTag({ side, size, price: last, oiBefore: chInfo?.oi, dayVolBefore: chInfo?.dayVol });

          const notional = last * size * 100;
          const print = {
            type: "options_ts", id: nextId(), provider: "tradier",
            symbol: m.symbol, underlying: und, price: last, size, side, oc: ocTag,
            bid, ask, notional, ts
          };
          pushAndFanout(print);
          considerBlock(print);
          considerSweep(print);
        }
      }
    } catch (e) { /* ignore bad lines */ }
  };

  tradierSession.ws.onclose = () => {
    tradierSession.ws = null;
    tradierSession.id = null;
    setTimeout(ensureTradierStream, 1000);
  };
  tradierSession.ws.onerror = () => {};
}

// ---------- Sweeps / Blocks ----------
function considerBlock(p) {
  // single large print
  if (p.type !== "options_ts") return;
  if (p.size >= BLOCK_MIN_CONTRACTS || p.notional >= BLOCK_MIN_NOTIONAL) {
    pushAndFanout({
      type: "blocks",
      id: nextId(),
      provider: p.provider,
      symbol: p.symbol,
      underlying: p.underlying,
      side: p.side,
      oc: p.oc,
      size: p.size,
      price: p.price,
      notional: p.notional,
      ts: p.ts
    });
  }
}

function flushSweep(key) {
  const b = sweepBuckets.get(key);
  if (!b || b.prints.length === 0) return;
  const totalContracts = b.prints.reduce((s,x)=>s+x.size,0);
  const totalNotional  = b.prints.reduce((s,x)=>s+x.notional,0);
  if (totalNotional >= SWEEP_MIN_NOTIONAL && totalContracts > 1) {
    // emit sweep
    pushAndFanout({
      type: "sweeps",
      id: nextId(),
      provider: "tradier",
      underlying: key,
      side: b.side,
      oc: dominantOC(b.prints),
      legs: b.prints.length,
      totalContracts,
      totalNotional,
      firstTs: b.start,
      lastTs: b.prints[b.prints.length-1].ts,
      sample: b.prints.slice(-3) // last few prints for the card
    });
  }
  sweepBuckets.delete(key);
}

function dominantOC(prints) {
  const cnt = new Map();
  for (const p of prints) if (p.oc) cnt.set(p.oc, (cnt.get(p.oc)||0)+1);
  let best=null, max=-1;
  for (const [k,v] of cnt) if (v>max) {max=v; best=k;}
  return best || null;
}

function considerSweep(p) {
  // aggregate rapid same-side prints by underlying
  if (p.type !== "options_ts" || !p.side) return;
  const key = p.underlying;
  const now = p.ts;

  const b = sweepBuckets.get(key);
  if (!b || (now - b.start) > SWEEP_WINDOW_MS || (b.side && p.side !== b.side)) {
    // flush old and start new
    if (b) flushSweep(key);
    sweepBuckets.set(key, { side: p.side, start: now, prints: [p] });
    return;
  }
  // add to bucket
  b.prints.push(p);
  // extend window slightly
  b.start = Math.min(b.start, now);
  // If window exceeded by growth, flush (optional; otherwise timerless flush happens on next miss)
  if ((now - b.start) > SWEEP_WINDOW_MS) flushSweep(key);
}

// ---------- Chains refresh (REST) ----------
const chainCache = new Map(); // occ -> { bid, ask, oi, dayVol, ts, underlying }

function latestChainInfoFor(occ) {
  return chainCache.get(occ);
}

async function refreshChainsNow() {
  const token = process.env.TRADIER_TOKEN;
  if (!token) return;

  // collect unique underlyings from watch
  const byUnderlying = new Map();
  for (const s of tradierOptWatch.set) {
    const o = JSON.parse(s);
    if (!byUnderlying.has(o.underlying)) byUnderlying.set(o.underlying, new Set());
    byUnderlying.get(o.underlying).add(o.expiration);
  }

  for (const [und, exps] of byUnderlying) {
    for (const exp of exps) {
      const url = `https://api.tradier.com/v1/markets/options/chains?symbol=${und}&expiration=${exp}`;
      const r = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
      if (!r.ok) continue;
      const text = await r.text();
      const xml = parser.parse(text);
      const arr = xml?.options?.option || [];
      const list = Array.isArray(arr) ? arr : [arr];

      for (const it of list) {
        const occ = String(it.symbol);
        const bid = Number(it.bid ?? 0);
        const ask = Number(it.ask ?? 0);
        const oi  = Number(it.open_interest ?? 0);
        const dayVol = Number(it.volume ?? 0);
        chainCache.set(occ, { bid, ask, oi, dayVol, ts: Date.now(), underlying: String(it.underlying) });
      }

      // snapshot for client “chains” screen
      buffers.chains.unshift({
        type: "chains",
        id: nextId(),
        provider: "tradier",
        underlying: und,
        expiration: exp,
        count: list.length,
        ts: Date.now()
      });
      if (buffers.chains.length > MAX_BUFFER) buffers.chains.length = MAX_BUFFER;
    }
  }
}

// periodic chains refresh
if (!MOCK) setInterval(() => refreshChainsNow().catch(()=>{}), 15_000);

// ---------- Mock (optional; unchanged) ----------
function startMock(){ /* keep your mock impl if you want */ }

