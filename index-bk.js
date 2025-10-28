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
//const MOCK = process.env.MOCK !== "0";
const rawMock = (process.env.MOCK ?? "").toString().trim();
export const MOCK = rawMock === "1";        // only ON when explicitly "1"

const TRADIER_BASE = process.env.TRADIER_BASE || "https://api.tradier.com";
const PORT = process.env.PORT || 8080;

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

// ---- equity quotes cache to classify buy/sell by bid/ask ----
const eqQuote = new Map(); // symbol -> { bid, ask, ts }

// ---- options OI/vol state to classify BTO/BTC/STO/STC ----
const optState = new Map(); // key: UL|YYYY-MM-DD|strike|right -> { oi_before, vol_today_before, last_reset }
const dayKey = () => new Date().toISOString().slice(0, 10);

const ALPACA_KEY = process.env.ALPACA_KEY, ALPACA_SECRET = process.env.ALPACA_SECRET;
const TRADIER_TOKEN = "DZi4KKhQVv05kjgqXtvJRyiFbEhn"; //process.env.TRADIER_TOKEN;

// if (!MOCK) {
//   const missing = [
//     !TRADIER_TOKEN && "TRADIER_TOKEN",
//     !ALPACA_KEY && "ALPACA_KEY",
//     !ALPACA_SECRET && "ALPACA_SECRET",
//   ].filter(Boolean);
//   if (missing.length) {
//     console.warn(`Missing required env vars (real mode): ${missing.join(", ")}`);
//   }
// }
// pseudo-cache objects backing /api/flow/*:
const cache = {
  equity_ts: { etag: "", items: [] },
  options_ts: { etag: "", items: [] },
  sweeps:    { etag: "", items: [] },
  blocks:    { etag: "", items: [] },
  chains:    { etag: "", items: [] },
};

function bump(listName, item) {
  const tgt = cache[listName];
  tgt.items.unshift(item);
  if (tgt.items.length > 1000) tgt.items.length = 1000;
  // new ETag each write so GETs flip from 304 -> 200
  tgt.etag = `${Date.now()}-${tgt.items.length}`;
}

// after adding to alpacaSubs
//if (MOCK) startMock();
//else ensureAlpacaWS();      // must attempt to connect NOW

// after adding to tradierOptWatch.set
//if (MOCK) startMock();
//else startTradierPoll();    // must start/refresh polling NOW

function getOptState(key) {
  const today = dayKey();
  const s = optState.get(key) ?? { oi_before: 0, vol_today_before: 0, last_reset: today };
  if (s.last_reset !== today) { s.vol_today_before = 0; s.last_reset = today; }
  return s;
}
const setOptOI  = (k, oi) => { const s = getOptState(k); s.oi_before = Number(oi)||0; optState.set(k, s); };
const setOptVol = (k, v)  => { const s = getOptState(k); s.vol_today_before = Math.max(0, Number(v)||0); optState.set(k, s); };
const bumpOptVol = (k, qty) => { const s = getOptState(k); s.vol_today_before += Number(qty)||0; optState.set(k, s); };

// ---- classification helpers ----
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

// // ---- fanout ----
// function broadcast(obj) {
//   const s = JSON.stringify(obj);
//   for (const c of wss.clients) if (c.readyState === 1) c.send(s);
// }
// function pushAndFanout(obj) {
//   const arr = buffers[obj.type];
//   if (!arr) return;
//   arr.unshift(obj);
//   if (arr.length > MAX_BUFFER) arr.length = MAX_BUFFER;
//   broadcast(obj);
// }

// after your `buffers` and `MAX_BUFFER`
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));

/** Make a stable-ish id from important fields so duplicates collapse cleanly. */
function stableId(msg) {
  // pick fields commonly present in your flow messages
  const key = JSON.stringify({
    type: msg.type,
    provider: msg.provider,
    symbol: msg.symbol ?? msg.underlying,
    occ: msg.occ,              // for options if you set it
    side: msg.side,
    price: msg.price,
    size: msg.size,
    ts: msg.ts ?? msg.time ?? msg.at, // any timestamp the source had
  });
  return createHash("sha1").update(key).digest("hex"); // 40-char hex
}

function normalizeForFanout(msg) {
  const now = Date.now();
  const withTs = { ts: typeof msg.ts === "number" ? msg.ts : now, ...msg };

  // prefer provided id, else stable hash, else random
  const id = msg.id ?? stableId(withTs) ?? randomUUID();
  return { id, ...withTs };
}

// ---- fanout ----
function broadcast(obj) {
  const s = JSON.stringify(obj);
  for (const c of wss.clients) if (c.readyState === 1) c.send(s);
}

function pushAndFanout(msg) {
  const m = normalizeForFanout(msg);
  const arr = buffers[m.type];
  if (!arr) return;

  const seen = seenIds[m.type] || (seenIds[m.type] = new Set());
  if (seen.has(m.id)) return; // de-dupe

  arr.unshift(m);
  seen.add(m.id);

  while (arr.length > MAX_BUFFER) {
    const dropped = arr.pop();
    if (dropped?.id) seen.delete(dropped.id);
  }

  broadcast(m);
}
// ---- normalizers ----
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

// ---- watchers (MOCK + REAL) ----
let alpacaWS = null;
const alpacaSubs = new Set(); // equities
const tradierOptWatch = { set: new Set(), pollTimer: null, lastTs: new Map() };

function startMock() {
  if (startMock._on) return;
  startMock._on = true;
  setInterval(() => {
    // mock equity quotes + trades
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
    // mock options prints for watched contracts
    for (const key of tradierOptWatch.set) {
      const o = JSON.parse(key);
      const size = Math.floor(Math.random()*50)+1;
      const price = +(Math.random()*2 + 0.05).toFixed(2);
      const side = Math.random()<0.5 ? "BUY" : "SELL";
      const sKey = `${o.underlying}|${o.expiration}|${o.strike}|${o.right}`;
      const s = getOptState(sKey);
      if (Math.random() < 0.3) setOptOI(sKey, Math.max(0, s.oi_before + Math.floor((Math.random()-0.5)*200)));
      pushAndFanout(normOptionsPrint("tradier", {
        underlying: o.underlying,
        option: { expiration: o.expiration, strike: o.strike, right: o.right },
        price, size, side, venue: "CBOE"
      }));
    }
  }, 800);
}

async function ensureAlpacaWS() {
  if (MOCK) {
      console.warn("Using mock."); 
      return;
  }
  if (alpacaWS && alpacaWS.readyState === 1) return;

  const KEY = "AKNND2CVUEIRFCDNVMXL2NYVWD";//process.env.ALPACA_KEY;
  const SECRET = "5xBdG2Go1PtWE36wnCrB4vES6mGF6tkusqDL7uSnnCxy";//process.env.ALPACA_SECRET;
  if (!KEY || !SECRET) { console.warn("Alpaca keys missing; staying mock."); return; }

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
        if (m.T === "t") pushAndFanout(normEquityPrint("alpaca", { symbol: m.S, price: m.p, size: m.s || m.z || 0 }));
      }
    } catch {}
  };
  alpacaWS.onclose = () => setTimeout(ensureAlpacaWS, 1200);
  alpacaWS.onerror = () => {};
}

async function pollTradierOptionsOnce() {
  if (MOCK || tradierOptWatch.set.size === 0) return;
  const token = "DZi4KKhQVv05kjgqXtvJRyiFbEhn"; //process.env.TRADIER_TOKEN;
  if (!token) { console.warn("Tradier token missing; staying mock."); return; }

  // group by UL, pull chains to refresh OI/volume
  const byUL = {};
  for (const key of tradierOptWatch.set) {
    const o = JSON.parse(key);
    (byUL[o.underlying] ??= []).push(o);
  }
  for (const [ul, arr] of Object.entries(byUL)) {
    const expirations = [...new Set(arr.map(a => a.expiration))];
    for (const exp of expirations) {
      const url = `${TRADIER_BASE}/v1/markets/options/chains?symbol=${ul}&expiration=${exp}`;
      const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${token}` } });
      if (!r.ok) continue;
      const j = await r.json();
      const list = j?.options?.option || [];
      for (const a of arr.filter(x => x.expiration === exp)) {
        const match = list.find(o =>
          Number(o.strike) === Number(a.strike) &&
          String(o.option_type).toUpperCase().startsWith(a.right)
        );
        if (match) {
          const k = `${ul}|${a.expiration}|${a.strike}|${a.right}`;
          if (Number.isFinite(match.open_interest)) setOptOI(k, Number(match.open_interest));
          if (Number.isFinite(match.volume)) setOptVol(k, Number(match.volume));
        }
      }
    }
  }

  // timesales polling (adjust to your plan if needed)
  for (const key of tradierOptWatch.set) {
    const o = JSON.parse(key);
    const last = tradierOptWatch.lastTs.get(key) ?? (Date.now() - 5000);
    // const startISO = new Date(last - 1000).toISOString();
    const occ = toOcc(o.underlying, o.expiration, o.right, o.strike);

    // Build query: options time & sales (tick granularity)
    const params = new URLSearchParams({
      symbol: occ,
      interval: "tick",
      session_filter: "all" // include all sessions; remove if you only want regular
    });

    const url = `${TRADIER_BASE}/v1/markets/timesales?${params.toString()}`;

    // const url = `${TRADIER_BASE}/v1/markets/timesales?symbol=${occ}&interval=1s&start=${startISO}`;
    const r = await fetch(url, { headers: { "Accept": "application/json", Authorization: `Bearer ${token}` } });
    if (!r.ok) continue;
    const j = await r.json();
    const prints = j?.series?.data || j?.data || [];
    for (const p of prints) {
      const price = Number(p.price ?? p.last ?? p.p ?? 0);
      const size  = Number(p.size  ?? p.volume ?? p.s ?? 0);
      const ts    = p.timestamp ? Date.parse(p.timestamp)
                   : (p.time ? Date.parse(p.time) : Date.now());
      const side  = (p.side || p.taker_side || "").toUpperCase().includes("SELL") || (p.side === "S")
                    ? "SELL" : "BUY";
      const out   = normOptionsPrint("tradier", {
        underlying: o.underlying,
        option: { expiration: o.expiration, strike: o.strike, right: o.right },
        price, size, side, venue: p.exch || p.venue, oi: undefined, volToday: undefined
      });
      out.ts = ts;
      pushAndFanout(out);
      tradierOptWatch.lastTs.set(key, ts);
    }
  }
}
function toOcc(ul, expISO, right, strike) {
  const exp = expISO.replaceAll("-", "").slice(2);
  const rt = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike)*1000)).padStart(8,"0");
  return `${ul.toUpperCase()}${exp}${rt}${k}`;
}
function startTradierPoll() {
  if (MOCK) {
      console.warn("Alpaca keys missing; staying mock."); 
      return;
  }
  if (!tradierOptWatch.pollTimer) tradierOptWatch.pollTimer = setInterval(pollTradierOptionsOnce, 1500);
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
  if (MOCK) startMock(); else startTradierPoll();
  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

app.get("/debug/metrics", (_req, res) => {
  res.json({
    mock: MOCK,
    count: httpMetrics.length,
    last5: httpMetrics.slice(-5),
  });
});

// ---- backfill REST ----
app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));
app.get("/health", (_, res) => res.json({ ok: true }));

server.listen(PORT, () => console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`));

