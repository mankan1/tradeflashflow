// index.js (ESM)
import "./instrument-http.js"; // keep if you rely on httpMetrics; else remove
import { httpMetrics } from "./instrument-http.js";
import http from "http";
import express from "express";
import cors from "cors";
import compression from "compression";
import morgan from "morgan";
import WebSocket, { WebSocketServer } from "ws";
import { randomUUID, createHash } from "crypto";

// --------------------- CONFIG ---------------------
const rawMock = (process.env.MOCK ?? "").toString().trim();
const MOCK = rawMock === "1";
const PORT = Number(process.env.PORT || 8080);
const TRADIER_BASE = process.env.TRADIER_BASE || "https://api.tradier.com";
const TRADIER_STREAM_BASE = process.env.TRADIER_STREAM_BASE || "https://api.tradier.com";
const TRADIER_WS_HOST = process.env.TRADIER_WS_HOST || "wss://ws.tradier.com";
const ALPACA_KEY = process.env.ALPACA_KEY || "";
const ALPACA_SECRET = process.env.ALPACA_SECRET || "";
const TRADIER_TOKEN = process.env.TRADIER_TOKEN || "";

// --------------------- APP/WS ---------------------
const app = express();
app.use(cors());
app.use(express.json({ limit: "2mb" }));
app.use(compression());
app.use(morgan("tiny"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// --------------------- BUFFERS ---------------------
const MAX_BUFFER = 400;
const buffers = {
  options_ts: [],
  equity_ts: [],
  sweeps: [],
  blocks: [],
  chains: [],
};
const seenIds = Object.fromEntries(Object.keys(buffers).map(t => [t, new Set()]));

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

// --------------------- STATE ---------------------
// equities (Alpaca)
let alpacaWS = null;
const alpacaSubs = new Set(); // e.g., AAPL, NVDA

// options/equities (Tradier WS)
let tradierWS = null;
let tradierSessionId = null;
let subTimer = null;
let subDirty = false;
const tradierOptWatch = { set: new Set() }; // stringified {underlying, expiration, strike, right}

// equity quote cache for buy/sell classification
const eqQuote = new Map(); // symbol -> { bid, ask, ts }

// option OI/Vol state for BTO/BTC/STO/STC
const optState = new Map();
const dayKey = () => new Date().toISOString().slice(0, 10);
function getOptState(key) {
  const today = dayKey();
  const s = optState.get(key) ?? { oi_before: 0, vol_today_before: 0, last_reset: today };
  if (s.last_reset !== today) { s.vol_today_before = 0; s.last_reset = today; }
  return s;
}
const setOptOI  = (k, oi) => { const s = getOptState(k); s.oi_before = Number(oi)||0; optState.set(k, s); };
const setOptVol = (k, v)  => { const s = getOptState(k); s.vol_today_before = Math.max(0, Number(v)||0); optState.set(k, s); };
const bumpOptVol = (k, qty) => { const s = getOptState(k); s.vol_today_before += Number(qty)||0; optState.set(k, s); };

// --------------------- CLASSIFIERS ---------------------
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
function normOptionsPrint(provider, { underlying, option, price, size, side, venue, oi, volToday, ts }) {
  const now = ts ?? Date.now();
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
function normEquityPrint(provider, { symbol, price, size, ts }) {
  const now = ts ?? Date.now();
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

// --------------------- HELPERS ---------------------
function toOcc(ul, expISO, right, strike) {
  const exp = expISO.replaceAll("-", "").slice(2);
  const rt = String(right).toUpperCase().startsWith("C") ? "C" : "P";
  const k = String(Math.round(Number(strike)*1000)).padStart(8,"0");
  return `${ul.toUpperCase()}${exp}${rt}${k}`;
}
function buildSubscribeSymbols() {
  const eq = Array.from(alpacaSubs);
  const occs = Array.from(tradierOptWatch.set).map(s => {
    const o = JSON.parse(s);
    return toOcc(o.underlying, o.expiration, o.right, o.strike);
  });
  // We can include equities in Tradier stream (for quotes/trades side refs)
  return [...new Set([...eq, ...occs])];
}
function sendWhenOpen(ws, payload) {
  const msg = JSON.stringify(payload);
  if (!ws || ws.readyState !== 1) return;
  ws.send(msg);
}

// --------------------- ALPACA WS (equities) ---------------------
async function ensureAlpacaWS() {
  if (MOCK) return;
  if (alpacaWS && alpacaWS.readyState === 1) return;
  if (!ALPACA_KEY || !ALPACA_SECRET) { console.warn("Alpaca keys missing."); return; }

  const url = "wss://stream.data.alpaca.markets/v2/iex";
  alpacaWS = new WebSocket(url);

  alpacaWS.onopen = () => {
    alpacaWS.send(JSON.stringify({ action: "auth", key: ALPACA_KEY, secret: ALPACA_SECRET }));
    if (alpacaSubs.size) {
      alpacaWS.send(JSON.stringify({
        action: "subscribe",
        trades: Array.from(alpacaSubs),
        quotes: Array.from(alpacaSubs)
      }));
    }
  };
  alpacaWS.onmessage = (e) => {
    try {
      const arr = JSON.parse(e.data);
      if (!Array.isArray(arr)) return;
      for (const m of arr) {
        if (m.T === "q") eqQuote.set(m.S, { bid: m.bp, ask: m.ap, ts: Date.parse(m.t) || Date.now() });
        if (m.T === "t") pushAndFanout(normEquityPrint("alpaca", { symbol: m.S, price: m.p, size: m.s || m.z || 0, ts: Date.parse(m.t) || Date.now() }));
      }
    } catch {}
  };
  alpacaWS.onclose = () => setTimeout(ensureAlpacaWS, 1200);
  alpacaWS.onerror = () => {};
}

// --------------------- TRADIER WS (options + quotes) ---------------------
let tradierConnectInFlight = false;

async function ensureTradierWS() {
  if (MOCK) return;
  if (tradierWS && tradierWS.readyState === 1) return;
  if (!TRADIER_TOKEN) { console.warn("Tradier token missing."); return; }
  if (tradierConnectInFlight) return;
  tradierConnectInFlight = true;

  try {
    const sessUrl = `${TRADIER_STREAM_BASE}/v1/markets/events/session`;
    const r = await fetch(sessUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${TRADIER_TOKEN}`,
        Accept: "application/json"
      }
    });
    if (!r.ok) {
      console.error("[tradier] session create failed", r.status);
      tradierConnectInFlight = false;
      return;
    }
    const j = await r.json();
    tradierSessionId = j?.stream?.session_id || j?.session_id || j?.sessionId || null;
    console.log("[tradier] session id:", tradierSessionId);

    const wsUrl = `${TRADIER_WS_HOST}/v1/markets/events`;
    tradierWS = new WebSocket(wsUrl);

    tradierWS.on("open", () => {
      console.log("[tradier] WS open");
      scheduleSubscriptionRefresh(); // will wait if no symbols yet
    });

    tradierWS.on("message", (data) => {
      const lines = String(data).split("\n").map(s => s.trim()).filter(Boolean);
      for (const line of lines) {
        try {
          const m = JSON.parse(line);
          // m.type examples: quote, trade, timesale
          if (m.type === "quote") {
            if (m.symbol && m.bid != null && m.ask != null) {
              eqQuote.set(m.symbol, { bid: Number(m.bid), ask: Number(m.ask), ts: Date.now() });
            }
          } else if (m.type === "trade" || m.type === "timesale") {
            // options or equities
            const sym = m.symbol;
            const isOption = /[CP]\d{8}$/.test(sym);
            const price = Number(m.price ?? m.last ?? m.p ?? 0);
            const size  = Number(m.size ?? m.volume ?? m.s ?? 0);
            const ts    = m.timestamp ? Date.parse(m.timestamp) : Date.now();
            const side  = (m.side || m.taker_side || "").toUpperCase().includes("SELL") || m.side === "S" ? "SELL" : "BUY";

            if (isOption) {
              // decode OCC
              const occ = sym;
              const rt = occ.slice(-9, -8) === "C" ? "CALL" : "PUT";
              const strike = Number(occ.slice(-8)) / 1000;
              const ul = occ.replace(/[CP]\d{8}$/, "").slice(0, -6); // UL + YYMMDD
              const exp = "20" + occ.slice(occ.length - 15, occ.length - 9).replace(/(..)(..)(..)/, "$1-$2-$3");
              // We don't know UL from stream reliably — you can maintain a reverse map from watch set:
              const o = Array.from(tradierOptWatch.set).map(JSON.parse).find(x =>
                toOcc(x.underlying, x.expiration, x.right, x.strike) === occ
              );
              const underlying = o?.underlying || ul;

              pushAndFanout(normOptionsPrint("tradier", {
                underlying,
                option: { expiration: o?.expiration ?? exp, strike: o?.strike ?? strike, right: o?.right ?? rt },
                price, size, side, venue: m.exch || m.venue, ts
              }));
            } else {
              pushAndFanout(normEquityPrint("tradier", { symbol: sym, price, size, ts }));
            }
          }
        } catch {/* ignore one bad line */}
      }
    });

    tradierWS.on("close", () => {
      console.warn("[tradier] WS close -> reconnect");
      tradierWS = null;
      setTimeout(() => { tradierConnectInFlight = false; ensureTradierWS(); }, 1000);
    });

    tradierWS.on("error", (err) => {
      console.warn("[tradier] WS error", err?.message || err);
    });

  } catch (e) {
    console.error("[tradier] ensureTradierWS error", e);
    tradierConnectInFlight = false;
  }
}

function scheduleSubscriptionRefresh() {
  subDirty = true;
  if (subTimer) return;
  subTimer = setTimeout(() => {
    subTimer = null;
    if (!subDirty) return;
    subDirty = false;

    if (!tradierWS || tradierWS.readyState !== 1) return; // wait until open
    const symbols = buildSubscribeSymbols();
    const payload = {
      sessionid: tradierSessionId,
      symbols,
      filter: ["quote", "trade", "timesale"],
      linebreak: true,
      validOnly: true
    };
    if (!Array.isArray(symbols) || symbols.length === 0) {
      console.log("[tradier] subscribe skipped (no symbols yet) -> retry in 1s");
      setTimeout(scheduleSubscriptionRefresh, 1000);
      return;
    }
    console.log("[tradier] subscribe ->", payload);
    sendWhenOpen(tradierWS, payload);
  }, 150);
}

// --------------------- WS (server fanout) ---------------------
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

// --------------------- WATCH ENDPOINTS ---------------------
app.post("/watch/alpaca", (req, res) => {
  const arr = (req.body?.equities || []).map(s => String(s).toUpperCase()).filter(Boolean);
  for (const s of arr) alpacaSubs.add(s);
  console.log("[watch/alpaca] now watching equities:", Array.from(alpacaSubs));

  if (MOCK) {
    // no mock stream here, but you could push fake ticks if desired
  } else {
    ensureAlpacaWS(); // subscribe trades+quotes for equities
    ensureTradierWS(); // we also want quotes/trades for these on Tradier side
    scheduleSubscriptionRefresh();
  }
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
  console.log("[watch/tradier] now watching options:", Array.from(tradierOptWatch.set).map(JSON.parse));

  if (!MOCK) {
    ensureTradierWS();
    scheduleSubscriptionRefresh();
  }
  res.json({ ok: true, watching: { options: Array.from(tradierOptWatch.set).map(JSON.parse) } });
});

// --------------------- BACKFILL/DEBUG ---------------------
app.get("/api/flow/options_ts", (_, res) => res.json(buffers.options_ts));
app.get("/api/flow/equity_ts",  (_, res) => res.json(buffers.equity_ts));
app.get("/api/flow/sweeps",     (_, res) => res.json(buffers.sweeps));
app.get("/api/flow/blocks",     (_, res) => res.json(buffers.blocks));
app.get("/api/flow/chains",     (_, res) => res.json(buffers.chains));

app.get("/debug/metrics", (_req, res) => {
  res.json({
    mock: MOCK,
    count: (httpMetrics || []).length,
    last5: (httpMetrics || []).slice(-5),
  });
});
app.get("/debug/ws", (_req, res) => {
  res.json({
    mock: MOCK,
    tradier: {
      sessionId: tradierSessionId,
      readyState: tradierWS?.readyState ?? -1,
      subscribed: buildSubscribeSymbols(),
    },
    alpaca: { equities: Array.from(alpacaSubs) },
    buffers: Object.fromEntries(Object.entries(buffers).map(([k, v]) => [k, v.length])),
  });
});

app.get("/health", (_, res) => res.json({ ok: true }));

// --------------------- START ---------------------
server.listen(PORT, () => {
  console.log(`HTTP+WS @ :${PORT}  MOCK=${MOCK ? "on" : "off"}`);
});
