const httpMetrics = [];
export { httpMetrics };

const origFetch = global.fetch;
global.fetch = async function patchedFetch(input, init = {}) {
  const url = typeof input === "string" ? input : input?.url;
  const method = (init?.method || "GET").toUpperCase();
  const t0 = Date.now();
  try {
    const res = await origFetch(input, init);
    const ms = Date.now() - t0;
    httpMetrics.push({ ts: Date.now(), method, url, status: res.status, ms });
    console.log(`[HTTP OUT] ${method} ${url} -> ${res.status} ${ms}ms`);
    return res;
  } catch (err) {
    const ms = Date.now() - t0;
    httpMetrics.push({ ts: Date.now(), method, url, error: err?.message, ms });
    console.error(`[HTTP OUT] ${method} ${url} -> ERROR ${err?.message}`);
    throw err;
  }
};
