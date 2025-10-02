// marketdata/worker.js
const TICKERS = ["ZDC.V","SHOP","CLS.V"];
const TTL = 86400;

function buildMarketstackUrl(symbolsCsv, env) {
  const base = "https://api.marketstack.com/v1/eod/latest";
  const params = new URLSearchParams({
    access_key: env.MARKETSTACK_KEY,
    symbols: symbolsCsv,
    sort: "DESC",
    groupby: "symbol",
  });
  return `${base}?${params.toString()}`;
}

export default {
  async scheduled(event, env, ctx) {
    if (!isBusinessDay(new Date())) return;
    const batchSize = 50;
    for (let i = 0; i < TICKERS.length; i += batchSize) {
      const symbolsCsv = TICKERS.slice(i, i + batchSize).join(",");
      ctx.waitUntil(fetchUpsertAndCache(symbolsCsv, env));
    }
  },

  // Optional: keep fetch disabled or readonly for testing
  async fetch(_req, _env) {
    return new Response("marketdata worker ok");
  }
};

function isBusinessDay(d) {
  const dow = d.getUTCDay();
  return dow !== 0 && dow !== 6; // add holidays later
}

async function fetchUpsertAndCache(symbolsCsv, env) {
  const url = buildMarketstackUrl(symbolsCsv, env);
  const r = await fetch(url);
  if (!r.ok) return;
  const text = await r.text();

  // Upsert into D1
  let payload;
  try { payload = JSON.parse(text); } catch { return; }
  for (const row of payload.data ?? []) {
    const symbol = row.symbol;
    const date = String(row.date).slice(0, 10);
    const close = Number(row.close);
    const volume = Number(row.volume ?? 0);
    await env.DB.prepare(
      `INSERT INTO quotes (symbol, date, close, volume)
       VALUES (?1, ?2, ?3, ?4)
       ON CONFLICT(symbol, date) DO UPDATE SET
         close = excluded.close,
         volume = excluded.volume`
    ).bind(symbol, date, close, volume).run();
  }

  // Optional: warm a synthetic edge cache key for the API worker
  const cache = await caches.open("eod-cache");
  const cacheKey = `https://edge-cache.local/eod?symbols=${encodeURIComponent(symbolsCsv)}`;
  await cache.put(cacheKey, new Response(text, {
    status: 200,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${TTL}, s-maxage=${TTL}`,
    }
  }));
}
