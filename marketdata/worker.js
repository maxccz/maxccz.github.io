// marketdata/worker.js
const TICKERS = ["ZDC.V","SHOP","PTU.V"];
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

// Map symbols to column names in the wide table
const COLS = { "ZDC.V": "ZDC_V", "SHOP": "SHOP", "PTU.V": "PTU_V" };

async function fetchUpsertAndCache(symbolsCsv, env) {
  const url = buildMarketstackUrl(symbolsCsv, env);
  const r = await fetch(url);
  if (!r.ok) return;

  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch { return; }

  const today = new Date().toISOString().slice(0, 10);

  // 1) Ensure date row exists once
  await env.DB.prepare(
    `INSERT OR IGNORE INTO Daily_Gains (date) VALUES (?1)`
  ).bind(today).run(); // create row if missing [web:176]

  // 2) Update per-ticker columns (identifier must be whitelisted)
  for (const row of data.data ?? []) {
    const col = COLS[row.symbol];
    if (!col) continue;
    const sql = `UPDATE Daily_Gains SET ${col} = ?1 WHERE date = ?2`; // identifier inline [web:195]
    await env.DB.prepare(sql).bind(Number(row.close), today).run();
  }

  // 3) Recompute total for the date (sum the known columns)
  const totalSql = `
    UPDATE Daily_Gains
    SET total = COALESCE(ZDC_V,0) + COALESCE(SHOP,0) + COALESCE(CLS_V,0)
    WHERE date = ?1
  `;
  await env.DB.prepare(totalSql).bind(today).run(); // recompute total [web:174]

  // 4) Optional: warm cache with the upstream payload or a derived view
  const cache = await caches.open("eod-cache");
  const cacheKey = `https://edge-cache.local/eod?symbols=${encodeURIComponent(symbolsCsv)}`;
  await cache.put(cacheKey, new Response(text, {
    status: 200,
    headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=86400, s-maxage=86400" }
  }));
}

