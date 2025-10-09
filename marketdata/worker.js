const TICKERS = ["SHOP","PTU.V"];
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

  async fetch(_req, _env) {
    return new Response("marketdata worker ok");
  }
};

function isBusinessDay(d) {
  const dow = d.getUTCDay();
  return dow !== 0 && dow !== 6; 
}

const COLS = {"SHOP": "SHOP", "PTU.V": "PTU_V" };

async function fetchUpsertAndCache(symbolsCsv, env) {
  const url = buildMarketstackUrl(symbolsCsv, env);
  const r = await fetch(url);
  if (!r.ok) return;

  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch { return; }

  const today = new Date().toISOString().slice(0, 10);

  await env.DB.prepare(
    `INSERT OR IGNORE INTO Daily_Gains (date) VALUES (?1)`
  ).bind(today).run(); 

  for (const row of data.data ?? []) {
    const col = COLS[row.symbol];
    if (!col) continue;
    const sql = `UPDATE Daily_Gains SET ${col} = ?1 WHERE date = ?2`; 
    await env.DB.prepare(sql).bind(Number(row.close), today).run();
  }

  const totalSql = `
    UPDATE Daily_Gains
    SET total = COALESCE(SHOP,0) + COALESCE(PTU_V,0)
    WHERE date = ?1
  `;
  await env.DB.prepare(totalSql).bind(today).run(); 

  const cache = await caches.open("eod-cache");
  const cacheKey = `https://edge-cache.local/eod?symbols=${encodeURIComponent(symbolsCsv)}`;
  await cache.put(cacheKey, new Response(text, {
    status: 200,
    headers: { "Content-Type": "application/json", "Cache-Control": "public, max-age=86400, s-maxage=86400" }
  }));
}

