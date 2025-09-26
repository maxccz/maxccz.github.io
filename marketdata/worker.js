const ORIGIN = "https://m-zhang.me";
const TTL = 86400; 
const SYMBOL_RE = /^[A-Z][A-Z0-9.-]{0,6}(,[A-Z][A-Z0-9.-]{0,6})*$/;
const INTERVALS = new Set(["1min","5min","15min","30min","60min"]);

function corsHeaders(request) {
  const reqOrigin = request.headers.get("Origin") || "";
  const allow = reqOrigin === ORIGIN ? reqOrigin : "";
  return {
    "Access-Control-Allow-Origin": allow,
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

async function fetchAndCacheIntraday(symbol, interval, outputsize, key, env) {
  const upstream = `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&outputsize=${encodeURIComponent(outputsize)}&datatype=json&apikey=${env.ALPHAVANTAGE_KEY}`;
  const r = await fetch(upstream);
  const body = await r.text();
  const resp = new Response(body, {
    status: r.status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${TTL}, s-maxage=${TTL}`,
    },
  });
  await caches.default.put(new Request(key, { cf: { cacheEverything: true } }), resp.clone());
  return resp;
}

export default {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response("", { headers: corsHeaders(request) });
    }
    if (request.method !== "GET") {
      return new Response("Method Not Allowed", { status: 405, headers: corsHeaders(request) });
    }

    const url = new URL(request.url);
    const symbols = url.searchParams.get("symbols") || "AAPL";
    const interval = url.searchParams.get("interval") || "5min";
    const outputsize = url.searchParams.get("outputsize") || "compact";

    if (!SYMBOL_RE.test(symbols)) {
      return new Response("Invalid symbols", { status: 400, headers: corsHeaders(request) });
    }
    if (!INTERVALS.has(interval)) {
      return new Response("Invalid interval", { status: 400, headers: corsHeaders(request) });
    }

    const symbol = symbols.split(",")[0].trim();

    const cacheKey = `https://cache/intraday?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}`;
    const cache = caches.default;
    const cached = await cache.match(new Request(cacheKey));
    if (cached) {
      return new Response(cached.body, {
        status: cached.status,
        headers: {
          ...Object.fromEntries(cached.headers),
          ...corsHeaders(request),
        },
      });
    }

    const resp = await fetchAndCacheIntraday(symbol, interval, outputsize, cacheKey, env);
    return new Response(await resp.text(), {
      status: resp.status,
      headers: {
        ...Object.fromEntries(resp.headers),
        ...corsHeaders(request),
      },
    });
  },

  async scheduled(event, env, ctx) {
    const symbols = ["AAPL","MSFT","GOOGL","AMZN","META"];
    const interval = "5min";
    const outputsize = "compact";

    const now = new Date();
    const hourUTC = now.getUTCHours();
    if (!(hourUTC === 20 || hourUTC === 21)) {
      return;
    }

    for (const symbol of symbols) {
      const cacheKey = `https://cache/intraday?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}`;
      ctx.waitUntil(fetchAndCache
