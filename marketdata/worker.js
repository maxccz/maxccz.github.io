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

function buildCacheKey(symbol, interval, outputsize) {
  return new Request(`/intraday?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}`, {
    cf: { cacheEverything: true }
  });
}

async function fetchAlpha(symbol, interval, outputsize, env) {
  const url = `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&outputsize=${encodeURIComponent(outputsize)}&datatype=json&apikey=${env.ALPHAVANTAGE_KEY}`;
  return fetch(url);
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
    const refresh = (url.searchParams.get("refresh") || "false").toLowerCase() === "true";

    if (!SYMBOL_RE.test(symbols)) {
      return new Response("Invalid symbols", { status: 400, headers: corsHeaders(request) });
    }
    if (!INTERVALS.has(interval)) {
      return new Response("Invalid interval", { status: 400, headers: corsHeaders(request) });
    }

    const symbol = symbols.split(",")[0].trim();
    const cache = caches.default;
    const cacheKey = `https://cache.example/intraday?symbol=${symbol}&interval=${interval}&outputsize=${outputsize}`;

    const cached = await cache.match(cacheKey);
    if (cached && !refresh) {
      return new Response(cached.body, {
        status: cached.status,
        headers: {
          ...Object.fromEntries(cached.headers),
          ...corsHeaders(request),
        },
      });
    }

    const upstreamResp = await fetchAlpha(symbol, interval, outputsize, env);
    const text = await upstreamResp.text();

    const isJson = upstreamResp.headers.get("Content-Type")?.includes("application/json");
    let containsLimitNote = false;
    if (isJson) {
      try {
        const parsed = JSON.parse(text);
        containsLimitNote = typeof parsed?.Note === "string";
      } catch {}
    }

    if (!upstreamResp.ok || containsLimitNote) {
      if (cached) {
        return new Response(cached.body, {
          status: cached.status,
          headers: {
            ...Object.fromEntries(cached.headers),
            ...corsHeaders(request),
            "X-Worker-Notice": "Upstream limited; served cached data",
          },
        });
      }
      return new Response("Upstream limited or error; try later or remove refresh", {
        status: 504,
        headers: corsHeaders(request),
      });
    }

    const resp = new Response(text, {
      status: upstreamResp.status,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": `public, max-age=${TTL}, s-maxage=${TTL}`,
        ...corsHeaders(request),
      },
    });
    await cache.put(cacheKey, resp.clone());
    return resp;
  },
};
