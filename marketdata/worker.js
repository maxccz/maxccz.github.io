
const ORIGIN = "https://m-zhang.me";                // Allowed browser origin for CORS [web:311]
const TTL = 86400;

// Simple validation: comma-separated tickers of A–Z, digits, ., -
const SYMBOL_RE = /^[A-Z][A-Z0-9.-]{0,6}(,[A-Z][A-Z0-9.-]{0,6})*$/;  // e.g., AAPL,MSFT [web:369]

// CORS helper
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

// Build Marketstack EOD URL for one or many symbols
function buildMarketstackUrl(symbolsCsv, env) {
  const base = "https://api.marketstack.com/v1/eod";
  const params = new URLSearchParams({
    access_key: env.MARKETSTACK_KEY,   // add via Worker Secret [web:369]
    symbols: symbolsCsv,               // supports comma-separated list [web:369]
    limit: "1",                        // latest EOD record per symbol [web:369]
    sort: "DESC"                       // newest first [web:369]
  });
  return `${base}?${params.toString()}`;
}

export default {
  async fetch(request, env) {
    // Preflight
    if (request.method === "OPTIONS") {
      return new Response("", { headers: corsHeaders(request) });
    }
    if (request.method !== "GET") {
      return new Response("Method Not Allowed", { status: 405, headers: corsHeaders(request) });
    }

    const url = new URL(request.url);
    const symbolsCsv = (url.searchParams.get("symbols") || "AAPL").toUpperCase(); // e.g., "AAPL,MSFT" [web:369]
    const refresh = (url.searchParams.get("refresh") || "false").toLowerCase() === "true"; // force upstream once [web:293]

    // Validate symbols format
    if (!SYMBOL_RE.test(symbolsCsv)) {
      return new Response(JSON.stringify({ error: "Invalid symbols" }), {
        status: 400,
        headers: { "Content-Type": "application/json", ...corsHeaders(request) },
      });
    }

    // Cache namespace and key
    const cache = await caches.open("eod-cache"); // private namespace for clarity [web:293]
    const cacheKey = `https://edge-cache.local/eod?symbols=${encodeURIComponent(symbolsCsv)}`; // absolute URL string [web:293]

    // Serve from cache unless explicitly refreshing
    const cached = await cache.match(cacheKey);
    if (cached && !refresh) {
      return new Response(cached.body, {
        status: cached.status,
        headers: {
          ...Object.fromEntries(cached.headers),
          ...corsHeaders(request),
          "X-Worker-Cache": "HIT",
        },
      });
    }

    // Build upstream request to Marketstack
    const upstreamUrl = buildMarketstackUrl(symbolsCsv, env); // includes access_key [web:369]
    const upstreamResp = await fetch(upstreamUrl);
    const bodyText = await upstreamResp.text();

    // Detect provider errors without poisoning cache
    // Marketstack error example: { "error": { "code": 429, "message": "rate_limit_reached" } } [web:370]
    const isJson = upstreamResp.headers.get("Content-Type")?.includes("application/json");
    let providerError = !upstreamResp.ok;
    if (isJson) {
      try {
        const parsed = JSON.parse(bodyText);
        if (parsed && typeof parsed === "object" && parsed.error){
          providerError = true; // treat any "error" object as non-cacheable [web:370]
        }
      } catch {
        // Ignore parse errors; fall back to status check
      }
    }

    // On error or rate-limit: fall back to cached data if present
    if (providerError) {
      if (cached) {
        return new Response(cached.body, {
          status: cached.status,
          headers: {
            ...Object.fromEntries(cached.headers),
            ...corsHeaders(request),
            "X-Worker-Cache": "HIT",
            "X-Worker-Notice": "Upstream limited/error; served cached data",
          },
        });
      }
      // No cache available — return a clear JSON error without caching it
      return new Response(JSON.stringify({ error: "Upstream limited or error; try later" }), {
        status: upstreamResp.status === 429 ? 429 : 504,
        headers: { "Content-Type": "application/json", ...corsHeaders(request) },
      });
    }

    // Success: cache and return
    const resp = new Response(bodyText, {
      status: upstreamResp.status,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": `public, max-age=${TTL}, s-maxage=${TTL}`,
        ...corsHeaders(request),
        "X-Worker-Cache": "MISS",
      },
    });
    await cache.put(cacheKey, resp.clone()); // store only good payloads [web:293]
    return resp;
  },
};
