// va-proxy/api/openrtb2.mjs
// Vercel Serverless API (Node 18+)
// ─────────────────────────────────────────────────────────────────────────────
// POST /va-proxy/api/openrtb2
// - Accepts an OpenRTB JSON body
// - Resolves the upstream bidder endpoint dynamically (no hardcoded default)
//     Priority:
//       1) Header: x-bid-endpoint-b64 (base64 URL)
//       2) Header: x-bid-endpoint      (full URL)
//       3) Query : ?bidEndpoint=...    (debug only)
//       4) Env   : EQUATIV_BID_URL     (fallback if set)
// - Optional allowlist via BID_ENDPOINT_ALLOWLIST (comma-separated hosts)
// - Proxies the body to the upstream, returns upstream response body/status
// - Adds CORS for browser testing; do NOT expose headers to untrusted clients
// ─────────────────────────────────────────────────────────────────────────────

export const config = {
  runtime: "nodejs18.x",
};

// Utility: send JSON error with status
function sendError(res, status, message) {
  res.status(status).json({ error: { code: String(status), message } });
}

// Utility: safe JSON.stringify for logging (not used in response)
function safeStringify(obj) {
  try { return JSON.stringify(obj); } catch { return "[unserializable]"; }
}

// ─────────────────────────────────────────────────────────────────────────────
// Allowlist / URL validation
// ─────────────────────────────────────────────────────────────────────────────
function parseAllowlist(env) {
  const raw = (env?.BID_ENDPOINT_ALLOWLIST || "").trim();
  if (!raw) return null;
  return raw
    .split(",")
    .map(s => s.trim().toLowerCase())
    .filter(Boolean);
}

function isHttpsUrl(u) {
  try {
    const url = new URL(u);
    return url.protocol === "https:";
  } catch {
    return false;
  }
}

function isAllowedHost(urlStr, allowlist) {
  if (!allowlist || allowlist.length === 0) return true;
  try {
    const { host } = new URL(urlStr);
    return allowlist.includes(host.toLowerCase());
  } catch {
    return false;
  }
}

/**
 * Resolve the upstream OpenRTB endpoint from:
 * 1) Header: x-bid-endpoint-b64 (base64-encoded full URL)
 * 2) Header: x-bid-endpoint (full URL)
 * 3) Query:  ?bidEndpoint=...   (debugging only)
 * 4) Env:    EQUATIV_BID_URL
 *
 * Throws with { statusCode } if invalid / missing.
 */
function getBidEndpoint(req, env = process.env) {
  const allowlist = parseAllowlist(env);

  let candidate =
    (req.headers?.["x-bid-endpoint-b64"]
      ? Buffer.from(String(req.headers["x-bid-endpoint-b64"]), "base64").toString("utf8")
      : null) ||
    (req.headers?.["x-bid-endpoint"] ? String(req.headers["x-bid-endpoint"]) : null) ||
    (req.query?.bidEndpoint ? String(req.query.bidEndpoint) : null) ||
    (env?.EQUATIV_BID_URL || null);

  if (!candidate) {
    const tips = [
      "Provide header 'x-bid-endpoint: https://host/path?callerId=241'",
      "Or set env EQUATIV_BID_URL",
      "Optional: 'x-bid-endpoint-b64' (base64) or query '?bidEndpoint=' (debug)"
    ].join(" | ");
    const err = new Error(`No bid endpoint provided. ${tips}`);
    err.statusCode = 400;
    throw err;
  }

  candidate = candidate.trim();

  if (!isHttpsUrl(candidate)) {
    const err = new Error("Bid endpoint must be a valid https URL.");
    err.statusCode = 400;
    throw err;
  }

  if (!isAllowedHost(candidate, allowlist)) {
    const err = new Error("Bid endpoint host is not in the allowlist.");
    err.statusCode = 403;
    throw err;
  }

  return candidate;
}

// ─────────────────────────────────────────────────────────────────────────────
// Body parsing helpers
// (Vercel usually parses JSON, but we guard for raw bodies / other CTs)
// ─────────────────────────────────────────────────────────────────────────────
async function readRawBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.setEncoding("utf8");
    req.on("data", chunk => { data += chunk; });
    req.on("end", () => resolve(data));
    req.on("error", reject);
  });
}

async function getRequestJson(req) {
  // If Vercel already parsed a JSON body, it'll be on req.body as object
  if (req.body && typeof req.body === "object") return req.body;

  // Otherwise, try parse raw
  const raw = await readRawBody(req);
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch {
    // Not JSON; the upstream may still accept text, so return raw string marker
    // For OpenRTB you almost always want JSON, so we error out instead:
    const err = new Error("Request body is not valid JSON.");
    err.statusCode = 400;
    throw err;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// CORS
// ─────────────────────────────────────────────────────────────────────────────
function setCors(res, req) {
  const origin = req.headers.origin || "*";
  // In production, set a specific origin or a small allowlist
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "content-type, x-bid-endpoint, x-bid-endpoint-b64"
  );
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
}

// ─────────────────────────────────────────────────────────────────────────────
// Timeout helper for fetch (AbortController)
// ─────────────────────────────────────────────────────────────────────────────
function withTimeout(ms = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  return { signal: controller.signal, cancel: () => clearTimeout(id) };
}

// ─────────────────────────────────────────────────────────────────────────────
// Main handler
// ─────────────────────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  setCors(res, req);

  if (req.method === "OPTIONS") {
    res.status(204).end();
    return;
  }

  if (req.method !== "POST") {
    sendError(res, 405, "Method Not Allowed. Use POST.");
    return;
  }

  let upstreamUrl;
  try {
    upstreamUrl = getBidEndpoint(req, process.env);
  } catch (e) {
    sendError(res, e.statusCode || 500, e.message || "Invalid endpoint");
    return;
  }

  let bodyJson;
  try {
    bodyJson = await getRequestJson(req);
  } catch (e) {
    sendError(res, e.statusCode || 400, e.message || "Invalid JSON body");
    return;
  }

  // Prepare upstream fetch
  const timeoutMs = Number(process.env.UPSTREAM_TIMEOUT_MS || 8000);
  const { signal, cancel } = withTimeout(timeoutMs);

  try {
    const upstreamResp = await fetch(upstreamUrl, {
      method: "POST",
      signal,
      headers: {
        accept: "application/json, text/plain, */*",
        "content-type": "application/json",
        // Optionally forward UA; don't forward the x-bid-* headers upstream
        "user-agent": req.headers["user-agent"] || "VAST-Unwrapper/1.0",
        // If you want to forward X-Forwarded-For, uncomment the next line:
        // "x-forwarded-for": req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "",
      },
      body: JSON.stringify(bodyJson),
    });

    // Stream or buffer response. Here we buffer then pass through as-is.
    const text = await upstreamResp.text();

    // Pass through status and content-type (if present)
    const ct = upstreamResp.headers.get("content-type") || "application/json; charset=utf-8";
    res.status(upstreamResp.status);
    res.setHeader("content-type", ct);

    // If you have wrapper→inline logic, you'd transform `text` here
    // and then send the modified payload instead. For now we return upstream.
    res.send(text);
  } catch (e) {
    const aborted = e?.name === "AbortError";
    const msg = aborted
      ? `Upstream request timed out after ${timeoutMs}ms`
      : `Upstream request failed: ${e.message || e}`;
    sendError(res, aborted ? 504 : 502, msg);
  } finally {
    cancel();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Notes:
// - To restrict which hosts can be proxied, set:
//     BID_ENDPOINT_ALLOWLIST="ssb-use1.smartadserver.com,ssb-us-east.smartadserver.com"
// - Optional fallback (used only if no header/query is provided):
//     EQUATIV_BID_URL="https://ssb-use1.smartadserver.com/api/bid?callerId=241"
// - For browser use, keep CORS tight by pinning Access-Control-Allow-Origin.
// - Do not expose this proxy to untrusted clients; consider SSRF defenses
//   (DNS resolve + private IP range block) if opening publicly.
// ─────────────────────────────────────────────────────────────────────────────
