// va-proxy/api/openrtb2.mjs
// Vercel Serverless API (Node runtime)
// VAST Tag Unwrapper – Dynamic Endpoint Proxy (hardened)
//
// Key features:
// - Dynamic upstream endpoint via headers (x-bid-endpoint / x-bid-endpoint-b64)
// - Optional fallback via EQUATIV_BID_URL env var
// - SSRF defenses: HTTPS-only, no credentials, host allowlist, DNS resolution,
//   reject private/link-local/loopback/multicast IPs, manual redirect validation
// - Request timeout (UPSTREAM_TIMEOUT_MS), response size cap (MAX_BODY_BYTES)
// - CORS for browser debugging (tighten in production)

import dns from "node:dns/promises";
import net from "node:net";

export const config = {
  runtime: "nodejs",
};

// ─────────────────────────────────────────────────────────────────────────────
// Utility: send JSON error with status
function sendError(res, status, message) {
  res.status(status).json({ error: { code: String(status), message } });
}

// ─────────────────────────────────────────────────────────────────────────────
// Body parsing (guard in case Vercel doesn't pre-parse)
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
  if (req.body && typeof req.body === "object") return req.body;
  const raw = await readRawBody(req);
  if (!raw) return {};
  try { return JSON.parse(raw); }
  catch {
    const err = new Error("Request body is not valid JSON.");
    err.statusCode = 400;
    throw err;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// CORS (tighten in production: pin to known origins)
function setCors(res, req) {
  const origin = req.headers.origin || "*";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "content-type, x-bid-endpoint, x-bid-endpoint-b64"
  );
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
}

// ─────────────────────────────────────────────────────────────────────────────
// Allowlist + URL checks
function parseAllowlist(env) {
  const raw = (env?.BID_ENDPOINT_ALLOWLIST || "").trim();
  if (!raw) return null;
  return raw.split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
}

function isHttpsUrlStrict(u) {
  let url;
  try { url = new URL(u); } catch { return null; }
  if (url.protocol !== "https:") return null;
  if (url.username || url.password) return null; // forbid creds in URL
  return url;
}

function isPrivateLikeIp(ip) {
  if (net.isIP(ip) === 0) return false;

  if (net.isIP(ip) === 4) {
    const parts = ip.split(".").map(Number);
    const n = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3];
    const inRange = (start, mask) => (n & mask) === start;
    if (inRange(0x0A000000, 0xFF000000)) return true;   // 10.0.0.0/8
    if (inRange(0xAC100000, 0xFFF00000)) return true;   // 172.16.0.0/12
    if (inRange(0xC0A80000, 0xFFFF0000)) return true;   // 192.168.0.0/16
    if (inRange(0x7F000000, 0xFF000000)) return true;   // 127.0.0.0/8 loopback
    if (inRange(0xA9FE0000, 0xFFFF0000)) return true;   // 169.254.0.0/16 link-local
    if (inRange(0x64400000, 0xFFC00000)) return true;   // 100.64.0.0/10 CGNAT
    const topNibble = n & 0xF0000000;
    if (topNibble === 0xE0000000) return true;          // 224.0.0.0/4 multicast
    if (topNibble === 0xF0000000) return true;          // 240.0.0.0/4 reserved
    return false;
  }

  if (net.isIP(ip) === 6) {
    const lower = ip.toLowerCase();
    if (lower === "::1") return true;                   // loopback
    if (lower.startsWith("fe8") || lower.startsWith("fe9") ||
        lower.startsWith("fea") || lower.startsWith("feb")) return true; // fe80::/10
    if (lower.startsWith("fc") || lower.startsWith("fd")) return true;   // fc00::/7 unique-local
    if (lower.startsWith("ff")) return true;            // ff00::/8 multicast
    return false;
  }

  return false;
}

async function resolveAndCheckHost(urlObj) {
  const host = urlObj.hostname;
  if (net.isIP(host)) throw Object.assign(new Error("IP literals are not allowed as hosts."), { statusCode: 403 });

  const addrs = new Set();
  try { for (const rr of await dns.resolve4(host, { ttl: false })) addrs.add(rr); } catch {}
  try { for (const rr of await dns.resolve6(host, { ttl: false })) addrs.add(rr); } catch {}
  if (addrs.size === 0) throw Object.assign(new Error("Host did not resolve to any IPs."), { statusCode: 502 });

  for (const ip of addrs) {
    if (isPrivateLikeIp(ip)) {
      throw Object.assign(new Error(`Resolved IP ${ip} is private/link-local/loopback/reserved.`), { statusCode: 403 });
    }
  }
  return [...addrs];
}

function isAllowedHost(urlObj, allowlist) {
  if (!allowlist || allowlist.length === 0) return true;
  return allowlist.includes(urlObj.host.toLowerCase()) || allowlist.includes(urlObj.hostname.toLowerCase());
}

// Timeout helper
function withTimeout(ms = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  return { signal: controller.signal, cancel: () => clearTimeout(id) };
}

// Read response body with size limit; supports Web streams and Node streams
function isNodeReadableStream(obj) {
  return obj && typeof obj.on === "function" && typeof obj.pipe === "function";
}

async function readBodyLimited(resp, maxBytes = 1_500_000) {
  // Content-Length pre-check (if present)
  const cl = resp.headers.get("content-length");
  if (cl && Number(cl) > maxBytes) {
    const err = new Error(`Upstream response too large (Content-Length ${cl} > ${maxBytes})`);
    err.statusCode = 502;
    throw err;
  }

  const body = resp.body;
  if (!body) return Buffer.alloc(0);

  // Node stream path (rare with WHATWG fetch, but handle it)
  if (isNodeReadableStream(body)) {
    return new Promise((resolve, reject) => {
      let total = 0;
      const chunks = [];
      body.on("data", (c) => {
        const b = Buffer.from(c);
        total += b.length;
        if (total > maxBytes) {
          body.destroy();
          return reject(Object.assign(new Error("Upstream response too large"), { statusCode: 502 }));
        }
        chunks.push(b);
      });
      body.on("end", () => resolve(Buffer.concat(chunks)));
      body.on("error", (e) => reject(e));
    });
  }

  // Web ReadableStream path (Vercel/Node fetch)
  const reader = body.getReader();
  let total = 0;
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      total += value.byteLength;
      if (total > maxBytes) {
        try { await reader.cancel(); } catch {}
        const err = new Error("Upstream response too large");
        err.statusCode = 502;
        throw err;
      }
      chunks.push(Buffer.from(value));
    }
  }
  return Buffer.concat(chunks);
}

// ─────────────────────────────────────────────────────────────────────────────
// Endpoint resolution with all checks
function getCandidateEndpoint(req, env = process.env) {
  const b64 = req.headers?.["x-bid-endpoint-b64"];
  const header = req.headers?.["x-bid-endpoint"];
  const qp = req.query?.bidEndpoint;

  const fromClient =
    (b64 ? Buffer.from(String(b64), "base64").toString("utf8")
         : header ? String(header)
         : (env?.NODE_ENV === "production" ? null : (qp ? String(qp) : null)));

  return fromClient || env?.EQUATIV_BID_URL || null;
}

async function resolveSecureEndpoint(req, env = process.env) {
  const allowlist = parseAllowlist(env);
  const cand = getCandidateEndpoint(req, env);

  if (!cand) throw Object.assign(new Error("No bid endpoint provided."), { statusCode: 400 });

  const url = isHttpsUrlStrict(cand);
  if (!url) throw Object.assign(new Error("Bid endpoint must be https and have no credentials."), { statusCode: 400 });

  if (!isAllowedHost(url, allowlist)) {
    throw Object.assign(new Error("Bid endpoint host is not in allowlist."), { statusCode: 403 });
  }

  await resolveAndCheckHost(url);
  return url;
}

// Manual-redirect fetch with re-validation per hop
async function secureFetch(initialUrl, fetchOpts, { maxRedirects = 3, timeoutMs = 8000 } = {}) {
  let current = initialUrl;
  let redirects = 0;
  const { signal, cancel } = withTimeout(timeoutMs);

  try {
    for (;;) {
      const resp = await fetch(current, { ...fetchOpts, redirect: "manual", signal });

      if ([301, 302, 303, 307, 308].includes(resp.status)) {
        if (redirects >= maxRedirects) throw Object.assign(new Error("Too many redirects"), { statusCode: 502 });
        const loc = resp.headers.get("location");
        if (!loc) throw Object.assign(new Error("Redirect without Location header"), { statusCode: 502 });

        const nextUrl = new URL(loc, current);
        if (!isHttpsUrlStrict(String(nextUrl))) {
          throw Object.assign(new Error("Redirect to non-https or URL with credentials is blocked"), { statusCode: 403 });
        }
        await resolveAndCheckHost(nextUrl);

        current = nextUrl;
        redirects++;
        continue;
      }

      return resp;
    }
  } finally {
    cancel();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Main handler
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

  // 1) Resolve + validate endpoint
  let upstreamUrl;
  try {
    const url = await resolveSecureEndpoint(req, process.env);
    upstreamUrl = url.toString();
  } catch (e) {
    return sendError(res, e.statusCode || 400, e.message || "Invalid endpoint");
  }

  // 2) Parse JSON body
  let bodyJson;
  try {
    bodyJson = await getRequestJson(req);
  } catch (e) {
    return sendError(res, e.statusCode || 400, e.message || "Invalid JSON body");
  }

  // 3) Fetch upstream with manual redirects, timeout, and size limit
  const timeoutMs = Number(process.env.UPSTREAM_TIMEOUT_MS || 8000);
  const maxBytes = Number(process.env.MAX_BODY_BYTES || 1_500_000);

  try {
    const upstreamResp = await secureFetch(new URL(upstreamUrl), {
      method: "POST",
      headers: {
        accept: "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": req.headers["user-agent"] || "VAST-Unwrapper/1.0",
      },
      body: JSON.stringify(bodyJson),
    }, { maxRedirects: 3, timeoutMs });

    const ct = upstreamResp.headers.get("content-type") || "application/json; charset=utf-8";
    if (!ct.toLowerCase().includes("application/json")) {
      return sendError(res, 502, `Upstream returned unexpected content-type: ${ct}`);
    }

    const buf = await readBodyLimited(upstreamResp, maxBytes);
    res.status(upstreamResp.status);
    res.setHeader("content-type", ct);
    res.send(buf);
  } catch (e) {
    const aborted = e?.name === "AbortError";
    const sc = e.statusCode || (aborted ? 504 : 502);
    sendError(res, sc, aborted ? `Upstream request timed out after ${timeoutMs}ms` : (e.message || "Upstream request failed"));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ENV Cheatsheet (recommended)
//   BID_ENDPOINT_ALLOWLIST=ssb-use1.smartadserver.com,ssb-us-east.smartadserver.com
//   EQUATIV_BID_URL=https://.../api/bid?callerId=241  # optional fallback
//   UPSTREAM_TIMEOUT_MS=8000
//   MAX_BODY_BYTES=1500000
//
// Production hardening tips:
// - Disable query-param endpoint by keeping NODE_ENV=production (already enforced).
// - Pin CORS to your internal tools’ origins.
// - Add rate-limiting (e.g., Upstash Redis) per IP if public.
// - Deploy behind a private gateway if possible.
// - Log rejects for visibility (add your own logger).
