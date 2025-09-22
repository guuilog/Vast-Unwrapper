// api/openrtb2.mjs
import {
  unwrapAdmIfWrapper,
  mergeWrapperImpressionsIntoInlineXml,
} from "../lib/resolver.mjs";

export const config = { runtime: "nodejs" };

const DEBUG = process.env.DEBUG === "1";

// ─────────────────────────────────────────────────────────────────────────────
// Dynamic upstream resolver (header/query/env + allowlist)
// ─────────────────────────────────────────────────────────────────────────────
function parseAllowlist(env) {
  const raw = (env?.BID_ENDPOINT_ALLOWLIST || "").trim();
  if (!raw) return null;
  return raw.split(",").map(s => s.trim().toLowerCase()).filter(Boolean);
}
function isHttpsUrl(u) {
  try { return new URL(u).protocol === "https:"; } catch { return false; }
}
function isAllowedHost(urlStr, allowlist) {
  if (!allowlist || allowlist.length === 0) return true;
  try { return allowlist.includes(new URL(urlStr).host.toLowerCase()); } catch { return false; }
}
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
    const tips = "Provide header x-bid-endpoint or set EQUATIV_BID_URL";
    const err = new Error(`No bid endpoint provided. ${tips}`);
    err.statusCode = 400; throw err;
  }
  if (!isHttpsUrl(candidate)) { const e = new Error("Bid endpoint must be https"); e.statusCode = 400; throw e; }
  if (!isAllowedHost(candidate, allowlist)) { const e = new Error("Endpoint host not allowed"); e.statusCode = 403; throw e; }
  return candidate.trim();
}

// ─────────────────────────────────────────────────────────────────────────────
async function readRequestBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) return {};
  try { return JSON.parse(raw); } catch { const e = new Error("Invalid JSON body"); e.statusCode = 400; throw e; }
}

function setCors(res, req) {
  const origin = req.headers.origin || "*";
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Vary", "Origin");
  res.setHeader("Access-Control-Allow-Headers", "content-type, x-bid-endpoint, x-bid-endpoint-b64");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
}

function withTimeout(ms = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), ms);
  return { signal: controller.signal, cancel: () => clearTimeout(id) };
}

// ─────────────────────────────────────────────────────────────────────────────
// Equativ-specific: derive RV (wrapper) URL from nurl for Inline-only bids
//  - ssb-use1.smartadserver.com → use1.smartadserver.com
//  - vastid = bidid, networkId = bidnwid
// ─────────────────────────────────────────────────────────────────────────────
function deriveEquativRvUrlFromNurl(nurl) {
  try {
    const u = new URL(nurl);
    const host = u.host.replace(/^ssb-/, "");
    const vastid = u.searchParams.get("bidid");
    const networkId = u.searchParams.get("bidnwid");
    if (!vastid || !networkId) return null;
    return `https://${host}/rv?vastid=${encodeURIComponent(vastid)}&networkId=${encodeURIComponent(networkId)}`;
  } catch {
    return null;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Handler
// ─────────────────────────────────────────────────────────────────────────────
export default async function handler(req, res) {
  try {
    setCors(res, req);
  } catch {}
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

  let upstreamUrl;
  try { upstreamUrl = getBidEndpoint(req); }
  catch (e) { return res.status(e.statusCode || 500).json({ error: { code: String(e.statusCode || 500), message: e.message } }); }

  let bodyJson;
  try { bodyJson = await readRequestBody(req); }
  catch (e) { return res.status(e.statusCode || 400).json({ error: { code: String(e.statusCode || 400), message: e.message } }); }

  const timeoutMs = Number(process.env.UPSTREAM_TIMEOUT_MS || 8000);
  const { signal, cancel } = withTimeout(timeoutMs);

  try {
    // 1) Forward OpenRTB request upstream
    const upstreamResp = await fetch(upstreamUrl, {
      method: "POST",
      signal,
      headers: {
        accept: "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": req.headers["user-agent"] || "VAST-Unwrapper/1.0",
      },
      body: JSON.stringify(bodyJson),
    });

    const ct = upstreamResp.headers.get("content-type") || "application/json; charset=utf-8";
    let bidRespText = await upstreamResp.text();

    // If not JSON, just pass through
    if (!ct.includes("json")) {
      res.status(upstreamResp.status).setHeader("content-type", ct);
      return res.send(bidRespText);
    }

    // Parse JSON
    let bidResp;
    try { bidResp = JSON.parse(bidRespText); }
    catch {
      res.status(upstreamResp.status).setHeader("content-type", ct);
      return res.send(bidRespText);
    }

    // 2) Traverse bids and unwrap/merge as needed
    let anyReplaced = false;
    let anyCacheHit = false;
    let anyMergedWrapperImps = false;

    if (Array.isArray(bidResp?.seatbid)) {
      for (const sb of bidResp.seatbid) {
        for (const b of (sb?.bid || [])) {
          if (typeof b.adm !== "string") continue;

          // Case A: normal unwrap (Wrapper → Inline)
          if (b.adm.includes("<Wrapper")) {
            try {
              const { adm, replaced, depth, cached } = await unwrapAdmIfWrapper(b.adm);
              if (replaced) {
                b.adm = adm;
                anyReplaced = true;
                if (cached === "hit") anyCacheHit = true;
                b.ext = { ...(b.ext || {}), unwrap: { depth, cached } };
              }
            } catch (e) {
              if (DEBUG) console.warn("[unwrap] failed:", e?.message || e);
              b.ext = { ...(b.ext || {}), unwrap: { depth: -1, cached: "n/a", error: "unwrap-failed" } };
            }
          }
          // Case B: Inline-only bid → derive wrapper URL from nurl to merge SSP Impressions
          else if (b.nurl && b.adm.includes("<InLine")) {
            let rvUrl = null;
            try { rvUrl = deriveEquativRvUrlFromNurl(b.nurl); } catch {}
            if (rvUrl) {
              try {
                const mergedXml = await mergeWrapperImpressionsIntoInlineXml(b.adm, rvUrl);
                if (mergedXml && typeof mergedXml === "string") {
                  b.adm = mergedXml;
                  anyMergedWrapperImps = true;
                }
                b.ext = { ...(b.ext || {}), unwrap: { depth: 0, cached: "n/a", mergedWrapperImps: true } };
              } catch (e) {
                if (DEBUG) console.warn("[unwrap:fallback] RV merge failed:", e?.message || e);
                b.ext = { ...(b.ext || {}), unwrap: { depth: 0, cached: "n/a", mergedWrapperImps: false, reason: "rv-fetch-failed" } };
              }
            } else {
              b.ext = { ...(b.ext || {}), unwrap: { depth: 0, cached: "n/a", mergedWrapperImps: false, reason: "rv-url-not-derived" } };
            }
          }
        }
      }
    }

    // 3) Observability headers
    res.setHeader("X-Unwrap", anyReplaced || anyMergedWrapperImps ? "inline" : "passthrough");
    res.setHeader("X-Unwrap-Cache", anyCacheHit ? "hit" : "miss");

    return res.status(upstreamResp.status).json(bidResp);
  } catch (e) {
    const aborted = e?.name === "AbortError";
    const msg = aborted ? `Upstream request timed out after ${timeoutMs}ms` : (e.message || String(e));
    return res.status(aborted ? 504 : 502).json({ error: { code: String(aborted ? 504 : 502), message: msg } });
  } finally {
    cancel();
  }
}
