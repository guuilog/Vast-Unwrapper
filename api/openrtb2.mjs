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

  if (!candidate) { const e = new Error("No bid endpoint provided"); e.statusCode = 400; throw e; }
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
// Equativ RV derivation (tolerant)
//  - ssb-use1.smartadserver.com → use1.smartadserver.com
//  - vastid = bidid, networkId = bidnwid
// ─────────────────────────────────────────────────────────────────────────────
function deriveEquativRvUrlFromNurl(nurl) {
  try {
    const u = new URL(nurl);
    let host = u.host;
    if (host.startsWith("ssb-")) host = host.slice(4);
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
  try { setCors(res, req); } catch {}
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

  const debug = DEBUG || String(req.query?.debug || "").toLowerCase() === "1";

  let upstreamUrl;
  try { upstreamUrl = getBidEndpoint(req); }
  catch (e) { return res.status(e.statusCode || 500).json({ error: { code: String(e.statusCode || 500), message: e.message } }); }

  let bodyJson;
  try { bodyJson = await readRequestBody(req); }
  catch (e) { return res.status(e.statusCode || 400).json({ error: { code: String(e.statusCode || 400), message: e.message } }); }

  const timeoutMs = Number(process.env.UPSTREAM_TIMEOUT_MS || 8000);
  const { signal, cancel } = withTimeout(timeoutMs);

  try {
    // 1) Upstream OpenRTB
    const upstreamResp = await fetch(upstreamUrl, {
      method: "POST",
      signal,
      headers: {
        accept: "application/json, text/plain, */*",
        "content-type": "application/json",
        "user-agent": req.headers["user-agent"] || "VAST-Unwrapper/1.2",
      },
      body: JSON.stringify(bodyJson),
    });

    const ct = upstreamResp.headers.get("content-type") || "application/json; charset=utf-8";
    let bidRespText = await upstreamResp.text();

    if (!ct.includes("json")) {
      res.status(upstreamResp.status).setHeader("content-type", ct);
      return res.send(bidRespText);
    }

    let bidResp;
    try { bidResp = JSON.parse(bidRespText); }
    catch { res.status(upstreamResp.status).setHeader("content-type", ct); return res.send(bidRespText); }

    // 2) Unwrap / Merge
    let anyReplaced = false;
    let anyCacheHit = false;
    let anyMergedWrapperImps = false;
    let lastDebug = { mode: "passthrough" };

    if (Array.isArray(bidResp?.seatbid)) {
      for (const sb of bidResp.seatbid) {
        for (const b of (sb?.bid || [])) {
          if (typeof b.adm !== "string") continue;

          // ── STEP A: unwrap if it's a Wrapper
          let didUnwrap = false;
          if (b.adm.includes("<Wrapper")) {
            try {
              const { adm, replaced, depth, cached } = await unwrapAdmIfWrapper(b.adm);
              if (replaced) {
                b.adm = adm;
                didUnwrap = true;
                anyReplaced = true;
                if (cached === "hit") anyCacheHit = true;
                b.ext = { ...(b.ext || {}), unwrap: { depth, cached } };
                lastDebug = { mode: "unwrap", depth, cached };
              } else {
                lastDebug = { mode: "unwrap", depth: 0, cached: "miss" };
              }
            } catch (e) {
              b.ext = { ...(b.ext || {}), unwrap: { depth: -1, cached: "n/a", error: "unwrap-failed" } };
              lastDebug = { mode: "unwrap", error: e?.message || String(e) };
            }
          }

          // ── STEP B: ALWAYS try RV merge if final adm is Inline and nurl present
          if (b.nurl && typeof b.adm === "string" && b.adm.includes("<InLine")) {
            const rvUrl = deriveEquativRvUrlFromNurl(b.nurl);
            if (rvUrl) {
              try {
                const beforeXml = b.adm;
                const { xml: mergedXml, stats } = await mergeWrapperImpressionsIntoInlineXml(beforeXml, rvUrl, { debug });
                const changed = Boolean(mergedXml && typeof mergedXml === "string" && mergedXml !== beforeXml);

                if (changed) {
                  b.adm = mergedXml;
                  anyMergedWrapperImps = true;
                }

                // Annotate the bid.ext.unwrap with the outcome + counts
                b.ext = {
                  ...(b.ext || {}),
                  unwrap: {
                    ...(b.ext?.unwrap || {}),
                    mergedWrapperImps: changed,
                    rvUrl,
                    rvHasWrapper: stats?.rvHasWrapper,
                    rvHasInline: stats?.rvHasInline,
                    rvImpCount: stats?.rvImpCount,
                    targetImpBefore: stats?.targetImpBefore,
                    targetImpAfter: stats?.targetImpAfter,
                    reason: changed ? undefined : "no-change"
                  }
                };

                // richer response header when debug=1 (lastDebug)
                lastDebug = {
                  mode: "merge-wrapper-imps",
                  rvUrl,
                  merged: changed,
                  afterUnwrap: didUnwrap,
                  rvHasWrapper: stats?.rvHasWrapper,
                  rvHasInline: stats?.rvHasInline,
                  rvImpCount: stats?.rvImpCount,
                  targetImpBefore: stats?.targetImpBefore,
                  targetImpAfter: stats?.targetImpAfter,
                  wrapperSnippet: debug ? stats?.wrapperSnippet : undefined
                };
              } catch (e) {
                b.ext = {
                  ...(b.ext || {}),
                  unwrap: { ...(b.ext?.unwrap || {}), mergedWrapperImps: false, rvUrl, reason: "rv-fetch-or-merge-failed" }
                };
                lastDebug = { mode: "merge-wrapper-imps", rvUrl, error: e?.message || String(e), afterUnwrap: didUnwrap };
              }
            } else {
              b.ext = { ...(b.ext || {}), unwrap: { ...(b.ext?.unwrap || {}), mergedWrapperImps: false, reason: "rv-url-not-derived" } };
              lastDebug = { mode: "merge-wrapper-imps", rvUrl: null, reason: "rv-url-not-derived", afterUnwrap: didUnwrap };
            }
          }
        }
      }
    }

    // 3) Debug / observability
    res.setHeader("X-Unwrap", (anyReplaced || anyMergedWrapperImps) ? "inline" : "passthrough");
    res.setHeader("X-Unwrap-Cache", anyCacheHit ? "hit" : "miss");
    if (debug && lastDebug) {
      res.setHeader("X-Unwrap-Debug", JSON.stringify(lastDebug));
    }

    return res.status(upstreamResp.status).json(bidResp);
  } catch (e) {
    const aborted = e?.name === "AbortError";
    const msg = aborted ? `Upstream request timed out after ${timeoutMs}ms` : (e.message || String(e));
    return res.status(aborted ? 504 : 502).json({ error: { code: String(aborted ? 504 : 502), message: msg } });
  } finally {
    cancel();
  }
}
