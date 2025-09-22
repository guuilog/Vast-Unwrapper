// lib/resolver.mjs
import { XMLParser, XMLBuilder } from "fast-xml-parser";

const MAX_DEPTH      = Number(process.env.MAX_DEPTH      || 8);
const TIMEOUT_MS     = Number(process.env.TIMEOUT_MS     || 2500);
const CACHE_TTL_MS   = Number(process.env.CACHE_TTL_MS   || 60_000);
const DOWNSTREAM_UA  = process.env.DOWNSTREAM_UA || "VAST-Resolver/1.0";
const DEBUG          = process.env.DEBUG === "1";

// ── simple in-memory cache (serverless: per-container) ───────────────────────
const cache = new Map();
function cacheGet(k) {
  const v = cache.get(k);
  if (!v) return null;
  if (Date.now() > v.exp) { cache.delete(k); return null; }
  return v.val;
}
function cacheSet(k, val, ttl = CACHE_TTL_MS) {
  cache.set(k, { val, exp: Date.now() + ttl });
}

// ── HTTP with timeout ────────────────────────────────────────────────────────
function fetchWithTimeout(url, opts = {}, timeout = TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  return fetch(url, { ...opts, signal: controller.signal }).finally(() => clearTimeout(timer));
}

// ── XML tools ────────────────────────────────────────────────────────────────
export const parser  = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
export const builder = new XMLBuilder({ ignoreAttributes: false, attributeNamePrefix: "", cdataPropName: "#text" });

const toArr = v => (Array.isArray(v) ? v : v ? [v] : []);
const mergeUnique = (a = [], b = [], keyFn) => {
  const seen = new Set(); const out = [];
  for (const it of [...a, ...b]) {
    const k = keyFn ? keyFn(it) : JSON.stringify(it);
    if (!seen.has(k)) { seen.add(k); out.push(it); }
  }
  return out;
};

// ── VAST helpers ─────────────────────────────────────────────────────────────
function getInline(doc) {
  try { return doc?.VAST?.Ad?.[0]?.InLine || doc?.VAST?.Ad?.InLine; } catch { return null; }
}
function getWrapper(doc) {
  try { return doc?.VAST?.Ad?.[0]?.Wrapper || doc?.VAST?.Ad?.Wrapper; } catch { return null; }
}
function getVastAdTagUri(doc) {
  const w = getWrapper(doc);
  if (!w) return null;
  const tag = w?.VASTAdTagURI;
  if (!tag) return null;
  // fast-xml-parser maps CDATA to "#text"
  return typeof tag === "string" ? tag : (tag?.["#text"] || null);
}

// ── Impression normalization + merge (keep SSP + DSP) ───────────────────────
function normalizeImpressions(imps) {
  const arr = Array.isArray(imps) ? imps : (imps ? [imps] : []);
  return arr.map(x => {
    if (typeof x === "string") return { "#text": x };
    if (x && typeof x === "object") {
      if (typeof x["#text"] === "string") return x;
      if (typeof x.url === "string") return { "#text": x.url, id: x.id };
    }
    return x;
  }).filter(Boolean);
}

function mergeImpressions(wrapperAd, inlineAd) {
  const wImp = normalizeImpressions(wrapperAd?.Impression);
  const iImp = normalizeImpressions(inlineAd?.Impression);

  // Dedup by URL ("#text"), keep first occurrence (preserving id if present)
  const seen = new Set();
  const merged = [];
  for (const src of [iImp, wImp]) {
    for (const imp of src) {
      const url = typeof imp === "string" ? imp : (imp?.["#text"] || "");
      if (!url) continue;
      if (seen.has(url)) continue;
      seen.add(url);
      merged.push(typeof imp === "string" ? { "#text": url } : imp);
    }
  }
  if (merged.length) inlineAd.Impression = merged;
}

// ── Merge wrapper trackers into inline ───────────────────────────────────────
function mergeWrapperIntoInline(wrapperDoc, inlineDoc) {
  const adW = wrapperDoc?.VAST?.Ad?.[0] || wrapperDoc?.VAST?.Ad;
  const adI = inlineDoc?.VAST?.Ad?.[0] || inlineDoc?.VAST?.Ad;
  if (!adW || !adI) return inlineDoc;

  // 1) Impressions (SSP + DSP both kept; dedup by URL)
  mergeImpressions(adW, adI);

  // 2) Linear TrackingEvents
  const wLin = adW.Creatives?.[0]?.Creative?.[0]?.Linear?.[0] || {};
  const iLin = adI.Creatives?.[0]?.Creative?.[0]?.Linear?.[0];
  const wTrk = toArr(wLin?.TrackingEvents?.[0]?.Tracking || []);
  const iTrk = toArr(iLin?.TrackingEvents?.[0]?.Tracking || []);
  const mergedTrk = mergeUnique(iTrk, wTrk, t => `${t?.event}|${t?.["#text"] || t}`);
  if (iLin) iLin.TrackingEvents = [{ Tracking: mergedTrk }];

  // 3) AdVerifications
  const wVer = toArr(adW.AdVerifications?.[0]?.Verification || []);
  const iVer = toArr(adI.AdVerifications?.[0]?.Verification || []);
  const verMerged = mergeUnique(iVer, wVer, v => JSON.stringify(v));
  if (verMerged.length) adI.AdVerifications = [{ Verification: verMerged }];

  // 4) ViewableImpression (Viewable + ViewUndetermined)
  const wVI = adW.ViewableImpression?.[0] || {};
  const iVI = adI.ViewableImpression?.[0] || {};
  const mergedViewable = mergeUnique(
    toArr(iVI.Viewable || []), toArr(wVI.Viewable || []),
    v => (typeof v === "string" ? v : v?.["#text"] || JSON.stringify(v))
  );
  const mergedUndet = mergeUnique(
    toArr(iVI.ViewUndetermined || []), toArr(wVI.ViewUndetermined || []),
    v => (typeof v === "string" ? v : v?.["#text"] || JSON.stringify(v))
  );
  const vi = {};
  if (mergedViewable.length) vi.Viewable = mergedViewable;
  if (mergedUndet.length) vi.ViewUndetermined = mergedUndet;
  if (Object.keys(vi).length) adI.ViewableImpression = [vi];

  return inlineDoc;
}

// ── Core unwrap: resolve Wrapper→Inline recursively, merge trackers ──────────
export async function resolveToInlineWithMeta(vastUrl) {
  const cacheKey = `rv:${vastUrl}`;
  const cached = cacheGet(cacheKey);
  if (cached) return { xml: cached, depth: 0, cached: "hit" };

  let depth = 0;
  let text = "";
  let doc;

  // fetch first document
  {
    const resp = await fetchWithTimeout(vastUrl, {
      headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" }
    });
    text = await resp.text();
    doc = parser.parse(text);
  }

  const wrappers = [];
  while (depth < MAX_DEPTH) {
    if (getInline(doc)) break;
    const next = getVastAdTagUri(doc);
    if (!next) throw new Error("Wrapper missing <VASTAdTagURI>.");
    wrappers.push(doc);
    depth++;

    if (DEBUG) console.log(`[unwrap] depth=${depth} → ${next}`);
    const resp = await fetchWithTimeout(next, {
      headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" }
    });
    text = await resp.text();
    doc = parser.parse(text);
  }

  // merge wrappers (outermost first) into final inline
  for (const w of wrappers.reverse()) doc = mergeWrapperIntoInline(w, doc);

  const xml = builder.build(doc);
  cacheSet(cacheKey, xml);
  return { xml, depth, cached: "miss" };
}

// ── Helper for OpenRTB: replace adm if it’s a Wrapper ────────────────────────
export async function unwrapAdmIfWrapper(admXml) {
  if (typeof admXml !== "string" || !admXml.includes("<Wrapper")) {
    return { adm: admXml, replaced: false, depth: 0, cached: "miss" };
  }
  const doc = parser.parse(admXml);
  const next = getVastAdTagUri(doc);
  if (!next) return { adm: admXml, replaced: false, depth: 0, cached: "miss" };
  const { xml, depth, cached } = await resolveToInlineWithMeta(next);
  return { adm: xml, replaced: true, depth, cached };
}

// ── NEW: Merge wrapper (SSP) impressions into an existing Inline XML ─────────
// This is used when the bid already comes Inline but we can reconstruct
// the wrapper RV URL (e.g., from Equativ nurl) and want to keep SSP pixels too.
export async function mergeWrapperImpressionsIntoInlineXml(inlineAdmXml, wrapperUrl) {
  const inlineDoc = parser.parse(inlineAdmXml);

  const resp = await fetchWithTimeout(wrapperUrl, {
    headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" }
  });
  const wrapperText = await resp.text();
  const wrapperDoc = parser.parse(wrapperText);

  // Merge (reuses the same logic used after full unwrap)
  const merged = mergeWrapperIntoInline(wrapperDoc, inlineDoc);
  return builder.build(merged);
}
