// lib/resolver.mjs
import { XMLParser, XMLBuilder } from "fast-xml-parser";

const MAX_DEPTH      = Number(process.env.MAX_DEPTH      || 8);
const TIMEOUT_MS     = Number(process.env.TIMEOUT_MS     || 2500);
const CACHE_TTL_MS   = Number(process.env.CACHE_TTL_MS   || 60_000);
const DOWNSTREAM_UA  = process.env.DOWNSTREAM_UA || "VAST-Resolver/1.1";
const DEBUG          = process.env.DEBUG === "1";
const IMP_DEDUP      = process.env.IMP_DEDUP === "1"; // default off → keep all to verify

// ───── cache ────────────────────────────────────────────────────────────────
const cache = new Map();
const cacheGet = k => { const v = cache.get(k); if (!v) return null; if (Date.now() > v.exp) { cache.delete(k); return null; } return v.val; };
const cacheSet = (k, val, ttl = CACHE_TTL_MS) => cache.set(k, { val, exp: Date.now() + ttl });

// ───── http ────────────────────────────────────────────────────────────────
function fetchWithTimeout(url, opts = {}, timeout = TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  return fetch(url, { ...opts, signal: controller.signal }).finally(() => clearTimeout(timer));
}

// ───── xml ─────────────────────────────────────────────────────────────────
export const parser  = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
export const builder = new XMLBuilder({ ignoreAttributes: false, attributeNamePrefix: "", cdataPropName: "#text" });

const toArr = v => (Array.isArray(v) ? v : v ? [v] : []);
const mergeUnique = (a = [], b = [], keyFn) => {
  const seen = new Set(); const out = [];
  for (const it of [...a, ...b]) { const k = keyFn ? keyFn(it) : JSON.stringify(it); if (!seen.has(k)) { seen.add(k); out.push(it); } }
  return out;
};

// ───── helpers ──────────────────────────────────────────────────────────────
function getInline(doc)   { try { return doc?.VAST?.Ad?.[0]?.InLine || doc?.VAST?.Ad?.InLine; } catch { return null; } }
function getWrapper(doc)  { try { return doc?.VAST?.Ad?.[0]?.Wrapper || doc?.VAST?.Ad?.Wrapper; } catch { return null; } }
function getVastAdTagUri(doc) {
  const w = getWrapper(doc); if (!w) return null;
  const tag = w?.VASTAdTagURI; if (!tag) return null;
  return typeof tag === "string" ? tag : (tag?.["#text"] || null);
}

// ───── impressions (now: KEEP ALL by default for verifiability) ────────────
function normalizeImpressions(imps) {
  const arr = Array.isArray(imps) ? imps : (imps ? [imps] : []);
  return arr.map(x => {
    if (typeof x === "string") return { "#text": x };
    if (x && typeof x === "object") {
      if (typeof x["#text"] === "string") return x;
      if (typeof x.url === "string") return { "#text": x.url, id: x.id };
    }
    return null;
  }).filter(Boolean);
}

function mergeImpressions(wrapperAd, inlineAd) {
  const wImp = normalizeImpressions(wrapperAd?.Impression).map(o => ({ ...o, "data-origin": "wrapper" })); // tag origin for debug
  const iImp = normalizeImpressions(inlineAd?.Impression);

  let merged;
  if (IMP_DEDUP) {
    const seen = new Set();
    merged = [];
    for (const src of [iImp, wImp]) {
      for (const imp of src) {
        const url = imp?.["#text"] || "";
        if (!url) continue;
        if (seen.has(url)) continue;
        seen.add(url);
        merged.push(imp);
      }
    }
  } else {
    // KEEP ALL (inline first, then wrapper) for visibility
    merged = [...iImp, ...wImp];
  }
  if (merged.length) inlineAd.Impression = merged;
}

// ───── merge wrapper → inline ───────────────────────────────────────────────
function mergeWrapperIntoInline(wrapperDoc, inlineDoc) {
  const adW = wrapperDoc?.VAST?.Ad?.[0] || wrapperDoc?.VAST?.Ad;
  const adI = inlineDoc?.VAST?.Ad?.[0] || inlineDoc?.VAST?.Ad;
  if (!adW || !adI) return inlineDoc;

  // 1) Impressions
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

  // 4) ViewableImpression
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

// ───── unwrap recursion ─────────────────────────────────────────────────────
export async function resolveToInlineWithMeta(vastUrl) {
  const cacheKey = `rv:${vastUrl}`;
  const cached = cacheGet(cacheKey);
  if (cached) return { xml: cached, depth: 0, cached: "hit" };

  let depth = 0;
  let text = "";
  let doc;

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

  for (const w of wrappers.reverse()) doc = mergeWrapperIntoInline(w, doc);

  const xml = builder.build(doc);
  cacheSet(cacheKey, xml);
  return { xml, depth, cached: "miss" };
}

// ───── OpenRTB helper ───────────────────────────────────────────────────────
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

// ───── NEW: merge SSP impressions into Inline-only XML (with debug) ─────────
export async function mergeWrapperImpressionsIntoInlineXml(inlineAdmXml, wrapperUrl, { debug = false } = {}) {
  const inlineDoc = parser.parse(inlineAdmXml);

  const resp = await fetchWithTimeout(wrapperUrl, {
    headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" }
  });
  if (!resp.ok) throw new Error(`Wrapper fetch failed: ${resp.status}`);

  const wrapperText = await resp.text();
  const wrapperDoc = parser.parse(wrapperText);

  // pre/post counts (debug)
  const pre = {
    inlineImps: toArr((inlineDoc?.VAST?.Ad?.[0] || inlineDoc?.VAST?.Ad)?.Impression || []).length
  };

  const merged = mergeWrapperIntoInline(wrapperDoc, inlineDoc);

  const post = {
    mergedImps: toArr((merged?.VAST?.Ad?.[0] || merged?.VAST?.Ad)?.Impression || []).length
  };

  if (debug && typeof console !== "undefined") {
    console.log(`[merge-wrapper-imps] ${pre.inlineImps} → ${post.mergedImps} via ${wrapperUrl}`);
  }

  return builder.build(merged);
}
