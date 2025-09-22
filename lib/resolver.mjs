// lib/resolver.mjs
import { XMLParser, XMLBuilder } from "fast-xml-parser";

const MAX_DEPTH      = Number(process.env.MAX_DEPTH      || 8);
const TIMEOUT_MS     = Number(process.env.TIMEOUT_MS     || 2500);
const CACHE_TTL_MS   = Number(process.env.CACHE_TTL_MS   || 60_000);
const DOWNSTREAM_UA  = process.env.DOWNSTREAM_UA || "VAST-Resolver/1.2";
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
  for (const it of [...a, ...b]) {
    const k = keyFn ? keyFn(it) : JSON.stringify(it);
    if (!seen.has(k)) { seen.add(k); out.push(it); }
  }
  return out;
};

// ───── helpers to locate nodes ──────────────────────────────────────────────
function getAd(doc) {
  return doc?.VAST?.Ad?.[0] || doc?.VAST?.Ad || null;
}
function getInline(doc) {
  const ad = getAd(doc);
  return ad?.InLine?.[0] || ad?.InLine || null;
}
function getWrapper(doc) {
  const ad = getAd(doc);
  return ad?.Wrapper?.[0] || ad?.Wrapper || null;
}
function getVastAdTagUri(doc) {
  const w = getWrapper(doc);
  if (!w) return null;
  const tag = w?.VASTAdTagURI;
  if (!tag) return null;
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

function mergeImpressions(wrapperNode, inlineNode) {
  const wImp = normalizeImpressions(wrapperNode?.Impression).map(o => ({ ...o, "data-origin": "wrapper" })); // tag origin for QA
  const iImp = normalizeImpressions(inlineNode?.Impression);

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
  if (merged.length) inlineNode.Impression = merged;
}

// ───── merge wrapper → inline (uses correct nodes) ──────────────────────────
function mergeWrapperIntoInline(wrapperDoc, inlineDoc) {
  const w = getWrapper(wrapperDoc);
  const i = getInline(inlineDoc);
  if (!w || !i) return inlineDoc;

  // 1) Impressions
  mergeImpressions(w, i);

  // 2) Linear TrackingEvents
  const wCreatives = toArr(w?.Creatives?.[0]?.Creative || w?.Creatives?.Creative || []);
  const iCreatives = toArr(i?.Creatives?.[0]?.Creative || i?.Creatives?.Creative || []);
  const wLinear    = toArr((wCreatives[0]?.Linear) || (wCreatives[0]?.Linear?.[0] ? wCreatives[0]?.Linear?.[0] : []));
  const iLinear    = toArr((iCreatives[0]?.Linear) || (iCreatives[0]?.Linear?.[0] ? iCreatives[0]?.Linear?.[0] : []));

  const wTrk = toArr(wLinear[0]?.TrackingEvents?.[0]?.Tracking || wLinear[0]?.TrackingEvents?.Tracking || []);
  const iTrk = toArr(iLinear[0]?.TrackingEvents?.[0]?.Tracking || iLinear[0]?.TrackingEvents?.Tracking || []);
  const mergedTrk = mergeUnique(iTrk, wTrk, t => `${t?.event}|${t?.["#text"] || t}`);
  if (iLinear[0]) {
    iLinear[0].TrackingEvents = [{ Tracking: mergedTrk }];
    // write back
    if (iCreatives[0]) {
      iCreatives[0].Linear = [iLinear[0]];
      i.Creatives = [{ Creative: iCreatives }];
    }
  }

  // 3) AdVerifications
  const wVer = toArr(w?.AdVerifications?.[0]?.Verification || w?.AdVerifications?.Verification || []);
  const iVer = toArr(i?.AdVerifications?.[0]?.Verification || i?.AdVerifications?.Verification || []);
  const verMerged = mergeUnique(iVer, wVer, v => JSON.stringify(v));
  if (verMerged.length) i.AdVerifications = [{ Verification: verMerged }];

  // 4) ViewableImpression (Viewable + ViewUndetermined)
  const wVI = w?.ViewableImpression?.[0] || w?.ViewableImpression || {};
  const iVI = i?.ViewableImpression?.[0] || i?.ViewableImpression || {};
  const mergedViewable = mergeUnique(
    toArr(iVI?.Viewable || []), toArr(wVI?.Viewable || []),
    v => (typeof v === "string" ? v : v?.["#text"] || JSON.stringify(v))
  );
  const mergedUndet = mergeUnique(
    toArr(iVI?.ViewUndetermined || []), toArr(wVI?.ViewUndetermined || []),
    v => (typeof v === "string" ? v : v?.["#text"] || JSON.stringify(v))
  );
  const vi = {};
  if (mergedViewable.length) vi.Viewable = mergedViewable;
  if (mergedUndet.length) vi.ViewUndetermined = mergedUndet;
  if (Object.keys(vi).length) i.ViewableImpression = [vi];

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

  // merge wrapper nodes (outermost first) into final inline node
  for (const wdoc of wrappers.reverse()) doc = mergeWrapperIntoInline(wdoc, doc);

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

export async function mergeWrapperImpressionsIntoInlineXml(inlineAdmXml, wrapperUrl, { debug = false } = {}) {
  const inlineDoc = parser.parse(inlineAdmXml);

  const resp = await fetchWithTimeout(wrapperUrl, {
    headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" }
  });
  if (!resp.ok) throw new Error(`Wrapper fetch failed: ${resp.status}`);

  const wrapperText = await resp.text();
  const wrapperDoc  = parser.parse(wrapperText);

  // Pick the source node we can actually merge from:
  // Prefer <Wrapper>, but fall back to <InLine> if RV already returns Inline
  const sourceWrapper = getWrapper(wrapperDoc);
  const sourceInline  = getInline(wrapperDoc);
  const sourceNode    = sourceWrapper || sourceInline || null;

  const targetInline  = getInline(inlineDoc);

  const preCounts = {
    rvHasWrapper: !!sourceWrapper,
    rvHasInline:  !!sourceInline,
    rvImpCount:   normalizeImpressions(sourceNode?.Impression).length,
    targetImpBefore: normalizeImpressions(targetInline?.Impression).length
  };

  let mergedDoc = inlineDoc;
  if (sourceNode && targetInline) {
    // Minimal, targeted merge: only Impressions from the RV doc's source node.
    // We reuse mergeImpressions(), but point it at (sourceNode → targetInline).
    mergeImpressions(sourceNode, targetInline);
  }
  const postCounts = {
    targetImpAfter: normalizeImpressions(getInline(mergedDoc)?.Impression).length
  };

  // Return both the XML and stats to the caller (API) so it can surface debug headers.
  return {
    xml: builder.build(mergedDoc),
    stats: {
      ...preCounts,
      ...postCounts,
      wrapperSnippet: debug ? wrapperText.slice(0, 512) : undefined // short peek for debugging
    }
  };
}
