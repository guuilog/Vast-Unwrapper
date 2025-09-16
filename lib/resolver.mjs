// lib/resolver.mjs
import { XMLParser, XMLBuilder } from "fast-xml-parser";

// ── Env & defaults ───────────────────────────────────────────────────────────
const MAX_DEPTH   = Number(process.env.MAX_DEPTH   || 8);
const TIMEOUT_MS  = Number(process.env.TIMEOUT_MS  || 2500);
const CACHE_TTL_MS= Number(process.env.CACHE_TTL_MS|| 60_000);
const DOWNSTREAM_UA = process.env.DOWNSTREAM_UA || "VAST-Resolver/1.0";
const DEBUG = process.env.DEBUG === "1";

// ── HTTP utils ───────────────────────────────────────────────────────────────
function fetchWithTimeout(url, opts = {}, timeout = TIMEOUT_MS) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeout);
  return fetch(url, { ...opts, signal: controller.signal }).finally(() => clearTimeout(t));
}

// ── XML utils ────────────────────────────────────────────────────────────────
export const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
export const builder = new XMLBuilder({ ignoreAttributes: false, attributeNamePrefix: "", cdataPropName: "#text" });

const toArr = (v) => (Array.isArray(v) ? v : v ? [v] : []);
const mergeUnique = (a = [], b = [], keyFn) => {
  const out = []; const seen = new Set();
  for (const x of [...a, ...b]) {
    const k = keyFn(x);
    if (!k || seen.has(k)) continue;
    seen.add(k); out.push(x);
  }
  return out;
};

const getAdNode  = (doc) => doc?.VAST?.Ad?.[0] ?? doc?.VAST?.Ad;
const getInline  = (doc) => { const ad = getAdNode(doc); return ad?.InLine?.[0] ?? ad?.InLine; };
const getWrapper = (doc) => { const ad = getAdNode(doc); return ad?.Wrapper?.[0] ?? ad?.Wrapper; };

const getVastAdTagUri = (doc) => {
  const w = getWrapper(doc);
  if (!w) return null;
  const raw = w.VASTAdTagURI;
  let uri = Array.isArray(raw) ? raw[0] : raw;
  if (uri && typeof uri === "object" && uri["#text"]) uri = uri["#text"];
  if (typeof uri === "string") uri = uri.trim();
  return uri || null;
};

const vastIdFromUrl = (url) => { try { return new URL(url).searchParams.get("vastid"); } catch { return null; } };

// ── In-memory (per-warm-instance) cache ──────────────────────────────────────
const cache = new Map(); // key -> { xml, exp }
const cacheGet = (k) => {
  const v = cache.get(k);
  if (!v) return null;
  if (v.exp < Date.now()) { cache.delete(k); return null; }
  return v.xml;
};
const cacheSet = (k, xml, ttl = CACHE_TTL_MS) => cache.set(k, { xml, exp: Date.now() + ttl });

// ── Merge wrapper → inline ───────────────────────────────────────────────────
export function mergeWrapperIntoInline(wrapperDoc, inlineDoc) {
  const adI = getInline(inlineDoc);
  const adW = getWrapper(wrapperDoc);
  if (!adI || !adW) return inlineDoc;

  // Error + Impression
  adI.Error = mergeUnique(toArr(adW.Error), toArr(adI.Error), x => (typeof x === "string" ? x : x?.["#text"] || JSON.stringify(x)));
  adI.Impression = mergeUnique(toArr(adW.Impression), toArr(adI.Impression), x => (typeof x === "string" ? x : x?.["#text"] || JSON.stringify(x)));

  // Linear.TrackingEvents
  const iLin = adI.Creatives?.[0]?.Creative?.[0]?.Linear?.[0] ?? adI.Creatives?.Creative?.Linear;
  const wLin = adW.Creatives?.[0]?.Creative?.[0]?.Linear?.[0] ?? adW.Creatives?.Creative?.Linear;
  const iTrk = iLin?.TrackingEvents?.[0]?.Tracking || [];
  const wTrk = wLin?.TrackingEvents?.[0]?.Tracking || [];
  const mergedTrk = mergeUnique(wTrk, iTrk, t => `${t?.event}|${t?.["#text"] || t}`);
  if (iLin) iLin.TrackingEvents = [{ Tracking: mergedTrk }];

  // AdVerifications (VAST 4)
  const wVer = adW.AdVerifications?.[0]?.Verification || [];
  const iVer = adI.AdVerifications?.[0]?.Verification || [];
  const ver = mergeUnique(wVer, iVer, v => JSON.stringify(v));
  if (ver.length) adI.AdVerifications = [{ Verification: ver }];

  // ViewableImpression (VAST 4)
  const iVI = adI.ViewableImpression?.[0] || {};
  const wVI = adW.ViewableImpression?.[0] || {};
  const iVC = iVI.Viewable || [], wVC = wVI.Viewable || [];
  const iNV = iVI.NotViewable || [], wNV = wVI.NotViewable || [];
  const iVU = iVI.ViewUndetermined || [], wVU = wVI.ViewUndetermined || [];
  if (iVC.length || wVC.length || iNV.length || wNV.length || iVU.length || wVU.length) {
    adI.ViewableImpression = [{
      Viewable:        mergeUnique(wVC, iVC, x => x?.["#text"] || x),
      NotViewable:     mergeUnique(wNV, iNV, x => x?.["#text"] || x),
      ViewUndetermined:mergeUnique(wVU, iVU, x => x?.["#text"] || x),
    }];
  }

  // VideoClicks.ClickTracking
  const iClicks = adI.Creatives?.[0]?.Creative?.[0]?.Linear?.[0]?.VideoClicks?.[0]?.ClickTracking || [];
  const wClicks = adW.Creatives?.[0]?.Creative?.[0]?.Linear?.[0]?.VideoClicks?.[0]?.ClickTracking || [];
  const mergedClicks = mergeUnique(wClicks, iClicks, c => c?.["#text"] || c);
  if (iLin) {
    const vc = iLin.VideoClicks?.[0] || {};
    iLin.VideoClicks = [{ ClickTracking: mergedClicks, ...vc }];
  }
  return inlineDoc;
}

// ── Resolve a VAST URL to Inline (with metadata) ─────────────────────────────
export async function resolveToInlineWithMeta(url) {
  let depth = 0;
  let cached = "miss";
  const cacheKey = vastIdFromUrl(url);
  if (cacheKey) {
    const hit = cacheGet(cacheKey);
    if (hit) {
      if (DEBUG) console.log("cache HIT", cacheKey);
      return { xml: hit, depth, cached: "hit" };
    }
  }

  const wrappers = [];
  let text = await (await fetchWithTimeout(url, { headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" } })).text();
  let doc = parser.parse(text);

  while (depth < MAX_DEPTH) {
    if (getInline(doc)) break;
    const next = getVastAdTagUri(doc);
    if (!next) throw new Error("Wrapper missing <VASTAdTagURI>.");
    wrappers.push(doc);
    depth++;
    if (DEBUG) console.log(`unwrap depth ${depth}:`, next);

    const resp = await fetchWithTimeout(next, { headers: { "User-Agent": DOWNSTREAM_UA, Accept: "application/xml,text/xml,*/*" } });
    text = await resp.text();
    doc = parser.parse(text);
  }

  for (const w of wrappers.reverse()) doc = mergeWrapperIntoInline(w, doc);
  const xml = builder.build(doc);
  if (cacheKey) cacheSet(cacheKey, xml);
  return { xml, depth, cached };
}

// ── Helper: unwrap adm if it’s a Wrapper ─────────────────────────────────────
export async function unwrapAdmIfWrapper(admXml) {
  if (typeof admXml !== "string" || !admXml.includes("<Wrapper")) return { adm: admXml, replaced: false, depth: 0, cached: "miss" };
  const doc = parser.parse(admXml);
  const next = getVastAdTagUri(doc);
  if (!next) return { adm: admXml, replaced: false, depth: 0, cached: "miss" };
  const { xml, depth, cached } = await resolveToInlineWithMeta(next);
  return { adm: xml, replaced: true, depth, cached };
}
