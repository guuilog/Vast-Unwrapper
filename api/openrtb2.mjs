// api/openrtb2.mjs
import { unwrapAdmIfWrapper } from "../lib/resolver.mjs";

const EQUATIV_BID_URL =
  process.env.EQUATIV_BID_URL || "https://ssb-use1.smartadserver.com/api/bid?callerId=241";
const DEBUG = process.env.DEBUG === "1";

// Vercel Node serverless doesn't auto-parse req.body; read it manually.
async function readRequestBody(req) {
  if (req.body && typeof req.body === "object") return req.body; // some clients send parsed
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) return {};
  try { return JSON.parse(raw); } catch (e) {
    throw new Error("Invalid JSON body");
  }
}

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).send("Method Not Allowed");
  }

  try {
    // 1) Read + validate body
    const payload = await readRequestBody(req);

    // 2) Forward OpenRTB to upstream
    const upstream = await fetch(EQUATIV_BID_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!upstream.ok) {
      if (DEBUG) console.warn("Upstream non-OK:", upstream.status);
      return res.status(204).end(); // no-bid or upstream issue
    }

    // 3) Transform bids: replace Wrapper adm → Inline
    const bidResp = await upstream.json();
    let replacedAny = false;
    let maxDepth = 0;
    let anyCacheHit = false;

    for (const sb of bidResp.seatbid ?? []) {
      for (const bid of sb.bid ?? []) {
        try {
          const { adm, replaced, depth, cached } = await unwrapAdmIfWrapper(bid.adm);
          if (replaced) {
            bid.adm = adm;
            replacedAny = true;
            maxDepth = Math.max(maxDepth, depth);
            if (cached === "hit") anyCacheHit = true;
          }
        } catch (e) {
          if (DEBUG) console.warn("unwrap failed:", e?.message || e);
        }
      }
    }

    // 4) Observability headers
    res.setHeader("X-Unwrap", replacedAny ? "inline" : "passthrough");
    res.setHeader("X-Unwrap-Depth", String(maxDepth));
    res.setHeader("X-Unwrap-Cache", anyCacheHit ? "hit" : "miss");

    return res.status(200).json(bidResp);
  } catch (err) {
    if (DEBUG) console.error("Proxy error:", err);
    return res.status(400).send(String(err?.message || "Bad Request"));
  }
}
