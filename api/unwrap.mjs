// api/unwrap.mjs
import { resolveToInlineWithMeta } from "../lib/resolver.mjs";

export default async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).send("Method Not Allowed");
  }

  // Parse ?url=... safely (works on Vercel)
  const { searchParams } = new URL(req.url, `https://${req.headers.host}`);
  const url = searchParams.get("url");
  if (!url) return res.status(400).send("missing ?url=");

  try {
    const { xml, depth, cached } = await resolveToInlineWithMeta(url);
    res.setHeader("Content-Type", "application/xml");
    res.setHeader("X-Unwrap-Depth", String(depth));
    res.setHeader("X-Unwrap-Cache", cached);
    return res.status(200).send(xml);
  } catch (e) {
    return res.status(500).send(String(e));
  }
}
