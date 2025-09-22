// api/_health.mjs
export const config = { runtime: "nodejs" };

export default async function handler(req, res) {
  try {
    res.status(200).json({
      ok: true,
      node: process.version,
      now: new Date().toISOString()
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
}
