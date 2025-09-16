// api/_health.mjs
export default async function handler(_req, res) {
  return res.status(200).send("ok");
}
