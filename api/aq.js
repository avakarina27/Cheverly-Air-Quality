export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  if (req.method === "OPTIONS") return res.status(200).end();

  const { action } = req.query;

  const PURPLE_AIR_KEY = process.env.PURPLE_AIR_KEY;
  const QUANTAQ_KEY = process.env.QUANTAQ_KEY;

  try {
    if (action === "purpleair_box") {
      if (!PURPLE_AIR_KEY) return res.status(500).json({ error: "missing_purpleair_key" });

      const { nwlng, nwlat, selng, selat, fields } = req.query;

      const url =
        `https://api.purpleair.com/v1/sensors` +
        `?nwlng=${encodeURIComponent(nwlng)}` +
        `&nwlat=${encodeURIComponent(nwlat)}` +
        `&selng=${encodeURIComponent(selng)}` +
        `&selat=${encodeURIComponent(selat)}` +
        `&fields=${encodeURIComponent(fields || "")}` +
        `&api_key=${encodeURIComponent(PURPLE_AIR_KEY)}`;

      const r = await fetch(url);
      const text = await r.text();
      return res.status(r.status).send(text);
    }

    if (action === "purpleair_history") {
      if (!PURPLE_AIR_KEY) return res.status(500).json({ error: "missing_purpleair_key" });

      const { id, fields, average, start_timestamp } = req.query;

      const url =
        `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history` +
        `?fields=${encodeURIComponent(fields || "pm2.5_atm")}` +
        `&average=${encodeURIComponent(average || "60")}` +
        `&start_timestamp=${encodeURIComponent(start_timestamp)}` +
        `&api_key=${encodeURIComponent(PURPLE_AIR_KEY)}`;

      const r = await fetch(url);
      const text = await r.text();
      return res.status(r.status).send(text);
    }

    if (action === "quantaq_devices") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_quantaq_key" });

      const url = "https://api.quant-aq.com/v1/devices/?page=1&per_page=200";
      const r = await fetch(url, { headers: { Authorization: QUANTAQ_KEY } });
      const text = await r.text();
      return res.status(r.status).send(text);
    }

    if (action === "quantaq_bydate") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_quantaq_key" });

      const { sn, date } = req.query;
      if (!sn || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;
      const r = await fetch(url, { headers: { Authorization: QUANTAQ_KEY } });
      const text = await r.text();
      return res.status(r.status).send(text);
    }

    return res.status(400).json({ error: "unknown_action", action });
  } catch (e) {
    return res.status(500).json({ error: "server_error", details: String(e?.message || e) });
  }
}
