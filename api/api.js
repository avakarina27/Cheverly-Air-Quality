export default async function handler(req, res) {
  try {
    const action = req.query.action;

    const PURPLEAIR_KEY = process.env.PURPLEAIR_API_KEY;
    const QUANTAQ_KEY = process.env.QUANTAQ_API_KEY;

    if (!action) return res.status(400).json({ error: "missing_action" });

    const jsonFetch = async (url, options = {}) => {
      const r = await fetch(url, options);
      const text = await r.text();
      let data = text;
      try { data = JSON.parse(text); } catch {}
      return { ok: r.ok, status: r.status, data };
    };

    // PurpleAir box query for map markers
    if (action === "purpleair_box") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const url =
        "https://api.purpleair.com/v1/sensors" +
        "?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75" +
        "&fields=sensor_index,latitude,longitude,pm2.5_atm";

      const out = await jsonFetch(url, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    // PurpleAir history for a station
    if (action === "purpleair_history") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const id = req.query.id;
      const start = req.query.start;
      if (!id || !start) return res.status(400).json({ error: "missing_id_or_start" });

      const url =
        `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history` +
        `?fields=pm2.5_atm&average=60&start_timestamp=${encodeURIComponent(start)}`;

      const out = await jsonFetch(url, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    // QuantAQ data by date
    if (action === "quantaq_by_date") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const sn = req.query.sn;
      const date = req.query.date;
      if (!sn || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      const url =
        `https://api.quant-aq.com/device-api/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;

      // QuantAQ uses Basic Auth where username is API key, password is blank
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");

      const out = await jsonFetch(url, {
        headers: {
          "Accept": "application/json",
          "Authorization": `Basic ${auth}`
        }
      });

      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    return res.status(404).json({ error: "unknown_action", action });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
