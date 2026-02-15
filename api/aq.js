export default async function handler(req, res) {
  try {
    const action = req.query.action;

    const PURPLEAIR_KEY = process.env.PURPLEAIR_API_KEY;
    const QUANTAQ_KEY = process.env.QUANTAQ_API_KEY;

    if (!action) return res.status(400).json({ error: "missing_action" });

    const fetchJson = async (url, options = {}) => {
      const r = await fetch(url, options);
      const text = await r.text();
      let data = text;
      try { data = JSON.parse(text); } catch {}
      return { ok: r.ok, status: r.status, data };
    };

    // PurpleAir can work either via header or query param depending on account/endpoint
    const purpleairFetch = async (baseUrl) => {
      // Attempt 1: header auth
      let out = await fetchJson(baseUrl, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      if (out.ok) return out;

      // Attempt 2: query auth
      const join = baseUrl.includes("?") ? "&" : "?";
      const url2 = `${baseUrl}${join}api_key=${encodeURIComponent(PURPLEAIR_KEY)}`;
      return await fetchJson(url2);
    };

    // ------------------------
    // PurpleAir: map marker box query
    // ------------------------
    if (action === "purpleair_box") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const url =
        "https://api.purpleair.com/v1/sensors" +
        "?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75" +
        "&fields=sensor_index,latitude,longitude,pm2.5_atm";

      const out = await purpleairFetch(url);

      if (!out.ok) {
        return res.status(out.status).json({
          error: "purpleair_box_failed",
          status: out.status,
          details: out.data
        });
      }

      return res.status(200).json(out.data);
    }

    // ------------------------
    // PurpleAir: history for a station
    // ------------------------
    if (action === "purpleair_history") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const id = req.query.id;
      const start = req.query.start;
      if (!id || !start) return res.status(400).json({ error: "missing_id_or_start" });

      const url =
        `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history` +
        `?fields=pm2.5_atm&average=60&start_timestamp=${encodeURIComponent(start)}`;

      const out = await purpleairFetch(url);

      if (!out.ok) {
        return res.status(out.status).json({
          error: "purpleair_history_failed",
          status: out.status,
          details: out.data
        });
      }

      return res.status(200).json(out.data);
    }

    // ------------------------
    // QuantAQ: data-by-date endpoint
    // Try multiple base URLs because QuantAQ deployments vary.
    // ------------------------
    if (action === "quantaq_by_date") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const sn = req.query.sn;
      const date = req.query.date;
      if (!sn || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      // Basic auth: username=API key, password blank
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");
      const headers = {
        "Accept": "application/json",
        "Authorization": `Basic ${auth}`
      };

      const urls = [
        `https://api.quant-aq.com/device-api/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`,
        `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`
      ];

      let last = null;

      for (const url of urls) {
        const out = await fetchJson(url, { headers });
        if (out.ok) return res.status(200).json(out.data);
        last = { url, status: out.status, details: out.data };
      }

      return res.status(last?.status || 502).json({
        error: "quantaq_failed",
        tried: urls,
        last
      });
    }

    return res.status(404).json({ error: "unknown_action", action });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
