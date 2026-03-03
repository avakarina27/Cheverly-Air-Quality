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

    // PurpleAir can work either via header or query param depending on endpoint/account
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
    // QuantAQ helpers
    // ------------------------
    const quantAuthHeaders = () => {
      if (!QUANTAQ_KEY) return {};
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");
      return {
        "Accept": "application/json",
        "Authorization": `Basic ${auth}`
      };
    };

    // Optional debugging endpoint: list devices you actually have access to
    if (action === "quantaq_devices") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });
      const headers = quantAuthHeaders();
      const url = "https://api.quant-aq.com/v1/devices?per_page=200&page=1";
      const out = await fetchJson(url, { headers });
      return res.status(out.status).json(out.data);
    }

    // ------------------------
    // QuantAQ: most recent (fast “current” value)
    // Docs: GET /v1/data/most-recent/ (requires sn OR org_id OR network_id)
    // ------------------------
    if (action === "quantaq_most_recent") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const sn = req.query.sn;
      if (!sn) return res.status(400).json({ error: "missing_sn" });

      const headers = quantAuthHeaders();
      const url =
        `https://api.quant-aq.com/v1/data/most-recent/?sn=${encodeURIComponent(sn)}&per_page=1&page=1`;

      const out = await fetchJson(url, { headers });

      if (!out.ok) {
        return res.status(out.status).json({
          error: "quantaq_most_recent_failed",
          status: out.status,
          details: out.data
        });
      }

      return res.status(200).json(out.data);
    }

    // ------------------------
    // QuantAQ: resampled data (best for 24h + annual without huge payloads)
    // Docs: GET /v1/data/resampled/ with sn,start_date,end_date,period
    // period: 15m | 1h | 8h | 1d
    // ------------------------
    if (action === "quantaq_resampled") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const sn = req.query.sn;
      const start_date = req.query.start_date;
      const end_date = req.query.end_date;
      const period = req.query.period || "1h";

      if (!sn || !start_date || !end_date) {
        return res.status(400).json({ error: "missing_sn_or_dates" });
      }

      const headers = quantAuthHeaders();
      const url =
        `https://api.quant-aq.com/v1/data/resampled/` +
        `?sn=${encodeURIComponent(sn)}` +
        `&start_date=${encodeURIComponent(start_date)}` +
        `&end_date=${encodeURIComponent(end_date)}` +
        `&period=${encodeURIComponent(period)}`;

      const out = await fetchJson(url, { headers });

      if (!out.ok) {
        return res.status(out.status).json({
          error: "quantaq_resampled_failed",
          status: out.status,
          details: out.data
        });
      }

      return res.status(200).json(out.data);
    }

    // ------------------------
    // Keep your existing QuantAQ data-by-date action (still useful sometimes)
    // ------------------------
    const quantDataByDate = async (sn, date) => {
      const headers = quantAuthHeaders();
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;
      return await fetchJson(url, { headers });
    };

    if (action === "quantaq_by_date") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const snInput = req.query.sn;
      const date = req.query.date;
      if (!snInput || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      const out = await quantDataByDate(snInput, date);
      if (out.ok) return res.status(200).json(out.data);

      return res.status(out.status).json({
        error: "quantaq_failed",
        status: out.status,
        details: out.data
      });
    }

    return res.status(404).json({ error: "unknown_action", action });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
