export default async function handler(req, res) {
  try {
    const { action } = req.query;

    // Keys stored in Vercel env vars
    const PURPLEAIR_KEY = process.env.PURPLEAIR_API_KEY;
    const QUANTAQ_KEY = process.env.QUANTAQ_API_KEY;
    const CLARITY_KEY = process.env.CLARITY_API_KEY;

    if (!action) {
      return res.status(400).json({ error: "missing_action" });
    }

    // Helper
    const jsonFetch = async (url, options = {}) => {
      const r = await fetch(url, options);
      const text = await r.text();
      let data;
      try { data = JSON.parse(text); } catch { data = text; }
      if (!r.ok) {
        return { ok: false, status: r.status, data };
      }
      return { ok: true, status: r.status, data };
    };

    // 1) PurpleAir box
    if (action === "purpleair_box") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const url =
        "https://api.purpleair.com/v1/sensors" +
        "?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75" +
        "&fields=sensor_index,latitude,longitude,pm2.5_atm" +
        `&api_key=${encodeURIComponent(PURPLEAIR_KEY)}`;

      const out = await jsonFetch(url);
      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    // 2) PurpleAir history
    if (action === "purpleair_history") {
      if (!PURPLEAIR_KEY) return res.status(500).json({ error: "missing_PURPLEAIR_API_KEY" });

      const { id, start } = req.query;
      if (!id || !start) return res.status(400).json({ error: "missing_id_or_start" });

      const url =
        `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history` +
        `?fields=pm2.5_atm&average=60&start_timestamp=${encodeURIComponent(start)}` +
        `&api_key=${encodeURIComponent(PURPLEAIR_KEY)}`;

      const out = await jsonFetch(url);
      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    // 3) QuantAQ by date (matches your R approach)
    if (action === "quantaq_by_date") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const { sn, date } = req.query;
      if (!sn || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      const url =
        `https://api.quant-aq.com/device-api/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;

      // QuantAQ uses Basic Auth where username is your API key and password is blank
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");

      const out = await jsonFetch(url, {
        headers: {
          "Accept": "application/json",
          "Authorization": `Basic ${auth}`
        }
      });

      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    // 4) Clarity recent query (C12)
    if (action === "clarity_recent") {
      if (!CLARITY_KEY) return res.status(500).json({ error: "missing_CLARITY_API_KEY" });

      const { org, datasourceIds, outputFrequency = "hour" } = req.query;
      if (!org || !datasourceIds) return res.status(400).json({ error: "missing_org_or_datasourceIds" });

      const ids = String(datasourceIds)
        .split(",")
        .map(s => s.trim())
        .filter(Boolean);

      const body = {
        org,
        datasourceIds: ids,
        outputFrequency,
        metricSelect: "pm2_5ConcMassCalibratedNowCastAqi,pm2_5ConcMassCalibratedNowCast,pm2_5ConcMassCalibrated1HourMean",
        format: "json-long"
      };

      const out = await jsonFetch(
        "https://clarity-data-api.clarity.io/v2/recent-datasource-measurements-query",
        {
          method: "POST",
          headers: {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-api-key": CLARITY_KEY
          },
          body: JSON.stringify(body)
        }
      );

      return res.status(out.ok ? 200 : out.status).json(out.data);
    }

    return res.status(404).json({ error: "unknown_action", action });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
