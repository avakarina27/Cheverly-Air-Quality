import fetch from 'node-fetch';

export default async function handler(req, res) {
  try {
    const action = req.query.action;
    const PURPLEAIR_KEY = process.env.PURPLEAIR_API_KEY;
    const QUANTAQ_KEY = process.env.QUANTAQ_API_KEY;
    const GROVE_KEY = process.env.GROVESTREAMS_API_KEY;

    if (!action) return res.status(400).json({ error: "missing_action" });

    const fetchJson = async (url, options = {}) => {
      try {
        const r = await fetch(url, options);
        const text = await r.text();
        let data = text;
        try { data = JSON.parse(text); } catch {}
        return { ok: r.ok, status: r.status, data };
      } catch (e) {
        return { ok: false, status: 500, data: e.message };
      }
    };

    // --- 1. QUANTAQ HISTORY ---
    if (action === "quantaq_history") {
      const sn = req.query.compId;
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString('base64');
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data/?limit=100&raw=true`;
      const out = await fetchJson(url, { headers: { "Authorization": `Basic ${auth}` } });
      if (out.ok && out.data && out.data.data) {
        return res.status(200).json(out.data.data.map(e => {
          const val = Number(e.pm25 || e.pm2_5 || e.opcn3_pm25 || e.pm25_env || 0);
          return {
            time: new Date(e.timestamp).getTime(),
            pm25: val, // For QuantAQ style
            data: val  // For Grove style (Double coverage)
          };
        }));
      }
      return res.status(out.status || 500).json({ error: "quantaq_failed", details: out.data });
    }

    // --- 2. GROVE HISTORY ---
    if (action === "grove_history") {
      const compId = req.query.compId;
      const url = `https://grovestreams.com/api/comp/${encodeURIComponent(compId)}/feed?api_key=${encodeURIComponent(GROVE_KEY)}&limit=100`;
      const out = await fetchJson(url);
      
      if (out.ok && out.data && out.data.data) {
          return res.status(200).json(out.data.data.map(point => {
              const val = point[1] !== null ? parseFloat(point[1]) : 0;
              return {
                  time: point[0],
                  pm25: val, // Add this!
                  data: val  // Keep this!
              };
          }));
      }
      return res.status(out.status || 500).json({ error: "grove_failed", details: out.data });
    }

    // --- 3. PURPLEAIR BOX ---
    if (action === "purpleair_box") {
      const url = "https://api.purpleair.com/v1/sensors?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75&fields=sensor_index,latitude,longitude,pm2.5_atm";
      const out = await fetchJson(url, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      return res.status(out.status).json(out.data);
    }

    // --- 4. PURPLEAIR HISTORY ---
    if (action === "purpleair_history") {
      const id = req.query.id, start = req.query.start;
      const url = `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history?fields=pm2.5_atm&average=60&start_timestamp=${encodeURIComponent(start)}`;
      const out = await fetchJson(url, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      return res.status(out.status).json(out.data);
    }

    return res.status(404).json({ error: "unknown_action" });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
