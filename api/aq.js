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

    const purpleairFetch = async (baseUrl) => {
      let out = await fetchJson(baseUrl, { headers: { "X-API-Key": PURPLEAIR_KEY } });
      if (out.ok) return out;
      const join = baseUrl.includes("?") ? "&" : "?";
      const url2 = `${baseUrl}${join}api_key=${encodeURIComponent(PURPLEAIR_KEY)}`;
      return await fetchJson(url2);
    };

    // --- 1. QUANTAQ HISTORY (KEEPING WORKING) ---
    if (action === "quantaq_history") {
      const sn = req.query.compId;
      if (!sn) return res.status(400).json({ error: "missing_sn" });
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString('base64');
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data/?limit=100&raw=true`;
      const out = await fetchJson(url, { headers: { "Authorization": `Basic ${auth}` } });

      if (out.ok && out.data && out.data.data) {
        const formatted = out.data.data.map(entry => ({
          time: new Date(entry.timestamp).getTime(),
          pm25: entry.pm25 || entry.pm2_5 || entry.opcn3_pm25 || entry.pm25_env || 0
        }));
        return res.status(200).json(formatted);
      }
      return res.status(out.status || 500).json({ error: "quantaq_failed", detail: out.data });
    }

    // --- 2. GROVE HISTORY (FIXED FOR C12s) ---
    if (action === "grove_history") {
      const compId = req.query.compId;
      if (!compId) return res.status(400).json({ error: "missing_compId" });
      
      // Specifically targeting the 'pm25' stream which C12s use
      const url = `https://grovestreams.com/api/comp/${encodeURIComponent(compId)}/stream/pm25/feed?api_key=${encodeURIComponent(GROVE_KEY)}&limit=100`;
      const out = await fetchJson(url);
      
      if (out.ok && out.data && out.data.data) {
          // dashboard.html line: (s.pm25 || s.data || 0)
          // Grove data returns [[time, val], [time, val]]
          const formatted = out.data.data.map(point => ({
              time: point[0],
              data: point[1]
          }));
          return res.status(200).json(formatted);
      }
      return res.status(out.status || 500).json({ error: "grove_failed", detail: out.data });
    }

    // --- 3. PURPLEAIR BOX ---
    if (action === "purpleair_box") {
      const url = "https://api.purpleair.com/v1/sensors?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75&fields=sensor_index,latitude,longitude,pm2.5_atm";
      const out = await purpleairFetch(url);
      return res.status(out.status).json(out.data);
    }

    // --- 4. PURPLEAIR HISTORY ---
    if (action === "purpleair_history") {
      const id = req.query.id, start = req.query.start;
      const url = `https://api.purpleair.com/v1/sensors/${encodeURIComponent(id)}/history?fields=pm2.5_atm&average=60&start_timestamp=${encodeURIComponent(start)}`;
      const out = await purpleairFetch(url);
      return res.status(out.status).json(out.data);
    }

    return res.status(404).json({ error: "unknown_action" });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
