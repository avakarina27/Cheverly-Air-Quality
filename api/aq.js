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

    // --- 1. QUANTAQ (WORKING) ---
    if (action === "quantaq_history") {
      const sn = req.query.compId;
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString('base64');
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data/?limit=100&raw=true`;
      const out = await fetchJson(url, { headers: { "Authorization": `Basic ${auth}` } });
      if (out.ok && out.data && out.data.data) {
        return res.status(200).json(out.data.data.map(e => ({
          time: new Date(e.timestamp).getTime(),
          pm25: Number(e.pm25 || e.pm2_5 || e.opcn3_pm25 || e.pm25_env || 0)
        })));
      }
      return res.status(out.status || 500).json({ error: "quantaq_failed", details: out.data });
    }

    // --- 2. GROVE HISTORY (FIXED FOR DASHBOARD DISPLAY) ---
    if (action === "grove_history") {
      const compId = req.query.compId;
      // We look for these IDs in the feed to make sure we don't grab the wrong sensor
      const targetStreamIds = ['pm25', 'pm2_5', 'pm2.5', 'PM2.5'];
      
      const url = `https://grovestreams.com/api/comp/${encodeURIComponent(compId)}/feed?api_key=${encodeURIComponent(GROVE_KEY)}&limit=100`;
      const out = await fetchJson(url);
      
      if (out.ok && out.data && out.data.stream) {
          // 1. Find the index of the PM2.5 stream in the component's stream list
          const streams = out.data.stream;
          const pmIndex = streams.findIndex(s => targetStreamIds.includes(s.streamId));
          
          // 2. Map the data points using that specific index
          // point[0] is time, point[pmIndex + 1] is the value for that specific stream
          if (pmIndex !== -1 && out.data.data) {
              const formatted = out.data.data.map(point => ({
                  time: point[0],
                  data: point[pmIndex + 1] !== null ? parseFloat(point[1]) : 0
              }));
              return res.status(200).json(formatted);
          }
          
          // Fallback if index isn't found: use the first available data point
          const formattedFallback = out.data.data.map(point => ({
              time: point[0],
              data: point[1] !== null ? parseFloat(point[1]) : 0
          }));
          return res.status(200).json(formattedFallback);
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
