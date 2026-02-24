import fetch from 'node-fetch';

export default async function handler(req, res) {
  const { action, compId, id, start } = req.query;
  const { PURPLEAIR_API_KEY: pk, QUANTAQ_API_KEY: qk, GROVESTREAMS_API_KEY: gk } = process.env;

  try {
    if (action === "quantaq_history") {
      const auth = Buffer.from(`${qk}:`).toString('base64');
      const r = await fetch(`https://api.quant-aq.com/device-api/v1/devices/${compId}/data-raw/?limit=100`, {
        headers: { "Authorization": `Basic ${auth}` }
      });
      const j = await r.json();
      return res.json(j.data.map(e => ({ time: new Date(e.timestamp).getTime(), pm25: e.pm25 || 0 })));
    }

    if (action === "purpleair_box") {
      const r = await fetch("https://api.purpleair.com/v1/sensors?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75&fields=sensor_index,latitude,longitude,pm2.5_atm", { headers: { "X-API-Key": pk } });
      const j = await r.json();
      return res.json(j);
    }

    if (action === "purpleair_history") {
      const r = await fetch(`https://api.purpleair.com/v1/sensors/${id}/history?fields=pm2.5_atm&average=60&start_timestamp=${start}`, { headers: { "X-API-Key": pk } });
      const j = await r.json();
      return res.json(j);
    }
    
    // Fallback for C12
    if (action === "grove_history") {
      const r = await fetch(`https://grovestreams.com/api/comp/${compId}/feed?api_key=${gk}`);
      const j = await r.json();
      return res.json(j.data.map(p => ({ time: p[0], data: p[1] })));
    }

    return res.status(404).send("Not Found");
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
