const fetch = require('node-fetch');

module.exports = async (req, res) => {
    // 1. Set Headers to allow the dashboard to talk to this API
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    const { action, id, compId, start } = req.query;

    // 2. YOUR API KEYS (CRITICAL: Fill these in)
    const PURPLEAIR_KEY = 'YOUR_PURPLE_AIR_READ_KEY'; 
    const QUANTAQ_KEY = 'YOUR_QUANTAQ_API_KEY'; 

    try {
        // --- ROUTE 1: PURPLEAIR SENSOR LIST (The Box) ---
        if (action === 'purpleair_box') {
            const url = `https://api.purpleair.com/v1/sensors?fields=latitude,longitude,name&nwlat=38.96&nwlng=-76.96&selat=38.89&selng=-76.88`;
            const response = await fetch(url, { headers: {'X-API-Key': PURPLEAIR_KEY}});
            const data = await response.json();
            return res.status(200).send(JSON.stringify(data));
        }

        // --- ROUTE 2: PURPLEAIR HISTORICAL DATA ---
        if (action === 'purpleair_history') {
            const url = `https://api.purpleair.com/v1/sensors/${id}/history?start_timestamp=${start}&fields=pm2.5_atm`;
            const response = await fetch(url, { headers: {'X-API-Key': PURPLEAIR_KEY}});
            const data = await response.json();
            return res.status(200).send(JSON.stringify(data));
        }

        // --- ROUTE 3: QUANTAQ DATA (The "Triangles") ---
        if (action === 'quantaq_history') {
            const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString('base64');
            const url = `https://api.quantaq.com/device-api/v1/devices/${compId}/data-raw/?limit=100`;
            
            const response = await fetch(url, {
                headers: { 'Authorization': `Basic ${auth}` }
            });
            const data = await response.json();
            
            // Format QuantAQ to match our Dashboard's expectations
            const formatted = (data.data || []).map(entry => ({
                time: new Date(entry.timestamp).getTime(),
                pm25: entry.pm25 || entry.pm2_5 || 0
            }));
            return res.status(200).send(JSON.stringify(formatted));
        }

        // --- ROUTE 4: GROVE / C12 DATA ---
        if (action === 'grove_history') {
            const url = `https://api.grove.id/v1/devices/${compId}/points?limit=100`;
            const response = await fetch(url);
            const data = await response.json();
            return res.status(200).send(JSON.stringify(data));
        }

        // Error if none of the above actions match
        return res.status(400).send(JSON.stringify({ error: "unknown_action", action }));

    } catch (error) {
        console.error("Server Error:", error);
        return res.status(500).send(JSON.stringify({ error: error.message }));
    }
};
