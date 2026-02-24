const fetch = require('node-fetch');

module.exports = async (req, res) => {
    const { action, id, compId, start } = req.query;

    // --- CONFIGURATION ---
    const PURPLEAIR_API_KEY = 'YOUR_PURPLE_AIR_READ_KEY'; 
    const QUANTAQ_API_KEY = 'YOUR_QUANTAQ_API_KEY'; // Found in your QuantAQ Console
    // ---------------------

    try {
        switch (action) {
            
            // 1. PURPLEAIR DATA
            case 'purpleair_box':
                // Cheverly Area Box
                const boxUrl = `https://api.purpleair.com/v1/sensors?fields=latitude,longitude,name&nwlat=38.96&nwlng=-76.96&selat=38.89&selng=-76.88`;
                const boxRes = await fetch(boxUrl, { headers: { 'X-API-Key': PURPLEAIR_API_KEY } });
                return res.json(await boxRes.json());

            case 'purpleair_history':
                const histUrl = `https://api.purpleair.com/v1/sensors/${id}/history?start_timestamp=${start}&fields=pm2.5_atm`;
                const histRes = await fetch(histUrl, { headers: { 'X-API-Key': PURPLEAIR_API_KEY } });
                return res.json(await histRes.json());

            // 2. QUANTAQ DATA (The New Section)
            case 'quantaq_history':
                if (!compId) return res.status(400).json({ error: "Missing compId" });
                
                // QuantAQ uses Basic Auth (Key as username, empty password)
                const auth = Buffer.from(`${QUANTAQ_API_KEY}:`).toString('base64');
                const qUrl = `https://api.quantaq.com/device-api/v1/devices/${compId}/data-raw/?limit=100`;
                
                const qRes = await fetch(qUrl, {
                    headers: { 'Authorization': `Basic ${auth}` }
                });
                
                const qData = await qRes.json();
                
                // Format QuantAQ response to match dashboard expectations
                if (qData && qData.data) {
                    const formatted = qData.data.map(entry => ({
                        time: new Date(entry.timestamp).getTime(),
                        pm25: entry.pm25 || entry.pm2_5 || 0
                    }));
                    return res.json(formatted);
                }
                return res.json([]);

            // 3. C12 / GROVE DATA
            case 'grove_history':
                // Replace with your specific Grove/C12 data endpoint if different
                const gUrl = `https://api.grove.id/v1/devices/${compId}/points?limit=100`;
                const gRes = await fetch(gUrl); 
                const gData = await gRes.json();
                return res.json(gData);

            default:
                return res.status(400).json({ error: "unknown_action", received: action });
        }
    } catch (err) {
        console.error("Proxy Error:", err);
        return res.status(500).json({ error: "Internal server error" });
    }
};
