/**
 * EJAT Network Proxy - FINAL VERSION
 * Handles: PurpleAir, QuantAQ, and GroveStreams (C12)
 * Environment Variables required in Vercel:
 * - PURPLEAIR_KEY
 * - QUANTAQ_KEY
 */

module.exports = async (req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    const { action, id, compId, start } = req.query;

    try {
        // --- 1. PURPLEAIR BOX (Initial Map Load) ---
        if (action === 'purpleair_box') {
            const response = await fetch(`https://api.purpleair.com/v1/sensors?fields=latitude,longitude,name&nwlat=38.96&nwlng=-76.96&selat=38.89&selng=-76.88`, {
                headers: { 'X-API-Key': process.env.PURPLEAIR_KEY }
            });
            const data = await response.json();
            return res.status(200).json(data);
        }

        // --- 2. PURPLEAIR HISTORY (Circles) ---
        if (action === 'purpleair_history') {
            const response = await fetch(`https://api.purpleair.com/v1/sensors/${id}/history?start_timestamp=${start}&fields=pm2.5_atm`, {
                headers: { 'X-API-Key': process.env.PURPLEAIR_KEY }
            });
            const data = await response.json();
            return res.status(200).json(data);
        }

        // --- 3. QUANTAQ HISTORY (SPODS/Triangles) ---
        if (action === 'quantaq_history') {
            const auth = Buffer.from(`${process.env.QUANTAQ_KEY}:`).toString('base64');
            const response = await fetch(`https://api.quantaq.com/device-api/v1/devices/${compId}/data-raw/?limit=100`, {
                headers: { 'Authorization': `Basic ${auth}` }
            });
            const data = await response.json();
            
            // Standardize QuantAQ format
            const formatted = (data.data || []).map(entry => ({
                time: new Date(entry.timestamp).getTime(),
                pm25: entry.pm25 || entry.pm2_5 || 0
            }));
            return res.status(200).json(formatted);
        }

        // --- 4. GROVESTREAMS HISTORY (C12s/Triangles) ---
        if (action === 'grove_history') {
            // compId here is your GroveStreams Component ID
            const response = await fetch(`https://grovestreams.com/api/feed?compid=${compId}&streamid=pm25&limit=100`);
            const data = await response.json();

            // GroveStreams typically returns { data: [[time, val], [time, val]...] }
            // We map it to our dashboard's standard: { time, pm25 }
            const formatted = (data.data || []).map(point => ({
                time: point[0], 
                pm25: point[1]
            }));
            return res.status(200).json(formatted);
        }

        return res.status(400).json({ error: "unknown_action", action });

    } catch (error) {
        console.error("Vercel Proxy Error:", error.message);
        return res.status(500).json({ error: "Server Error: " + error.message });
    }
};
