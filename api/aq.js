/**
 * EJAT Network Proxy - Vercel Optimized
 * Matches dashboard.html logic for PurpleAir, QuantAQ, and GroveStreams.
 */

module.exports = async (req, res) => {
    // 1. Set Headers for CORS and JSON
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    const { action, id, compId, start } = req.query;

    try {
        // --- ROUTE 1: PURPLEAIR SENSOR LIST (The Box) ---
        if (action === 'purpleair_box') {
            const url = `https://api.purpleair.com/v1/sensors?fields=latitude,longitude,name&nwlat=38.96&nwlng=-76.96&selat=38.89&selng=-76.88`;
            const response = await fetch(url, {
                headers: { 'X-API-Key': process.env.PURPLEAIR_KEY }
            });
            const data = await response.json();
            return res.status(200).json(data);
        }

        // --- ROUTE 2: PURPLEAIR HISTORY (Circles) ---
        if (action === 'purpleair_history') {
            const url = `https://api.purpleair.com/v1/sensors/${id}/history?start_timestamp=${start}&fields=pm2.5_atm`;
            const response = await fetch(url, {
                headers: { 'X-API-Key': process.env.PURPLEAIR_KEY }
            });
            const data = await response.json();
            return res.status(200).json(data);
        }

        // --- ROUTE 3: QUANTAQ HISTORY (Triangles - MOD units) ---
        if (action === 'quantaq_history') {
            const auth = Buffer.from(`${process.env.QUANTAQ_KEY}:`).toString('base64');
            const url = `https://api.quantaq.com/device-api/v1/devices/${compId}/data-raw/?limit=100`;
            
            const response = await fetch(url, {
                headers: { 'Authorization': `Basic ${auth}` }
            });
            const data = await response.json();
            
            // Format to match: samples[i].pm25 and samples[i].time
            const formatted = (data.data || []).map(entry => ({
                time: new Date(entry.timestamp).getTime(),
                pm25: entry.pm25 || entry.pm2_5 || 0
            }));
            return res.status(200).json(formatted);
        }

        // --- ROUTE 4: GROVESTREAMS HISTORY (Triangles - C12 units) ---
        if (action === 'grove_history') {
            // Updated to use GroveStreams Feed API for the C12 pm25 streams
            const url = `https://grovestreams.com/api/feed?compid=${compId}&streamid=pm25&limit=100`;
            const response = await fetch(url);
            const data = await response.json();

            // Map GroveStreams [[time, val]] to {time, data}
            const formatted = (data.data || []).map(point => ({
                time: point[0], 
                data: point[1]
            }));
            return res.status(200).json(formatted);
        }

        return res.status(400).json({ error: "unknown_action", action });

    } catch (error) {
        console.error("Vercel Proxy Error:", error.message);
        return res.status(500).json({ error: "Server Error: " + error.message });
    }
};
