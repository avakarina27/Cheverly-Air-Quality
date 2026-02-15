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
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");
      return {
        "Accept": "application/json",
        "Authorization": `Basic ${auth}`
      };
    };

    const normSn = (s) => String(s || "")
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, ""); // remove dashes/spaces/underscores

    const getQuantDevices = async () => {
      const headers = quantAuthHeaders();
      // We request a larger page size to reduce pagination headaches.
      // If you have more than 200 devices, we can paginate later.
      const url = "https://api.quant-aq.com/v1/devices?per_page=200&page=1";
      return await fetchJson(url, { headers });
    };

    // Optional debugging endpoint: list devices you actually have access to
    if (action === "quantaq_devices") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });
      const out = await getQuantDevices();
      return res.status(out.status).json(out.data);
    }

    const quantDataByDate = async (sn, date) => {
      const headers = quantAuthHeaders();
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;
      return await fetchJson(url, { headers });
    };

    // ------------------------
    // QuantAQ: data-by-date with auto-serial-resolution
    // ------------------------
    if (action === "quantaq_by_date") {
      if (!QUANTAQ_KEY) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const snInput = req.query.sn;
      const date = req.query.date;
      if (!snInput || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      // 1) Try exactly as provided
      let out = await quantDataByDate(snInput, date);
      if (out.ok) return res.status(200).json(out.data);

      // 2) If it's a 404, try to resolve sn by listing devices and matching normalized serials
      if (out.status === 404) {
        const devs = await getQuantDevices();

        // If listing devices fails, return what we know
        if (!devs.ok) {
          return res.status(devs.status).json({
            error: "quantaq_devices_list_failed",
            details: devs.data,
            original_try: { sn: snInput, date, status: out.status, details: out.data }
          });
        }

        const list = Array.isArray(devs.data?.data) ? devs.data.data : [];
        const target = normSn(snInput);

        // Look for exact normalized match
        let match = list.find(d => normSn(d?.sn) === target);

        // If no match, try a looser match: sometimes leading zeros differ (001616 vs 1616)
        if (!match) {
          const stripZeros = (x) => normSn(x).replace(/0+/g, "0").replace(/0([1-9])/g, "$1");
          const targetLoose = stripZeros(snInput);
          match = list.find(d => stripZeros(d?.sn) === targetLoose);
        }

        if (match?.sn) {
          const retry = await quantDataByDate(match.sn, date);
          if (retry.ok) {
            return res.status(200).json({
              resolved_sn: match.sn,
              data: retry.data
            });
          }

          return res.status(retry.status).json({
            error: "quantaq_retry_failed",
            resolved_sn: match.sn,
            details: retry.data
          });
        }

        // No match at all: return a few serials so you can update SPODS correctly
        const sample = list.slice(0, 15).map(d => d?.sn).filter(Boolean);

        return res.status(404).json({
          error: "quantaq_sn_not_found_for_key",
          provided_sn: snInput,
          hint: "Your QuantAQ key does not see a device with that serial. Use one of the returned sns in your SPODS list.",
          sample_sns: sample,
          total_visible_devices: list.length
        });
      }

      // Any non-404 errors just return directly
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
