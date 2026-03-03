export default async function handler(req, res) {
  try {
    const action = req.query.action;

    const PURPLEAIR_KEY = process.env.PURPLEAIR_API_KEY;
    const QUANTAQ_KEY = process.env.QUANTAQ_API_KEY;

    const GROVE_KEY = process.env.GROVESTREAMS_API_KEY;
    const GROVE_ORG = process.env.GROVESTREAMS_ORG;

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
      if (!PURPLEAIR_KEY) {
        return { ok: false, status: 500, data: { error: "missing_PURPLEAIR_API_KEY" } };
      }

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
      if (!QUANTAQ_KEY) return null;
      const auth = Buffer.from(`${QUANTAQ_KEY}:`).toString("base64");
      return {
        "Accept": "application/json",
        "Authorization": `Basic ${auth}`
      };
    };

    const normSn = (s) => String(s || "")
      .toUpperCase()
      .replace(/[^A-Z0-9]/g, "");

    const getQuantDevices = async () => {
      const headers = quantAuthHeaders();
      if (!headers) return { ok: false, status: 500, data: { error: "missing_QUANTAQ_API_KEY" } };
      const url = "https://api.quant-aq.com/v1/devices?per_page=200&page=1";
      return await fetchJson(url, { headers });
    };

    if (action === "quantaq_devices") {
      const out = await getQuantDevices();
      return res.status(out.status).json(out.data);
    }

    const quantDataByDate = async (sn, date) => {
      const headers = quantAuthHeaders();
      if (!headers) return { ok: false, status: 500, data: { error: "missing_QUANTAQ_API_KEY" } };
      const url = `https://api.quant-aq.com/v1/devices/${encodeURIComponent(sn)}/data-by-date/${encodeURIComponent(date)}/`;
      return await fetchJson(url, { headers });
    };

    // QuantAQ: data-by-date with serial resolution
    if (action === "quantaq_by_date") {
      const snInput = req.query.sn;
      const date = req.query.date;
      if (!snInput || !date) return res.status(400).json({ error: "missing_sn_or_date" });

      let out = await quantDataByDate(snInput, date);
      if (out.ok) return res.status(200).json(out.data);

      // If 404, attempt resolution by listing devices
      if (out.status === 404) {
        const devs = await getQuantDevices();
        if (!devs.ok) {
          return res.status(devs.status).json({
            error: "quantaq_devices_list_failed",
            details: devs.data,
            original_try: { sn: snInput, date, status: out.status, details: out.data }
          });
        }

        const list = Array.isArray(devs.data?.data) ? devs.data.data : [];
        const target = normSn(snInput);
        let match = list.find(d => normSn(d?.sn) === target);

        if (!match) {
          const stripZeros = (x) => normSn(x).replace(/^0+/, "");
          const targetLoose = stripZeros(snInput);
          match = list.find(d => stripZeros(d?.sn) === targetLoose);
        }

        if (match?.sn) {
          const retry = await quantDataByDate(match.sn, date);
          if (retry.ok) return res.status(200).json({ resolved_sn: match.sn, data: retry.data });

          return res.status(retry.status).json({
            error: "quantaq_retry_failed",
            resolved_sn: match.sn,
            details: retry.data
          });
        }

        return res.status(404).json({
          error: "quantaq_sn_not_found_for_key",
          provided_sn: snInput,
          total_visible_devices: list.length,
          sample_sns: list.slice(0, 15).map(d => d?.sn).filter(Boolean)
        });
      }

      return res.status(out.status).json({
        error: "quantaq_failed",
        status: out.status,
        details: out.data
      });
    }

    // ------------------------
    // QuantAQ: range endpoint (CHUNKED to <= 31 days per request)
    // This is what you need for annual and long windows.
    // Query:
    //   action=quantaq_range&sn=MOD-00536&start_date=2025-03-01&end_date=2026-03-01
    // Returns: { sn, chunks: [...], data: [...] }
    // ------------------------
    if (action === "quantaq_range") {
      const sn = req.query.sn;
      const start_date = req.query.start_date;
      const end_date = req.query.end_date;

      if (!sn || !start_date || !end_date) {
        return res.status(400).json({ error: "missing_sn_or_dates" });
      }

      const headers = quantAuthHeaders();
      if (!headers) return res.status(500).json({ error: "missing_QUANTAQ_API_KEY" });

      const start = new Date(`${start_date}T00:00:00Z`);
      const end = new Date(`${end_date}T00:00:00Z`);
      if (!Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || start >= end) {
        return res.status(400).json({ error: "invalid_dates" });
      }

      const dayMs = 86400000;
      const maxDays = 31;
      const chunks = [];

      let cur = start.getTime();
      while (cur < end.getTime()) {
        const next = Math.min(cur + maxDays * dayMs, end.getTime());
        const a = new Date(cur);
        const b = new Date(next);

        const aStr = a.toISOString().slice(0, 10);
        const bStr = b.toISOString().slice(0, 10);

        // QuantAQ’s API surface varies by account; we route through data-by-date
        // because we know it works for you. We chunk by day and merge.
        chunks.push({ start_date: aStr, end_date: bStr });
        cur = next;
      }

      const merged = [];
      for (const c of chunks) {
        // day-by-day within each chunk
        let d = new Date(`${c.start_date}T00:00:00Z`).getTime();
        const dEnd = new Date(`${c.end_date}T00:00:00Z`).getTime();

        while (d < dEnd) {
          const day = new Date(d).toISOString().slice(0, 10);
          const out = await quantDataByDate(sn, day);

          if (!out.ok) {
            return res.status(out.status).json({
              error: "quantaq_range_failed",
              sn,
              day,
              status: out.status,
              details: out.data
            });
          }

          // data-by-date returns {data:[...], meta:{...}} (your earlier output)
          if (Array.isArray(out.data?.data)) merged.push(...out.data.data);
          d += dayMs;
        }
      }

      return res.status(200).json({ sn, chunks, data: merged });
    }

    // ------------------------
    // GroveStreams (C12): last value
    // Query:
    //   action=grove_last&compId=D14645
    // Uses GROVESTREAMS_API_KEY + GROVESTREAMS_ORG from env.
    // ------------------------
    if (action === "grove_last") {
      const compId = req.query.compId;
      if (!compId) return res.status(400).json({ error: "missing_compId" });
      if (!GROVE_KEY) return res.status(500).json({ error: "missing_GROVESTREAMS_API_KEY" });
      if (!GROVE_ORG) return res.status(500).json({ error: "missing_GROVESTREAMS_ORG" });

      const url =
        `https://grovestreams.com/api/comp/${encodeURIComponent(compId)}/last_value` +
        `?retStreamId&org=${encodeURIComponent(GROVE_ORG)}` +
        `&api_key=${encodeURIComponent(GROVE_KEY)}`;

      const out = await fetchJson(url);
      if (!out.ok) {
        return res.status(out.status).json({
          error: "grove_last_failed",
          status: out.status,
          url,
          details: out.data
        });
      }
      return res.status(200).json(out.data);
    }

    // ------------------------
    // GroveStreams (C12): history
    // Query:
    //   action=grove_history&compId=D14645&start=1771446780&end=1771533180
    // epoch seconds
    // ------------------------
    if (action === "grove_history") {
      const compId = req.query.compId;
      const start = req.query.start;
      const end = req.query.end;
      if (!compId || !start || !end) return res.status(400).json({ error: "missing_compId_or_start_or_end" });
      if (!GROVE_KEY) return res.status(500).json({ error: "missing_GROVESTREAMS_API_KEY" });
      if (!GROVE_ORG) return res.status(500).json({ error: "missing_GROVESTREAMS_ORG" });

      const url =
        `https://grovestreams.com/api/comp/${encodeURIComponent(compId)}/stream` +
        `?org=${encodeURIComponent(GROVE_ORG)}` +
        `&api_key=${encodeURIComponent(GROVE_KEY)}` +
        `&startDate=${encodeURIComponent(Number(start) * 1000)}` +
        `&endDate=${encodeURIComponent(Number(end) * 1000)}` +
        `&retStreamId=true`;

      const out = await fetchJson(url);
      if (!out.ok) {
        return res.status(out.status).json({
          error: "grove_history_failed",
          status: out.status,
          url,
          details: out.data
        });
      }
      return res.status(200).json(out.data);
    }

    return res.status(404).json({ error: "unknown_action", action });
  } catch (err) {
    return res.status(500).json({ error: "server_error", detail: String(err) });
  }
}
