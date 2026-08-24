import pg from "pg";

const { Pool } = pg;

const pool =
  globalThis.cheverlyDbPool ||
  new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
    max: 1,
    min: 0,
    idleTimeoutMillis: 5000,
    connectionTimeoutMillis: 10000,
    allowExitOnIdle: true
  });

globalThis.cheverlyDbPool = pool;

export const config = { runtime: "nodejs" };

const AIRNOW_REFERENCE_SITES = [
  {
    key: "hu_beltsville",
    name: "Howard University Beltsville",
    agency: "Maryland Department of the Environment",
    latitude: 39.055302,
    longitude: -76.878304
  },
  {
    key: "river_terrace",
    name: "River Terrace",
    agency: "District Department of Energy and Environment",
    latitude: 38.895683,
    longitude: -76.958089
  },
  {
    key: "dc_near_road",
    name: "DC Near Road",
    agency: "District Department of Energy and Environment",
    latitude: 38.894749,
    longitude: -76.953427
  }
];

const AIRNOW_BBOX = "-76.98,38.88,-76.85,39.07";
const AIRNOW_CACHE_MS = 15 * 60 * 1000;

const airNowCache =
  globalThis.cheverlyAirNowCache ||
  new Map();

globalThis.cheverlyAirNowCache = airNowCache;

function formatAirNowHour(date) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");
  const hour = String(date.getUTCHours()).padStart(2, "0");
  return `${year}-${month}-${day}T${hour}`;
}

function normalizeAirNowTimestamp(value) {
  if (!value) return null;

  const text = String(value).trim();

  const withSeconds =
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(text)
      ? `${text}:00Z`
      : text.endsWith("Z")
        ? text
        : `${text}Z`;

  const date = new Date(withSeconds);

  return Number.isFinite(date.getTime())
    ? date.toISOString()
    : null;
}

function coordinatesMatch(
  row,
  site,
  tolerance = 0.0005
) {
  const latitude = Number(row?.Latitude);
  const longitude = Number(row?.Longitude);

  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    Math.abs(latitude - site.latitude) <= tolerance &&
    Math.abs(longitude - site.longitude) <= tolerance
  );
}

function findAirNowReferenceSite(row) {
  return (
    AIRNOW_REFERENCE_SITES.find(site =>
      coordinatesMatch(row, site)
    ) || null
  );
}

function parseAirNowReferenceRows(rows) {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows
    .map(row => {
      const site =
        findAirNowReferenceSite(row);

      if (!site) return null;

      const parameter = String(
        row?.Parameter || ""
      )
        .toUpperCase()
        .replace(/[^A-Z0-9.]/g, "");

      const unit = String(
        row?.Unit || ""
      ).toUpperCase();

      const value = Number(row?.Value);

      const timestamp =
        normalizeAirNowTimestamp(
          row?.UTC
        );

      if (
        parameter !== "PM2.5" &&
        parameter !== "PM25"
      ) {
        return null;
      }

      if (unit !== "UG/M3") {
        return null;
      }

      if (
        !Number.isFinite(value) ||
        value < 0
      ) {
        return null;
      }

      if (!timestamp) {
        return null;
      }

      return {
        siteKey: site.key,
        siteName: site.name,
        agency: site.agency,
        timestamp,
        value,
        units: "µg/m³"
      };
    })
    .filter(Boolean)
    .sort(
      (a, b) =>
        new Date(a.timestamp) -
        new Date(b.timestamp)
    );
}

// --------------------------------------------------
// Cached map snapshot helpers
// --------------------------------------------------

const MAP_SNAPSHOT_CACHE_MS =
  5 * 60 * 1000;

let mapSnapshotCache =
  globalThis.cheverlyMapSnapshotCache || {
    data: null,
    expiresAt: 0
  };

globalThis.cheverlyMapSnapshotCache =
  mapSnapshotCache;

const MAP_PURPLEAIR_SENSORS = {
  "52823": "EJAT.CV.6.52823",
  "53677": "EJAT.CV.1.53677",
  "54293": "EJAT.CV.2.54293",
  "57777": "EJAT.CV.1.57777",
  "57783": "EJAT.CV.6.57783",
  "57811": "EJAT.CV.4.57811",
  "57841": "EJAT.CV.3.57841",
  "203577": "EJAT.CV.2.203577",
  "203601": "EJAT.CV.1.203601",
  "207729": "EJAT.CV.1.207729",
  "211993": "EJAT.CV.3.211993",
  "156595": "EJAT.CV.156595",
  "284362": "EJAT.PG.1.284362",
  "160037": "EJAT.PG.1.160037",
  "175563": "EJAT.PG.1.175563",
  "178169": "EJAT.PG.1.178169",
  "184191": "EJAT.PG.1.184191",
  "197937": "EJAT.PG.1.197937",
  "218227": "EJAT.PG.1.218227",
  "218237": "EJAT.PG.1.218237",
  "218273": "EJAT.PG.1.218273",
  "57955": "EJAT.FH.1.57955",
  "185085": "EJAT.FH.1.185085",
  "203597": "EJAT.FH.1.203597",
  "181253": "EJAT.CH.1.181253"
};

const MAP_SPODS = [
  {
    sn: "MOD-00536",
    name: "QuantAQ MOD-00536",
    lat: 38.920610,
    lon: -76.919770
  },
  {
    sn: "MOD-00745",
    name: "QuantAQ MOD-00745",
    lat: 38.905100,
    lon: -76.910000
  },
  {
    sn: "MOD-00746",
    name: "QuantAQ MOD-00746",
    lat: 39.143200,
    lon: -76.846000
  },
  {
    sn: "MOD-00747",
    name: "QuantAQ MOD-00747",
    lat: 39.233944,
    lon: -76.504501
  },
  {
    sn: "MOD-00748",
    name: "QuantAQ MOD-00748",
    lat: 38.969685,
    lon: -76.542290
  },
  {
    sn: "MOD-00749",
    name: "QuantAQ MOD-00749",
    lat: 39.055400,
    lon: -76.878800
  },
  {
    sn: "MOD-00537",
    name: "QuantAQ MOD-00537 – Turner Station North",
    lat: null,
    lon: null
  }
];

const MAP_C12_IDS = [
  "D14645",
  "D17615",
  "E10588",
  "D14646",
  "B19939",
  "D14648",
  "D11556"
];

const MAP_BP_PM25_24H = [
  {
    cLow: 0.0,
    cHigh: 12.0,
    iLow: 0,
    iHigh: 50
  },
  {
    cLow: 12.1,
    cHigh: 35.4,
    iLow: 51,
    iHigh: 100
  },
  {
    cLow: 35.5,
    cHigh: 55.4,
    iLow: 101,
    iHigh: 150
  },
  {
    cLow: 55.5,
    cHigh: 150.4,
    iLow: 151,
    iHigh: 200
  },
  {
    cLow: 150.5,
    cHigh: 250.4,
    iLow: 201,
    iHigh: 300
  },
  {
    cLow: 250.5,
    cHigh: 500.4,
    iLow: 301,
    iHigh: 500
  }
];

const MAP_BP_PM10_24H = [
  {
    cLow: 0,
    cHigh: 54,
    iLow: 0,
    iHigh: 50
  },
  {
    cLow: 55,
    cHigh: 154,
    iLow: 51,
    iHigh: 100
  },
  {
    cLow: 155,
    cHigh: 254,
    iLow: 101,
    iHigh: 150
  },
  {
    cLow: 255,
    cHigh: 354,
    iLow: 151,
    iHigh: 200
  },
  {
    cLow: 355,
    cHigh: 424,
    iLow: 201,
    iHigh: 300
  },
  {
    cLow: 425,
    cHigh: 604,
    iLow: 301,
    iHigh: 500
  }
];

const MAP_BP_O3_8H = [
  {
    cLow: 0,
    cHigh: 54,
    iLow: 0,
    iHigh: 50
  },
  {
    cLow: 55,
    cHigh: 70,
    iLow: 51,
    iHigh: 100
  },
  {
    cLow: 71,
    cHigh: 85,
    iLow: 101,
    iHigh: 150
  },
  {
    cLow: 86,
    cHigh: 105,
    iLow: 151,
    iHigh: 200
  },
  {
    cLow: 106,
    cHigh: 200,
    iLow: 201,
    iHigh: 300
  }
];

const MAP_BP_CO_8H_PPM = [
  {
    cLow: 0.0,
    cHigh: 4.4,
    iLow: 0,
    iHigh: 50
  },
  {
    cLow: 4.5,
    cHigh: 9.4,
    iLow: 51,
    iHigh: 100
  },
  {
    cLow: 9.5,
    cHigh: 12.4,
    iLow: 101,
    iHigh: 150
  },
  {
    cLow: 12.5,
    cHigh: 15.4,
    iLow: 151,
    iHigh: 200
  },
  {
    cLow: 15.5,
    cHigh: 30.4,
    iLow: 201,
    iHigh: 300
  },
  {
    cLow: 30.5,
    cHigh: 50.4,
    iLow: 301,
    iHigh: 500
  }
];

const MAP_BP_NO2_1H = [
  {
    cLow: 0,
    cHigh: 53,
    iLow: 0,
    iHigh: 50
  },
  {
    cLow: 54,
    cHigh: 100,
    iLow: 51,
    iHigh: 100
  },
  {
    cLow: 101,
    cHigh: 360,
    iLow: 101,
    iHigh: 150
  },
  {
    cLow: 361,
    cHigh: 649,
    iLow: 151,
    iHigh: 200
  },
  {
    cLow: 650,
    cHigh: 1249,
    iLow: 201,
    iHigh: 300
  },
  {
    cLow: 1250,
    cHigh: 2049,
    iLow: 301,
    iHigh: 500
  }
];

function mapSafeNum(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : null;
}

function mapActiveNum(value) {
  const number =
    mapSafeNum(value);

  return number !== null &&
    number > 0
    ? number
    : null;
}

function mapHasCoords(lat, lon) {
  const latitude = Number(lat);
  const longitude = Number(lon);

  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude !== 0 &&
    longitude !== 0
  );
}

function mapAQIFromBreakpoints(
  concentration,
  table
) {
  const value =
    mapSafeNum(concentration);

  if (value === null) {
    return null;
  }

  for (const bp of table) {
    if (
      value >= bp.cLow &&
      value <= bp.cHigh
    ) {
      return Math.round(
        (
          (bp.iHigh - bp.iLow) /
          (bp.cHigh - bp.cLow)
        ) *
          (value - bp.cLow) +
          bp.iLow
      );
    }
  }

  return null;
}

function mapAQIFromBC(value) {
  const bc =
    mapActiveNum(value);

  return bc === null
    ? null
    : Math.round(
        (bc / 600) * 100
      );
}

function mapAQIFromDPM(value) {
  const bc =
    mapActiveNum(value);

  return bc === null
    ? null
    : Math.round(
        ((bc * 1.25) / 330) *
          100
      );
}

function mapHaversineKm(
  lat1,
  lon1,
  lat2,
  lon2
) {
  const earthRadiusKm = 6371;

  const dLat =
    ((lat2 - lat1) * Math.PI) /
    180;

  const dLon =
    ((lon2 - lon1) * Math.PI) /
    180;

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(
      (lat1 * Math.PI) / 180
    ) *
      Math.cos(
        (lat2 * Math.PI) / 180
      ) *
      Math.sin(dLon / 2) ** 2;

  return (
    earthRadiusKm *
    2 *
    Math.atan2(
      Math.sqrt(a),
      Math.sqrt(1 - a)
    )
  );
}

function mapAddAQI(
  pollutants,
  label,
  aqi
) {
  const value =
    mapSafeNum(aqi);

  if (
    value === null ||
    value <= 0
  ) {
    return;
  }

  pollutants.push({
    label,
    aqi: Math.round(value)
  });
}

function mapAddQuantPollutants(
  pollutants,
  data,
  label,
  includePM25 = true
) {
  if (!data) return;

  const pm25 = mapActiveNum(
    data.pm25_24h ??
    data.pm25
  );

  const pm10 = mapActiveNum(
    data.pm10_24h ??
    data.pm10
  );

  const ozone = mapActiveNum(
    data.o3_highest_8h ??
    data.o3
  );

  const coPpb = mapActiveNum(
    data.co_highest_8h ??
    data.co
  );

  const no2 = mapActiveNum(
    data.no2_highest_1h ??
    data.no2
  );

  if (includePM25) {
    mapAddAQI(
      pollutants,
      `PM2.5 (${label})`,
      mapAQIFromBreakpoints(
        pm25,
        MAP_BP_PM25_24H
      )
    );
  }

  mapAddAQI(
    pollutants,
    `O₃ (${label})`,
    mapAQIFromBreakpoints(
      ozone,
      MAP_BP_O3_8H
    )
  );

  mapAddAQI(
    pollutants,
    `CO (${label})`,
    mapAQIFromBreakpoints(
      coPpb !== null
        ? coPpb / 1000
        : null,
      MAP_BP_CO_8H_PPM
    )
  );

  mapAddAQI(
    pollutants,
    `NO₂ (${label})`,
    mapAQIFromBreakpoints(
      no2,
      MAP_BP_NO2_1H
    )
  );

  mapAddAQI(
    pollutants,
    `PM10 (${label})`,
    mapAQIFromBreakpoints(
      pm10,
      MAP_BP_PM10_24H
    )
  );
}

function mapAddC12Pollutants(
  pollutants,
  bc,
  label
) {
  const value =
    mapActiveNum(bc);

  if (value === null) {
    return;
  }

  mapAddAQI(
    pollutants,
    `BC non-cancer (${label})`,
    mapAQIFromBC(value)
  );

  mapAddAQI(
    pollutants,
    `DPM cancer (${label})`,
    mapAQIFromDPM(value)
  );
}

function mapCalcCEAQI(pollutants) {
  const valid = pollutants
    .map(item => ({
      ...item,
      aqi: Math.round(
        Number(item.aqi)
      )
    }))
    .filter(
      item =>
        Number.isFinite(item.aqi) &&
        item.aqi > 0
    );

  if (!valid.length) {
    return {
      ceaqi: 0,
      baseLabel: "No data"
    };
  }

  const baseItem =
    valid.reduce(
      (best, item) =>
        item.aqi > best.aqi
          ? item
          : best,
      valid[0]
    );

  const base =
    baseItem.aqi;

  const adders =
    valid.reduce(
      (sum, item) => {
        if (item === baseItem) {
          return sum;
        }

        if (item.aqi >= 120) {
          return (
            sum +
            Math.round(
              base * 0.03
            )
          );
        }

        if (item.aqi >= 100) {
          return (
            sum +
            Math.round(
              base * 0.02
            )
          );
        }

        if (item.aqi >= 80) {
          return (
            sum +
            Math.round(
              base * 0.01
            )
          );
        }

        return sum;
      },
      0
    );

  return {
    ceaqi: base + adders,
    baseLabel: baseItem.label
  };
}

function mapNearestQuant(
  lat,
  lon,
  quantRows
) {
  const candidates =
    MAP_SPODS
      .map(sensor => ({
        ...sensor,
        data:
          quantRows.get(
            sensor.sn
          ) || null
      }))
      .filter(
        sensor =>
          sensor.data
      );

  if (!candidates.length) {
    return null;
  }

  if (!mapHasCoords(lat, lon)) {
    return candidates[0];
  }

  return (
    candidates.reduce(
      (best, sensor) => {
        if (
          !mapHasCoords(
            sensor.lat,
            sensor.lon
          )
        ) {
          return best;
        }

        const distance =
          mapHaversineKm(
            lat,
            lon,
            sensor.lat,
            sensor.lon
          );

        if (
          !best ||
          distance <
            best.distance
        ) {
          return {
            ...sensor,
            distance
          };
        }

        return best;
      },
      null
    ) || candidates[0]
  );
}

function mapNearestC12(
  lat,
  lon,
  c12Rows
) {
  const candidates =
    MAP_C12_IDS
      .map(compId => ({
        compId,
        data:
          c12Rows.get(
            compId
          ) || null
      }))
      .filter(
        sensor =>
          sensor.data &&
          mapActiveNum(
            sensor.data
              .bc_available_average
          ) !== null
      );

  if (!candidates.length) {
    return null;
  }

  if (!mapHasCoords(lat, lon)) {
    return candidates[0];
  }

  return (
    candidates.reduce(
      (best, sensor) => {
        const sensorLat =
          sensor.data?.latitude;

        const sensorLon =
          sensor.data?.longitude;

        if (
          !mapHasCoords(
            sensorLat,
            sensorLon
          )
        ) {
          return best;
        }

        const distance =
          mapHaversineKm(
            lat,
            lon,
            sensorLat,
            sensorLon
          );

        if (
          !best ||
          distance <
            best.distance
        ) {
          return {
            ...sensor,
            distance
          };
        }

        return best;
      },
      null
    ) || candidates[0]
  );
}

export default async function handler(
  req,
  res
) {
  try {
    const action =
      req.query.action;

    const PURPLEAIR_KEY =
      process.env
        .PURPLEAIR_API_KEY;

    const QUANTAQ_KEY =
      process.env
        .QUANTAQ_API_KEY;

    const GROVE_KEY =
      process.env
        .GROVESTREAMS_API_KEY;

    const AIRNOW_KEY =
      process.env
        .AIRNOW_API_KEY;

    if (!action) {
      return res
        .status(400)
        .json({
          error: "missing_action"
        });
    }

    const fetchJson = async (
      url,
      options = {}
    ) => {
      const response =
        await fetch(
          url,
          options
        );

      const text =
        await response.text();

      let data = text;

      try {
        data =
          JSON.parse(text);
      } catch {
        // Keep plain text when the response is not JSON.
      }

      return {
        ok: response.ok,
        status: response.status,
        data
      };
    };

    // --------------------------------------------------
    // Cached CE-AQI map snapshot
    // --------------------------------------------------

    if (action === "map_snapshot") {
      if (!PURPLEAIR_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_PURPLEAIR_API_KEY"
          });
      }

      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const now =
        Date.now();

      if (
        mapSnapshotCache.data &&
        now <
          mapSnapshotCache
            .expiresAt
      ) {
        res.setHeader(
          "Cache-Control",
          "s-maxage=300, stale-while-revalidate=900"
        );

        return res
          .status(200)
          .json(
            mapSnapshotCache.data
          );
      }

      // --------------------------------------------------
      // Resolve missing coordinates for newly added stations
      // --------------------------------------------------

      const dynamicQuantCoords = new Map();
      const dynamicC12Coords = new Map();

      if (QUANTAQ_KEY) {
        try {
          const quantAuth =
            Buffer.from(
              `${QUANTAQ_KEY}:`
            ).toString(
              "base64"
            );

          const devicesOutput =
            await fetchJson(
              "https://api.quant-aq.com/v1/devices?per_page=200&page=1",
              {
                headers: {
                  Accept:
                    "application/json",

                  Authorization:
                    `Basic ${quantAuth}`
                }
              }
            );

          const devices =
            Array.isArray(
              devicesOutput
                ?.data?.data
            )
              ? devicesOutput
                  .data.data
              : [];

          for (
            const device of
            devices
          ) {
            const sn =
              String(
                device?.sn ||
                ""
              );

            if (!sn) {
              continue;
            }

            const lat =
              mapSafeNum(
                device
                  ?.latitude ??
                device?.lat ??
                device
                  ?.location
                  ?.latitude ??
                device
                  ?.location
                  ?.lat
              );

            const lon =
              mapSafeNum(
                device
                  ?.longitude ??
                device?.lon ??
                device?.lng ??
                device
                  ?.location
                  ?.longitude ??
                device
                  ?.location
                  ?.lon ??
                device
                  ?.location
                  ?.lng
              );

            if (
              mapHasCoords(
                lat,
                lon
              )
            ) {
              dynamicQuantCoords
                .set(
                  sn,
                  {
                    lat,
                    lon
                  }
                );
            }
          }
        } catch (error) {
          console.warn(
            "Map snapshot: could not resolve QuantAQ coordinates:",
            error
          );
        }
      }

      if (GROVE_KEY) {
        await Promise.all(
          MAP_C12_IDS.map(
            async compId => {
              try {
                const url =
                  `https://grovestreams.com/api/comp/` +
                  `${encodeURIComponent(
                    compId
                  )}` +
                  `/last_value` +
                  `?retStreamId` +
                  `&api_key=${encodeURIComponent(
                    GROVE_KEY
                  )}`;

                const output =
                  await fetchJson(
                    url
                  );

                const rows =
                  Array.isArray(
                    output?.data
                  )
                    ? output.data
                    : [];

                const lat =
                  mapSafeNum(
                    rows.find(
                      row =>
                        row
                          ?.streamId ===
                        "lat"
                    )?.data
                  );

                const lon =
                  mapSafeNum(
                    rows.find(
                      row =>
                        row
                          ?.streamId ===
                        "long"
                    )?.data
                  );

                if (
                  mapHasCoords(
                    lat,
                    lon
                  )
                ) {
                  dynamicC12Coords
                    .set(
                      compId,
                      {
                        lat,
                        lon
                      }
                    );
                }
              } catch (error) {
                console.warn(
                  `Map snapshot: could not resolve C-12 coordinates for ${compId}:`,
                  error
                );
              }
            }
          )
        );
      }

      const purpleUrl =
        "https://api.purpleair.com/v1/sensors" +
        "?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75" +
        "&fields=sensor_index,latitude,longitude,pm2.5_atm";

      const purplePromise =
        fetchJson(
          purpleUrl,
          {
            headers: {
              "X-API-Key":
                PURPLEAIR_KEY
            }
          }
        );

      const quantPromise =
        pool.query(
          `
          WITH latest AS (
            SELECT
              sensor_sn,
              MAX(time_stamp) AS latest_time
            FROM quantaq_master
            WHERE sensor_sn = ANY($1::text[])
            GROUP BY sensor_sn
          ),

          bounded AS (
            SELECT
              q.sensor_sn,
              q.time_stamp,
              q.pm25,
              q.pm10,
              q.o3,
              q.co,
              q.no2,
              l.latest_time
            FROM quantaq_master q
            JOIN latest l
              ON l.sensor_sn = q.sensor_sn
            WHERE q.time_stamp >
                  l.latest_time - INTERVAL '32 hours'
              AND q.time_stamp <= l.latest_time
          ),

          rolling AS (
            SELECT
              sensor_sn,
              time_stamp,
              pm25,
              pm10,
              latest_time,

              AVG(o3) OVER (
                PARTITION BY sensor_sn
                ORDER BY time_stamp
                RANGE BETWEEN INTERVAL '8 hours' PRECEDING
                      AND CURRENT ROW
              ) AS o3_8h_average,

              AVG(co) OVER (
                PARTITION BY sensor_sn
                ORDER BY time_stamp
                RANGE BETWEEN INTERVAL '8 hours' PRECEDING
                      AND CURRENT ROW
              ) AS co_8h_average,

              AVG(no2) OVER (
                PARTITION BY sensor_sn
                ORDER BY time_stamp
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING
                      AND CURRENT ROW
              ) AS no2_1h_average

            FROM bounded
          ),

          recent AS (
            SELECT *
            FROM rolling
            WHERE time_stamp >
                  latest_time - INTERVAL '24 hours'
              AND time_stamp <= latest_time
          )

          SELECT
            sensor_sn AS sn,
            AVG(pm25)
              FILTER (
                WHERE pm25 IS NOT NULL
              ) AS pm25_24h,
            AVG(pm10)
              FILTER (
                WHERE pm10 IS NOT NULL
              ) AS pm10_24h,
            MAX(o3_8h_average)
              AS o3_highest_8h,
            MAX(co_8h_average)
              AS co_highest_8h,
            MAX(no2_1h_average)
              AS no2_highest_1h,
            MAX(latest_time)
              AS latest_record
          FROM recent
          GROUP BY sensor_sn
          `,
          [
            MAP_SPODS.map(
              sensor =>
                sensor.sn
            )
          ]
        );

      const c12Promise =
        pool.query(
          `
          WITH cleaned AS (
            SELECT
              device_id,
              time_stamp,
              NULLIF(
                bc_880nm,
                ''
              )::double precision AS bc,
              latitude,
              longitude
            FROM c12_master
            WHERE device_id =
                  ANY($1::text[])
              AND bc_880nm
                  IS NOT NULL
              AND bc_880nm <> ''
          ),

          latest AS (
            SELECT
              device_id,
              MAX(time_stamp)
                AS latest_time
            FROM cleaned
            GROUP BY device_id
          ),

          averages AS (
            SELECT
              c.device_id,
              AVG(c.bc)
                AS bc_available_average,
              MIN(c.time_stamp)
                AS earliest_record,
              MAX(c.time_stamp)
                AS latest_record,
              COUNT(*)
                AS stored_rows
            FROM cleaned c
            JOIN latest l
              ON l.device_id =
                 c.device_id
            WHERE c.time_stamp >
                  l.latest_time -
                  INTERVAL '1 year'
              AND c.time_stamp <=
                  l.latest_time
            GROUP BY c.device_id
          ),

          latest_coords AS (
            SELECT DISTINCT ON (
              device_id
            )
              device_id,
              latitude,
              longitude,
              time_stamp
            FROM cleaned
            WHERE latitude
                  IS NOT NULL
              AND longitude
                  IS NOT NULL
            ORDER BY
              device_id,
              time_stamp DESC
          )

          SELECT
            a.device_id,
            a.bc_available_average,
            a.earliest_record,
            a.latest_record,
            a.stored_rows,
            lc.latitude,
            lc.longitude
          FROM averages a
          LEFT JOIN latest_coords lc
            ON lc.device_id =
               a.device_id
          `,
          [MAP_C12_IDS]
        );

      let purpleOutput;
      let quantResult;
      let c12Result;

      try {
        [
          purpleOutput,
          quantResult,
          c12Result
        ] =
          await Promise.all([
            purplePromise,
            quantPromise,
            c12Promise
          ]);
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "map_snapshot_load_failed",
            detail:
              String(error)
          });
      }

      if (!purpleOutput.ok) {
        return res
          .status(
            purpleOutput.status
          )
          .json({
            error:
              "map_snapshot_purpleair_failed",
            details:
              purpleOutput.data
          });
      }

      const purpleRows =
        Array.isArray(
          purpleOutput.data?.data
        )
          ? purpleOutput.data.data
          : [];

      const quantRows =
        new Map(
          (
            quantResult.rows ||
            []
          ).map(row => [
            row.sn,
            row
          ])
        );

      const c12Rows =
        new Map(
          (
            c12Result.rows ||
            []
          ).map(row => [
            row.device_id,
            row
          ])
        );

      // Fill missing QuantAQ coordinates from the
      // QuantAQ devices endpoint.
      for (
        const sensor of
        MAP_SPODS
      ) {
        if (
          !mapHasCoords(
            sensor.lat,
            sensor.lon
          )
        ) {
          const dynamic =
            dynamicQuantCoords
              .get(
                sensor.sn
              );

          if (dynamic) {
            sensor.lat =
              dynamic.lat;

            sensor.lon =
              dynamic.lon;
          }
        }
      }

      // Fill missing C-12 coordinates from
      // GroveStreams live station metadata.
      // The historical BC backfill can exist in
      // c12_master without latitude/longitude.
      for (
        const compId of
        MAP_C12_IDS
      ) {
        const row =
          c12Rows.get(
            compId
          );

        if (!row) {
          continue;
        }

        if (
          !mapHasCoords(
            row.latitude,
            row.longitude
          )
        ) {
          const dynamic =
            dynamicC12Coords
              .get(
                compId
              );

          if (dynamic) {
            row.latitude =
              dynamic.lat;

            row.longitude =
              dynamic.lon;
          }
        }
      }

      const stations = [];

      for (
        const row of
        purpleRows
      ) {
        const id =
          String(
            row?.[0] ?? ""
          );

        if (
          !MAP_PURPLEAIR_SENSORS[
            id
          ]
        ) {
          continue;
        }

        const lat =
          mapSafeNum(
            row?.[1]
          );

        const lon =
          mapSafeNum(
            row?.[2]
          );

        const pm25 =
          mapActiveNum(
            row?.[3]
          );

        if (
          !mapHasCoords(
            lat,
            lon
          )
        ) {
          continue;
        }

        const pollutants = [];

        const nearestQ =
          mapNearestQuant(
            lat,
            lon,
            quantRows
          );

        const nearestQPM25 =
          nearestQ
            ? mapActiveNum(
                nearestQ
                  .data
                  ?.pm25_24h
              )
            : null;

        if (
          nearestQ &&
          nearestQPM25 !== null
        ) {
          mapAddAQI(
            pollutants,
            `PM2.5 (nearby ${nearestQ.sn})`,
            mapAQIFromBreakpoints(
              nearestQPM25,
              MAP_BP_PM25_24H
            )
          );
        } else {
          mapAddAQI(
            pollutants,
            "PM2.5 (PurpleAir fallback)",
            mapAQIFromBreakpoints(
              pm25,
              MAP_BP_PM25_24H
            )
          );
        }

        if (nearestQ) {
          mapAddQuantPollutants(
            pollutants,
            nearestQ.data,
            `nearby ${nearestQ.sn}`,
            false
          );
        }

        const nearestC =
          mapNearestC12(
            lat,
            lon,
            c12Rows
          );

        if (nearestC) {
          mapAddC12Pollutants(
            pollutants,
            nearestC
              .data
              .bc_available_average,
            `nearby ${nearestC.compId}`
          );
        }

        const result =
          mapCalcCEAQI(
            pollutants
          );

        stations.push({
          id,
          name:
            MAP_PURPLEAIR_SENSORS[
              id
            ],
          type: "PurpleAir",
          lat,
          lon,
          ceaqi:
            result.ceaqi,
          baseLabel:
            result.baseLabel,
          pm25
        });
      }

      for (
        const sensor of
        MAP_SPODS
      ) {
        const data =
          quantRows.get(
            sensor.sn
          );

        if (
          !data ||
          !mapHasCoords(
            sensor.lat,
            sensor.lon
          )
        ) {
          continue;
        }

        const pollutants = [];

        mapAddQuantPollutants(
          pollutants,
          data,
          sensor.sn,
          true
        );

        const nearestC =
          mapNearestC12(
            sensor.lat,
            sensor.lon,
            c12Rows
          );

        if (nearestC) {
          mapAddC12Pollutants(
            pollutants,
            nearestC
              .data
              .bc_available_average,
            `nearby ${nearestC.compId}`
          );
        }

        const result =
          mapCalcCEAQI(
            pollutants
          );

        stations.push({
          id: sensor.sn,
          name: sensor.name,
          type: "QuantAQ",
          lat: sensor.lat,
          lon: sensor.lon,
          ceaqi:
            result.ceaqi,
          baseLabel:
            result.baseLabel,
          latestTimeMs:
            data.latest_record
              ? new Date(
                  data.latest_record
                ).getTime()
              : null
        });
      }

      for (
        const compId of
        MAP_C12_IDS
      ) {
        const data =
          c12Rows.get(
            compId
          );

        const lat =
          mapSafeNum(
            data?.latitude
          );

        const lon =
          mapSafeNum(
            data?.longitude
          );

        const bcAverage =
          mapActiveNum(
            data
              ?.bc_available_average
          );

        if (
          !data ||
          !mapHasCoords(
            lat,
            lon
          ) ||
          bcAverage === null
        ) {
          continue;
        }

        const pollutants = [];

        mapAddC12Pollutants(
          pollutants,
          bcAverage,
          compId
        );

        const nearestQ =
          mapNearestQuant(
            lat,
            lon,
            quantRows
          );

        if (nearestQ) {
          mapAddQuantPollutants(
            pollutants,
            nearestQ.data,
            `nearby ${nearestQ.sn}`,
            true
          );
        }

        const result =
          mapCalcCEAQI(
            pollutants
          );

        stations.push({
          id: `C12-${compId}`,
          name:
            `C-12 ${compId}`,
          type: "C-12",
          lat,
          lon,
          ceaqi:
            result.ceaqi,
          baseLabel:
            result.baseLabel,
          latestTimeMs:
            data.latest_record
              ? new Date(
                  data.latest_record
                ).getTime()
              : null
        });
      }

      const payload = {
        generatedAt:
          new Date()
            .toISOString(),

        cacheSeconds: 300,

        stations,

        support: {
          quant:
            Object.fromEntries(
              quantRows
            ),

          c12:
            Object.fromEntries(
              c12Rows
            )
        }
      };

      mapSnapshotCache = {
        data: payload,
        expiresAt:
          Date.now() +
          MAP_SNAPSHOT_CACHE_MS
      };

      globalThis
        .cheverlyMapSnapshotCache =
        mapSnapshotCache;

      res.setHeader(
        "Cache-Control",
        "s-maxage=300, stale-while-revalidate=900"
      );

      return res
        .status(200)
        .json(payload);
    }

    // --------------------------------------------------
    // AirNow: PM2.5 history for the three reference sites
    // --------------------------------------------------

    if (
      action ===
      "airnow_pm25_history"
    ) {
      if (!AIRNOW_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_AIRNOW_API_KEY"
          });
      }

      const requestedHours =
        Number(
          req.query.hours ??
          24
        );

      const hours =
        Number.isFinite(
          requestedHours
        )
          ? Math.min(
              Math.max(
                Math.round(
                  requestedHours
                ),
                1
              ),
              48
            )
          : 24;

      const cacheKey =
        `pm25:${hours}`;

      const cached =
        airNowCache.get(
          cacheKey
        );

      if (
        cached &&
        Date.now() <
          cached.expiresAt
      ) {
        res.setHeader(
          "Cache-Control",
          "s-maxage=300, stale-while-revalidate=600"
        );

        return res
          .status(200)
          .json(
            cached.data
          );
      }

      const end =
        new Date();

      end.setUTCMinutes(
        0,
        0,
        0
      );

      const start =
        new Date(
          end.getTime() -
          hours *
            60 *
            60 *
            1000
        );

      const params =
        new URLSearchParams({
          startDate:
            formatAirNowHour(
              start
            ),

          endDate:
            formatAirNowHour(
              end
            ),

          parameters: "PM25",

          BBOX:
            AIRNOW_BBOX,

          dataType: "B",

          format:
            "application/json",

          verbose: "0",

          monitorType: "0",

          includerawconcentrations:
            "0",

          API_KEY:
            AIRNOW_KEY
        });

      const airNowUrl =
        "https://www.airnowapi.org/aq/data/?" +
        params.toString();

      const controller =
        new AbortController();

      const timeout =
        setTimeout(
          () =>
            controller.abort(),
          20000
        );

      try {
        const output =
          await fetchJson(
            airNowUrl,
            {
              signal:
                controller.signal,

              headers: {
                Accept:
                  "application/json"
              }
            }
          );

        if (!output.ok) {
          return res
            .status(502)
            .json({
              error:
                "airnow_request_failed",

              status:
                output.status,

              details:
                output.data
            });
        }

        if (
          !Array.isArray(
            output.data
          )
        ) {
          return res
            .status(502)
            .json({
              error:
                "airnow_unexpected_response",

              details:
                output.data
            });
        }

        const parsedRows =
          parseAirNowReferenceRows(
            output.data
          );

        const sites =
          AIRNOW_REFERENCE_SITES.map(
            site => ({
              key: site.key,

              name:
                site.name,

              agency:
                site.agency,

              latitude:
                site.latitude,

              longitude:
                site.longitude,

              points:
                parsedRows
                  .filter(
                    point =>
                      point.siteKey ===
                      site.key
                  )
                  .map(
                    point => ({
                      timestamp:
                        point.timestamp,

                      value:
                        point.value
                    })
                  )
            })
          );

        const payload = {
          parameter: "PM2.5",

          units: "µg/m³",

          hours,

          preliminary: true,

          generatedAt:
            new Date()
              .toISOString(),

          requestedPeriod: {
            start:
              start.toISOString(),

            end:
              end.toISOString()
          },

          sites
        };

        airNowCache.set(
          cacheKey,
          {
            expiresAt:
              Date.now() +
              AIRNOW_CACHE_MS,

            data: payload
          }
        );

        res.setHeader(
          "Cache-Control",
          "s-maxage=300, stale-while-revalidate=600"
        );

        return res
          .status(200)
          .json(payload);
      } catch (error) {
        const isTimeout =
          error?.name ===
          "AbortError";

        return res
          .status(502)
          .json({
            error:
              isTimeout
                ? "airnow_request_timeout"
                : "airnow_request_error",

            detail:
              String(
                error?.message ||
                error
              )
          });
      } finally {
        clearTimeout(
          timeout
        );
      }
    }

    // --------------------------------------------------
    // PurpleAir helper
    // --------------------------------------------------

    const purpleairFetch =
      async baseUrl => {
        let output =
          await fetchJson(
            baseUrl,
            {
              headers: {
                "X-API-Key":
                  PURPLEAIR_KEY
              }
            }
          );

        if (output.ok) {
          return output;
        }

        const joinCharacter =
          baseUrl.includes("?")
            ? "&"
            : "?";

        const fallbackUrl =
          `${baseUrl}${joinCharacter}` +
          `api_key=${encodeURIComponent(
            PURPLEAIR_KEY
          )}`;

        return await fetchJson(
          fallbackUrl
        );
      };

    // --------------------------------------------------
    // PurpleAir: current map readings
    // --------------------------------------------------

    if (
      action ===
      "purpleair_box"
    ) {
      if (!PURPLEAIR_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_PURPLEAIR_API_KEY"
          });
      }

      const url =
        "https://api.purpleair.com/v1/sensors" +
        "?nwlng=-77.15&nwlat=39.05&selng=-76.75&selat=38.75" +
        "&fields=sensor_index,latitude,longitude,pm2.5_atm";

      const output =
        await purpleairFetch(
          url
        );

      if (!output.ok) {
        return res
          .status(
            output.status
          )
          .json({
            error:
              "purpleair_box_failed",

            status:
              output.status,

            details:
              output.data
          });
      }

      return res
        .status(200)
        .json(
          output.data
        );
    }

    // --------------------------------------------------
    // PurpleAir: station history
    // --------------------------------------------------

    if (
      action ===
      "purpleair_history"
    ) {
      if (!PURPLEAIR_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_PURPLEAIR_API_KEY"
          });
      }

      const id =
        req.query.id;

      const start =
        req.query.start;

      if (
        !id ||
        !start
      ) {
        return res
          .status(400)
          .json({
            error:
              "missing_id_or_start"
          });
      }

      const url =
        `https://api.purpleair.com/v1/sensors/` +
        `${encodeURIComponent(
          id
        )}/history` +
        `?fields=pm2.5_atm` +
        `&average=60` +
        `&start_timestamp=${encodeURIComponent(
          start
        )}`;

      const output =
        await purpleairFetch(
          url
        );

      if (!output.ok) {
        return res
          .status(
            output.status
          )
          .json({
            error:
              "purpleair_history_failed",

            status:
              output.status,

            details:
              output.data
          });
      }

      return res
        .status(200)
        .json(
          output.data
        );
    }

    // --------------------------------------------------
    // QuantAQ helper functions
    // --------------------------------------------------

    const quantAuthHeaders =
      () => {
        const auth =
          Buffer.from(
            `${QUANTAQ_KEY}:`
          ).toString(
            "base64"
          );

        return {
          Accept:
            "application/json",

          Authorization:
            `Basic ${auth}`
        };
      };

    const normalizeSerialNumber =
      value =>
        String(
          value || ""
        )
          .toUpperCase()
          .replace(
            /[^A-Z0-9]/g,
            ""
          );

    const getQuantDevices =
      async () => {
        const url =
          "https://api.quant-aq.com/v1/devices" +
          "?per_page=200&page=1";

        return await fetchJson(
          url,
          {
            headers:
              quantAuthHeaders()
          }
        );
      };

    const quantDataByDate =
      async (
        serialNumber,
        date
      ) => {
        const url =
          `https://api.quant-aq.com/v1/devices/` +
          `${encodeURIComponent(
            serialNumber
          )}/data-by-date/` +
          `${encodeURIComponent(
            date
          )}/`;

        return await fetchJson(
          url,
          {
            headers:
              quantAuthHeaders()
          }
        );
      };

    if (
      action ===
      "quantaq_devices"
    ) {
      if (!QUANTAQ_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_QUANTAQ_API_KEY"
          });
      }

      const output =
        await getQuantDevices();

      return res
        .status(
          output.status
        )
        .json(
          output.data
        );
    }

    if (
      action ===
      "quantaq_by_date"
    ) {
      if (!QUANTAQ_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_QUANTAQ_API_KEY"
          });
      }

      const serialNumberInput =
        req.query.sn;

      const date =
        req.query.date;

      if (
        !serialNumberInput ||
        !date
      ) {
        return res
          .status(400)
          .json({
            error:
              "missing_sn_or_date"
          });
      }

      let output =
        await quantDataByDate(
          serialNumberInput,
          date
        );

      if (output.ok) {
        return res
          .status(200)
          .json(
            output.data
          );
      }

      if (
        output.status === 404
      ) {
        const devicesOutput =
          await getQuantDevices();

        if (
          !devicesOutput.ok
        ) {
          return res
            .status(
              devicesOutput.status
            )
            .json({
              error:
                "quantaq_devices_list_failed",

              details:
                devicesOutput.data,

              original_try: {
                sn:
                  serialNumberInput,

                date,

                status:
                  output.status,

                details:
                  output.data
              }
            });
        }

        const devices =
          Array.isArray(
            devicesOutput
              .data?.data
          )
            ? devicesOutput
                .data.data
            : [];

        const target =
          normalizeSerialNumber(
            serialNumberInput
          );

        let match =
          devices.find(
            device =>
              normalizeSerialNumber(
                device?.sn
              ) === target
          );

        if (!match) {
          const looselyNormalize =
            value =>
              normalizeSerialNumber(
                value
              )
                .replace(
                  /0+/g,
                  "0"
                )
                .replace(
                  /0([1-9])/g,
                  "$1"
                );

          const looseTarget =
            looselyNormalize(
              serialNumberInput
            );

          match =
            devices.find(
              device =>
                looselyNormalize(
                  device?.sn
                ) ===
                looseTarget
            );
        }

        if (match?.sn) {
          const retry =
            await quantDataByDate(
              match.sn,
              date
            );

          if (retry.ok) {
            return res
              .status(200)
              .json({
                resolved_sn:
                  match.sn,

                data:
                  retry.data
              });
          }

          return res
            .status(
              retry.status
            )
            .json({
              error:
                "quantaq_retry_failed",

              resolved_sn:
                match.sn,

              details:
                retry.data
            });
        }

        const sampleSerialNumbers =
          devices
            .slice(0, 15)
            .map(
              device =>
                device?.sn
            )
            .filter(Boolean);

        return res
          .status(404)
          .json({
            error:
              "quantaq_sn_not_found_for_key",

            provided_sn:
              serialNumberInput,

            sample_sns:
              sampleSerialNumbers,

            total_visible_devices:
              devices.length
          });
      }

      return res
        .status(
          output.status
        )
        .json({
          error:
            "quantaq_failed",

          status:
            output.status,

          details:
            output.data
        });
    }

    // --------------------------------------------------
    // QuantAQ database: latest stored reading
    // --------------------------------------------------

    if (
      action ===
      "quantaq_latest"
    ) {
      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const serialNumber =
        req.query.sn;

      if (!serialNumber) {
        return res
          .status(400)
          .json({
            error:
              "missing_sn"
          });
      }

      try {
        const result =
          await pool.query(
            `
            SELECT
              time_stamp AS timestamp,
              sensor_sn AS sn,
              pm25,
              pm10,
              o3,
              co,
              no2
            FROM quantaq_master
            WHERE sensor_sn = $1
              AND (
                pm25 IS NOT NULL
                OR pm10 IS NOT NULL
                OR o3 IS NOT NULL
                OR co IS NOT NULL
                OR no2 IS NOT NULL
              )
            ORDER BY time_stamp DESC
            LIMIT 1
            `,
            [serialNumber]
          );

        return res
          .status(200)
          .json({
            sn:
              serialNumber,

            data:
              result.rows
          });
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "quantaq_latest_failed",

            detail:
              String(error)
          });
      }
    }

    // --------------------------------------------------
    // QuantAQ database: stored history
    // --------------------------------------------------

    if (
      action ===
      "quantaq_history"
    ) {
      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const serialNumber =
        req.query.sn;

      const requestedHours =
        Number(
          req.query.hours ||
          24
        );

      if (!serialNumber) {
        return res
          .status(400)
          .json({
            error:
              "missing_sn"
          });
      }

      const safeHours =
        Number.isFinite(
          requestedHours
        ) &&
        requestedHours > 0
          ? Math.min(
              requestedHours,
              8760
            )
          : 24;

      try {
        const result =
          await pool.query(
            `
            SELECT
              time_stamp AS timestamp,
              sensor_sn AS sn,
              pm25,
              pm10,
              o3,
              co,
              no2
            FROM quantaq_master
            WHERE sensor_sn = $1
              AND time_stamp >=
                NOW() -
                ($2::text || ' hours')::interval
              AND (
                pm25 IS NOT NULL
                OR pm10 IS NOT NULL
                OR o3 IS NOT NULL
                OR co IS NOT NULL
                OR no2 IS NOT NULL
              )
            ORDER BY time_stamp ASC
            `,
            [
              serialNumber,
              safeHours
            ]
          );

        return res
          .status(200)
          .json({
            sn:
              serialNumber,

            hours:
              safeHours,

            data:
              result.rows
          });
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "quantaq_history_failed",

            detail:
              String(error)
          });
      }
    }

    // --------------------------------------------------
    // QuantAQ database: rolling CE-AQI inputs
    // --------------------------------------------------

    if (
      action ===
      "quantaq_averages"
    ) {
      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const serialNumber =
        req.query.sn;

      if (!serialNumber) {
        return res
          .status(400)
          .json({
            error:
              "missing_sn"
          });
      }

      try {
        const result =
          await pool.query(
            `
            WITH sensor_data AS (
              SELECT
                time_stamp,
                pm25,
                pm10,
                o3,
                co,
                no2
              FROM quantaq_master
              WHERE sensor_sn = $1
            ),

            latest AS (
              SELECT
                MAX(time_stamp)
                  AS latest_time
              FROM sensor_data
            ),

            recent_24h AS (
              SELECT
                s.*
              FROM sensor_data s
              CROSS JOIN latest l
              WHERE
                s.time_stamp >
                l.latest_time -
                INTERVAL '24 hours'
                AND
                s.time_stamp <=
                l.latest_time
            ),

            rolling_values AS (
              SELECT
                current_row.time_stamp,

                (
                  SELECT
                    AVG(
                      window_row.o3
                    )
                  FROM sensor_data
                    window_row
                  WHERE
                    window_row.time_stamp >
                    current_row.time_stamp -
                    INTERVAL '8 hours'
                    AND
                    window_row.time_stamp <=
                    current_row.time_stamp
                    AND
                    window_row.o3
                    IS NOT NULL
                )
                  AS o3_8h_average,

                (
                  SELECT
                    AVG(
                      window_row.co
                    )
                  FROM sensor_data
                    window_row
                  WHERE
                    window_row.time_stamp >
                    current_row.time_stamp -
                    INTERVAL '1 hour'
                    AND
                    window_row.time_stamp <=
                    current_row.time_stamp
                    AND
                    window_row.co
                    IS NOT NULL
                )
                  AS co_1h_average,

                (
                  SELECT
                    AVG(
                      window_row.co
                    )
                  FROM sensor_data
                    window_row
                  WHERE
                    window_row.time_stamp >
                    current_row.time_stamp -
                    INTERVAL '8 hours'
                    AND
                    window_row.time_stamp <=
                    current_row.time_stamp
                    AND
                    window_row.co
                    IS NOT NULL
                )
                  AS co_8h_average,

                (
                  SELECT
                    AVG(
                      window_row.no2
                    )
                  FROM sensor_data
                    window_row
                  WHERE
                    window_row.time_stamp >
                    current_row.time_stamp -
                    INTERVAL '1 hour'
                    AND
                    window_row.time_stamp <=
                    current_row.time_stamp
                    AND
                    window_row.no2
                    IS NOT NULL
                )
                  AS no2_1h_average

              FROM recent_24h
                current_row
            )

            SELECT
              $1::text AS sn,

              (
                SELECT
                  AVG(pm25)
                FROM recent_24h
                WHERE
                  pm25 IS NOT NULL
              )
                AS pm25_24h,

              (
                SELECT
                  AVG(pm10)
                FROM recent_24h
                WHERE
                  pm10 IS NOT NULL
              )
                AS pm10_24h,

              (
                SELECT
                  MAX(
                    o3_8h_average
                  )
                FROM rolling_values
              )
                AS o3_highest_8h,

              (
                SELECT
                  MAX(
                    co_1h_average
                  )
                FROM rolling_values
              )
                AS co_highest_1h,

              (
                SELECT
                  MAX(
                    co_8h_average
                  )
                FROM rolling_values
              )
                AS co_highest_8h,

              (
                SELECT
                  MAX(
                    no2_1h_average
                  )
                FROM rolling_values
              )
                AS no2_highest_1h,

              (
                SELECT
                  AVG(s.no2)
                FROM sensor_data s
                CROSS JOIN latest l
                WHERE
                  s.time_stamp >
                  l.latest_time -
                  INTERVAL '1 year'
                  AND
                  s.time_stamp <=
                  l.latest_time
                  AND
                  s.no2 IS NOT NULL
              )
                AS no2_annual_available_average,

              (
                SELECT
                  MIN(time_stamp)
                FROM sensor_data
              )
                AS earliest_record,

              (
                SELECT
                  latest_time
                FROM latest
              )
                AS latest_record,

              (
                SELECT
                  COUNT(*)
                FROM sensor_data
              )
                AS stored_rows
            `,
            [serialNumber]
          );

        return res
          .status(200)
          .json({
            sn:
              serialNumber,

            data:
              result.rows[0] ||
              null
          });
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "quantaq_averages_failed",

            detail:
              String(error)
          });
      }
    }

    // --------------------------------------------------
    // GroveStreams / C-12: historical stream data
    // --------------------------------------------------

    if (
      action ===
      "grove_history"
    ) {
      if (!GROVE_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_GROVESTREAMS_API_KEY"
          });
      }

      const componentId =
        req.query.compId;

      const streamId =
        req.query.streamId ||
        "880nm";

      const start =
        req.query.start;

      const end =
        req.query.end;

      if (
        !componentId ||
        !start ||
        !end
      ) {
        return res
          .status(400)
          .json({
            error:
              "missing_compId_start_or_end"
          });
      }

      const url =
        `https://grovestreams.com/api/comp/${encodeURIComponent(
          componentId
        )}` +
        `/stream/${encodeURIComponent(
          streamId
        )}/feed` +
        `?sd=${encodeURIComponent(
          start
        )}` +
        `&ed=${encodeURIComponent(
          end
        )}` +
        `&api_key=${encodeURIComponent(
          GROVE_KEY
        )}`;

      const output =
        await fetchJson(url);

      if (!output.ok) {
        return res
          .status(
            output.status
          )
          .json({
            error:
              "grove_history_failed",

            status:
              output.status,

            details:
              output.data
          });
      }

      return res
        .status(200)
        .json({
          device_id:
            componentId,

          stream_id:
            streamId,

          start,

          end,

          data:
            Array.isArray(
              output.data
            )
              ? output.data
              : []
        });
    }

    // --------------------------------------------------
    // GroveStreams / C-12: latest readings
    // --------------------------------------------------

    if (
      action ===
      "grove_last"
    ) {
      if (!GROVE_KEY) {
        return res
          .status(500)
          .json({
            error:
              "missing_GROVESTREAMS_API_KEY"
          });
      }

      const componentId =
        req.query.compId;

      if (!componentId) {
        return res
          .status(400)
          .json({
            error:
              "missing_compId"
          });
      }

      const url =
        `https://grovestreams.com/api/comp/` +
        `${encodeURIComponent(
          componentId
        )}` +
        `/last_value` +
        `?retStreamId` +
        `&api_key=${encodeURIComponent(
          GROVE_KEY
        )}`;

      const output =
        await fetchJson(url);

      if (!output.ok) {
        return res
          .status(
            output.status
          )
          .json({
            error:
              "grove_last_failed",

            status:
              output.status,

            details:
              output.data
          });
      }

      return res
        .status(200)
        .json(
          output.data
        );
    }

    // --------------------------------------------------
    // C-12 database: Black Carbon history
    // --------------------------------------------------

    if (
      action ===
      "c12_history"
    ) {
      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const componentId =
        req.query.compId;

      const requestedHours =
        Number(
          req.query.hours ||
          18
        );

      if (!componentId) {
        return res
          .status(400)
          .json({
            error:
              "missing_compId"
          });
      }

      const safeHours =
        Number.isFinite(
          requestedHours
        ) &&
        requestedHours > 0
          ? Math.min(
              requestedHours,
              8760
            )
          : 18;

      try {
        const result =
          await pool.query(
            `
            SELECT
              time_stamp AS time,
              bc_880nm AS bc,
              latitude,
              longitude,
              device_id
            FROM c12_master
            WHERE device_id = $1
              AND time_stamp >=
                NOW() -
                ($2::text || ' hours')::interval
              AND bc_880nm IS NOT NULL
            ORDER BY time_stamp ASC
            `,
            [
              componentId,
              safeHours
            ]
          );

        return res
          .status(200)
          .json({
            device_id:
              componentId,

            hours:
              safeHours,

            data:
              result.rows
          });
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "c12_history_failed",

            detail:
              String(error)
          });
      }
    }

    // --------------------------------------------------
    // C-12 database: available long-term average
    // --------------------------------------------------

    if (
      action ===
      "c12_average"
    ) {
      if (
        !process.env
          .DATABASE_URL
      ) {
        return res
          .status(500)
          .json({
            error:
              "missing_DATABASE_URL"
          });
      }

      const componentId =
        req.query.compId;

      if (!componentId) {
        return res
          .status(400)
          .json({
            error:
              "missing_compId"
          });
      }

      try {
        const result =
          await pool.query(
            `
            WITH device_data AS (
              SELECT
                time_stamp,
                NULLIF(
                  bc_880nm,
                  ''
                )::double precision
                  AS bc,
                device_id
              FROM c12_master
              WHERE
                device_id = $1
                AND
                bc_880nm
                IS NOT NULL
                AND
                bc_880nm <> ''
            ),

            latest AS (
              SELECT
                MAX(time_stamp)
                  AS latest_time
              FROM device_data
            )

            SELECT
              $1::text
                AS device_id,

              AVG(d.bc)
                AS bc_available_average,

              AVG(d.bc) * 1.25
                AS dpm_available_average,

              MIN(d.time_stamp)
                AS earliest_record,

              MAX(d.time_stamp)
                AS latest_record,

              COUNT(*)
                AS stored_rows

            FROM device_data d
            CROSS JOIN latest l

            WHERE
              d.time_stamp >
              l.latest_time -
              INTERVAL '1 year'

              AND

              d.time_stamp <=
              l.latest_time
            `,
            [componentId]
          );

        return res
          .status(200)
          .json({
            device_id:
              componentId,

            data:
              result.rows[0] ||
              null
          });
      } catch (error) {
        return res
          .status(500)
          .json({
            error:
              "c12_average_failed",

            detail:
              String(error)
          });
      }
    }

    return res
      .status(404)
      .json({
        error:
          "unknown_action",

        action
      });
  } catch (error) {
    return res
      .status(500)
      .json({
        error:
          "server_error",

        detail:
          String(error)
      });
  }
}
