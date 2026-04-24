from http.server import BaseHTTPRequestHandler
import os
import psycopg2
import json

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        db_url = os.environ.get('DATABASE_URL')
        results = []
        
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

            # SECTION 1: PURPLEAIR (Confirmed: purple_air_master)
            try:
                query_pa = """
                    SELECT DISTINCT ON (station_id) 
                        station_id, pm2_5_atm, 
                        (SELECT MAX(pm2_5_atm) FROM purple_air_master p2 
                         WHERE p2.station_id = p1.station_id 
                         AND p2.time_stamp > NOW() - INTERVAL '24 hours') as max_24
                    FROM purple_air_master p1
                    WHERE p1.time_stamp > NOW() - INTERVAL '24 hours'
                    ORDER BY station_id, time_stamp DESC;
                """
                cur.execute(query_pa)
                rows = cur.fetchall()
                print(f"PurpleAir rows found: {len(rows)}")
                for r in rows:
                    live = float(r[1] or 0)
                    m24 = float(r[2] or live)
                    # CE-AQI Adder Logic
                    adder = 0.03 if m24 >= 120 else (0.02 if m24 >= 100 else (0.01 if m24 >= 80 else 0))
                    ce_aqi = round(m24 + (live * adder))
                    results.append({"id": str(r[0]), "live": live, "ce_aqi": ce_aqi, "type": "pm25"})
            except Exception as e:
                print(f"PurpleAir Error: {e}")

            # SECTION 2: QUANTAQ
            try:
                query_qa = """
                    SELECT DISTINCT ON (sn) 
                        sn, pm25, 
                        (SELECT MAX(pm25) FROM quantaq_master q2 
                         WHERE q2.sn = q1.sn 
                         AND q2.timestamp > NOW() - INTERVAL '24 hours') as max_24
                    FROM quantaq_master q1
                    WHERE q1.timestamp > NOW() - INTERVAL '24 hours'
                    ORDER BY sn, timestamp DESC;
                """
                cur.execute(query_qa)
                rows = cur.fetchall()
                print(f"QuantAQ rows found: {len(rows)}")
                for r in rows:
                    live = float(r[1] or 0)
                    m24 = float(r[2] or live)
                    adder = 0.03 if m24 >= 120 else (0.02 if m24 >= 100 else (0.01 if m24 >= 80 else 0))
                    ce_aqi = round(m24 + (live * adder))
                    results.append({"id": str(r[0]), "live": live, "ce_aqi": ce_aqi, "type": "pm25"})
            except Exception as e:
                print(f"QuantAQ Error: {e}")

            # SECTION 3: C-12
            try:
                cur.execute("SELECT comp_id, bc_value FROM c12_master ORDER BY timestamp DESC LIMIT 10;")
                rows = cur.fetchall()
                for c in rows:
                    val = float(c[1] or 0)
                    results.append({
                        "id": str(c[0]), 
                        "live": val, 
                        "ce_aqi": round((val / 600) * 100), 
                        "type": "c12"
                    })
            except Exception as e:
                print(f"C12 Error: {e}")

            cur.close()
            conn.close()

        except Exception as e:
            # Fallback error response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps([{"id": "DB_CONNECT_ERROR", "msg": str(e)}]).encode())
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
