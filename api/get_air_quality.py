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

            # SECTION 1: PURPLEAIR & QUANTAQ
            try:
                query_pm = """
                    SELECT DISTINCT ON (station_id) 
                        station_id, pm2_5_atm, 
                        (SELECT MAX(pm2_5_atm) FROM purple_air_master p2 
                         WHERE p2.station_id = p1.station_id 
                         AND p2.time_stamp > NOW() - INTERVAL '24 hours') as max_24
                    FROM purple_air_master p1
                    ORDER BY station_id, time_stamp DESC;
                """
                cur.execute(query_pm)
                for r in cur.fetchall():
                    live = float(r[1] or 0)
                    m24 = float(r[2] or live)
                    # CE-AQI Adder Logic
                    adder = 0.03 if m24 >= 120 else (0.02 if m24 >= 100 else (0.01 if m24 >= 80 else 0))
                    ce_aqi = round(m24 + (live * adder))
                    results.append({"id": r[0], "live": live, "ce_aqi": ce_aqi, "type": "pm25"})
            except Exception as e:
                print(f"PM2.5 Query Error: {e}")

            # SECTION 2: C-12 (Calculated safely)
            try:
                # Simplified query to avoid Window Function crashes if table is empty
                cur.execute("""
                    SELECT comp_id, bc_value 
                    FROM c12_master 
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                    ORDER BY timestamp DESC LIMIT 10;
                """)
                for c in cur.fetchall():
                    # For now, we'll use a simple live calculation to get you back online
                    val = float(c[1] or 0)
                    results.append({
                        "id": c[0], 
                        "live": val, 
                        "ce_aqi": round((val / 600) * 100), 
                        "type": "c12"
                    })
            except Exception as e:
                # This catches if 'c12_master' doesn't exist yet
                print(f"C12 Data unavailable: {e}")

            cur.close()
            conn.close()

        except Exception as e:
            # Final fallback so the dashboard gets valid JSON even if DB is down
            self.send_response(200) # Send 200 so JS doesn't crash
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps([{"id": "ERROR", "ce_aqi": 0, "live": 0, "type": "error"}]).encode())
            return

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
