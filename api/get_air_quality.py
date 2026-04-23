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

            # 1. PURPLEAIR & QUANTAQ (The reliable tables)
            query_pm = """
                SELECT DISTINCT ON (station_id) 
                    station_id, pm2_5_atm, 
                    (SELECT MAX(pm2_5_atm) FROM purple_air_master p2 WHERE p2.station_id = p1.station_id AND p2.time_stamp > NOW() - INTERVAL '24 hours') as max_24
                FROM purple_air_master p1
                ORDER BY station_id, time_stamp DESC;
            """
            cur.execute(query_pm)
            for r in cur.fetchall():
                live = float(r[1] or 0)
                m24 = float(r[2] or live)
                # Adder Logic
                adder = 0.03 if m24 >= 120 else (0.02 if m24 >= 100 else (0.01 if m24 >= 80 else 0))
                ce_aqi = round(m24 + (live * adder))
                results.append({"id": r[0], "live": live, "ce_aqi": ce_aqi, "type": "pm25"})

            # 2. C-12 (Wrap in its own try block so it doesn't kill the site)
            try:
                cur.execute("""
                    SELECT comp_id, bc_value,
                    AVG(bc_value) OVER (PARTITION BY comp_id ORDER BY timestamp ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)
                    FROM c12_master WHERE timestamp > NOW() - INTERVAL '12 hours'
                """)
                for c in cur.fetchall():
                    results.append({
                        "id": c[0], 
                        "live": float(c[1] or 0), 
                        "ce_aqi": round((float(c[2] or 0) / 600) * 100), 
                        "type": "c12"
                    })
            except Exception as e:
                print(f"C12 Table Error (likely missing): {e}")

            cur.close()
            conn.close()

        except Exception as e:
            # If everything fails, send the error as JSON so the dashboard doesn't break
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        # Success Response
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
