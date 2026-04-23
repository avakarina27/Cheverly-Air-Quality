from http.server import BaseHTTPRequestHandler
import os
import psycopg2
import json
from datetime import datetime

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        db_url = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # 1. PURPLEAIR & QUANTAQ (24-Hour Max + Adders)
        # We pull the current value and the max from the last 24 hours
        query_pa_qa = """
            SELECT station_id, pm2_5_atm, 
            (SELECT MAX(pm2_5_atm) FROM purple_air_master p2 WHERE p2.station_id = p1.station_id AND p2.time_stamp > NOW() - INTERVAL '24 hours') as max_24
            FROM purple_air_master p1 WHERE p1.time_stamp > NOW() - INTERVAL '10 minutes'
            UNION ALL
            SELECT sn as station_id, pm25 as pm2_5_atm,
            (SELECT MAX(pm25) FROM quantaq_master q2 WHERE q2.sn = q1.sn AND q2.timestamp > NOW() - INTERVAL '24 hours') as max_24
            FROM quantaq_master q1 WHERE q1.timestamp > NOW() - INTERVAL '10 minutes';
        """
        cur.execute(query_pa_qa)
        rows = cur.fetchall()
        
        results = []
        for r in rows:
            live, m24 = float(r[1] or 0), float(r[2] or 0)
            # CE-AQI Adder Logic
            base = m24
            adder = 0
            if base >= 120: adder = 0.03
            elif base >= 100: adder = 0.02
            elif base >= 80: adder = 0.01
            
            ce_aqi = round(base + (live * adder))
            results.append({"id": r[0], "live": live, "ce_aqi": ce_aqi, "type": "pm25"})

        # 2. C-12 BLACK CARBON (8-Hour Rolling Average)
        # Note: This assumes you have a table named 'c12_master'
        cur.execute("""
            SELECT comp_id, 
            AVG(bc_value) OVER (PARTITION BY comp_id ORDER BY timestamp ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as avg_8h,
            bc_value
            FROM c12_master
            WHERE timestamp > NOW() - INTERVAL '8 hours'
        """)
        c12_rows = cur.fetchall()
        for c in c12_rows:
            # Simple conversion: (8hr Avg BC / 600) * 100 to get a pseudo-AQI
            bc_live = float(c[2] or 0)
            bc_avg = float(c[1] or 0)
            c12_aqi = round((bc_avg / 600) * 100)
            results.append({"id": c[0], "live": bc_live, "ce_aqi": c12_aqi, "type": "c12"})

        cur.close()
        conn.close()

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(results).encode())
