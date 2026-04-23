from http.server import BaseHTTPRequestHandler
import os
import psycopg2
import json
from datetime import datetime

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        db_url = os.environ.get('DATABASE_URL')
        
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()

            # 1. Pull Latest & 24h Max from PurpleAir
            cur.execute("""
                SELECT DISTINCT ON (station_id) 
                    station_id, pm2_5_atm, time_stamp,
                    (SELECT MAX(pm2_5_atm) FROM purple_air_master p2 WHERE p2.station_id = p1.station_id AND p2.time_stamp > NOW() - INTERVAL '24 hours') as max_24h
                FROM purple_air_master p1
                ORDER BY station_id, time_stamp DESC;
            """)
            pa_rows = cur.fetchall()

            # 2. Pull Latest & 24h Max from QuantAQ
            cur.execute("""
                SELECT DISTINCT ON (sn) 
                    sn, pm25, timestamp,
                    (SELECT MAX(pm25) FROM quantaq_master q2 WHERE q2.sn = q1.sn AND q2.timestamp > NOW() - INTERVAL '24 hours') as max_24h
                FROM quantaq_master q1
                ORDER BY sn, timestamp DESC;
            """)
            qa_rows = cur.fetchall()

            combined_data = []

            # Format PurpleAir
            for row in pa_rows:
                live = float(row[1]) if row[1] else 0
                m24 = float(row[3]) if row[3] else live
                combined_data.append({
                    "id": row[0],
                    "type": "purpleair",
                    "live_pm25": live,
                    "ce_aqi": round((live + m24) / 2), # Simplified CE-AQI logic
                    "time": row[2].strftime("%Y-%m-%d %H:%M:%S")
                })

            # Format QuantAQ
            for row in qa_rows:
                live = float(row[1]) if row[1] else 0
                m24 = float(row[3]) if row[3] else live
                combined_data.append({
                    "id": row[0],
                    "type": "quantaq",
                    "live_pm25": live,
                    "ce_aqi": round((live + m24) / 2),
                    "time": row[2].strftime("%Y-%m-%d %H:%M:%S")
                })

            cur.close()
            conn.close()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*') 
            self.end_headers()
            self.wfile.write(json.dumps(combined_data).encode())

        except Exception as e:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(str(e).encode())
