from http.server import BaseHTTPRequestHandler
import os
import psycopg2
import json
from urllib.parse import urlparse

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # 1. Connect to Aiven
        db_url = os.environ.get('DATABASE_URL')
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # 2. Get the most recent data for each station
        # This SQL query grabs the latest unique entry for every station
        query = """
        SELECT DISTINCT ON (station_id) 
            station_id, ward_number, pm2_5_atm, humidity, temperature, time_stamp
        FROM purple_air_master
        ORDER BY station_id, time_stamp DESC;
        """
        
        cur.execute(query)
        rows = cur.fetchall()

        # 3. Format as JSON
        data = []
        for row in rows:
            data.append({
                "station_id": row[0],
                "ward": row[1],
                "pm25": float(row[2]) if row[2] else 0,
                "humidity": float(row[3]) if row[3] else 0,
                "temp": float(row[4]) if row[4] else 0,
                "time": row[5].strftime("%Y-%m-%d %H:%M:%S")
            })

        cur.close()
        conn.close()

        # 4. Send Response
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        # Allow your website to talk to this API
        self.send_header('Access-Control-Allow-Origin', '*') 
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
        return
