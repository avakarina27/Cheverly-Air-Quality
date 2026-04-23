import os
import requests
import psycopg2
from datetime import datetime, timedelta
from flask import Flask, jsonify, request

app = Flask(__name__)

# Config - Set these in Vercel Environment Variables
DATABASE_URL = os.environ.get('DATABASE_URL')
QUANTAQ_KEY = "QC2TTD7QPKL1GXSTHDXAXOC3"

def get_db():
    return psycopg2.connect(DATABASE_URL)

@app.route('/api/aq', methods=['GET'])
def get_sensor_data():
    sensor_id = request.args.get('id') # e.g., "MOD-00745" or "156595"
    sensor_type = request.args.get('type') # "quantaq", "purpleair", or "c12"
    
    response_data = {"id": sensor_id, "live": None, "history": []}

    # --- PART 1: LIVE DATA PULLS ---
    if sensor_type == "quantaq":
        res = requests.get(f"https://api.quantaq.com/v1/devices/{sensor_id}/data/", 
                           auth=(QUANTAQ_KEY, ""), params={"limit": 1})
        if res.status_code == 200:
            latest = res.json().get('data', [{}])[0]
            response_data["live"] = latest.get('pm25') or latest.get('pm2_5')

    elif sensor_type == "purpleair":
        res = requests.get(f"https://api.purpleair.com/v1/sensors/{sensor_id}", 
                           headers={"X-API-Key": "YOUR_PURPLE_AIR_KEY"}) # Use your PA Key
        if res.status_code == 200:
            response_data["live"] = res.json().get('sensor', {}).get('pm2.5')

    elif sensor_type == "c12":
        # Pulling from your Grove/C12 proxy or direct endpoint
        res = requests.get(f"https://api.grove.id/v1/devices/{sensor_id}/live")
        if res.status_code == 200:
            response_data["live"] = res.json().get('black_carbon') # Or mapped AQI

    # --- PART 2: HISTORICAL DATA (From Aiven) ---
    # Only for PurpleAir and QuantAQ as requested
    if sensor_type in ["purpleair", "quantaq"]:
        try:
            conn = get_db()
            cur = conn.cursor()
            # This pulls the last 24 hours of logs stored in your Aiven DB
            cur.execute("""
                SELECT timestamp, pm25 FROM sensor_logs 
                WHERE sensor_id = %s AND timestamp > NOW() - INTERVAL '24 hours'
                ORDER BY timestamp ASC
            """, (sensor_id,))
            rows = cur.fetchall()
            response_data["history"] = [{"t": r[0].isoformat(), "v": r[1]} for r in rows]
            cur.close()
            conn.close()
        except Exception as e:
            response_data["db_error"] = str(e)

    return jsonify(response_data)
