import os
import psycopg2
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

# Aiven Service URI from your Vercel Env Vars
DATABASE_URL = os.environ.get('DATABASE_URL')
QUANTAQ_KEY = "QC2TTD7QPKL1GXSTHDXAXOC3"

def get_db():
    return psycopg2.connect(DATABASE_URL)

@app.route('/api/data', methods=['GET'])
def get_sensor_data():
    sn = request.args.get('sn')
    stype = request.args.get('type') # 'quantaq', 'purpleair', or 'c12'
    
    # 1. LIVE DATA (For the immediate dashboard display)
    live_pm = 0
    if stype == "quantaq":
        res = requests.get(f"https://api.quantaq.com/v1/devices/{sn}/data/", 
                           auth=(QUANTAQ_KEY, ""), params={"limit": 1})
        live_pm = res.json()['data'][0]['pm25'] if res.status_code == 200 else 0
    elif stype == "purpleair":
        # Direct API call to PurpleAir for current reading
        res = requests.get(f"https://api.purpleair.com/v1/sensors/{sn}", 
                           headers={"X-API-Key": os.environ.get('PA_READ_KEY')})
        live_pm = res.json()['sensor']['pm2.5'] if res.status_code == 200 else 0
    elif stype == "c12":
        # Assuming C12 is live-only pass-through
        live_pm = pull_c12_live(sn) 

    # 2. HISTORICAL DATA (For CE-AQI and Trend Graph)
    # Only for PurpleAir and QuantAQ
    max_24h = live_pm
    history = []
    
    if stype in ["purpleair", "quantaq"]:
        table = "purple_air_master" if stype == "purpleair" else "quantaq_master"
        try:
            conn = get_db()
            cur = conn.cursor()
            # Pulling max for CE-AQI math and raw data for the trend line
            cur.execute(f"""
                SELECT pm25, timestamp FROM {table} 
                WHERE sn = %s AND timestamp > NOW() - INTERVAL '24 hours'
                ORDER BY timestamp DESC
            """, (sn,))
            rows = cur.fetchall()
            if rows:
                history = [{"pm25": r[0], "time": r[1].isoformat()} for r in rows]
                max_24h = max([r[0] for r in rows])
            cur.close()
            conn.close()
        except Exception as e:
            print(f"DB Error: {e}")

    # 3. CE-AQI CALCULATION
    # Using the live reading and the 24h historical peak from Aiven
    # (Adjust formula constants as needed for your specific logic)
    ce_aqi = (live_pm + max_24h) / 2 

    return jsonify({
        "sn": sn,
        "live": live_pm,
        "ce_aqi": round(ce_aqi),
        "history": history
    })
