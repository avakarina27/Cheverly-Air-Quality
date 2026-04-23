import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
# We use your Vercel proxy URL which we know works
BASE_URL = "https://cheverly-air-quality.vercel.app/api/aq"
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("Missing DB_URL")
        return

    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        # Match the exact parameters used by your dashboard
        params = {
            "action": "grove_last",
            "compId": dev
        }
        
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                streams = res.json()
                
                bc_val, lat, lon, sensor_time = None, None, None, None
                
                for s in streams:
                    # The proxy uses these exact keys
                    s_id = str(s.get('streamId', '')).strip()
                    raw_val = s.get('data')
                    
                    try:
                        # Convert values like 38.989616 but ignore "#UNKNOWN_EXCEPTION"
                        val = float(raw_val)
                    except (ValueError, TypeError):
                        val = None

                    if s_id == "880nm":
                        bc_val = val
                        if s.get('time'):
                            sensor_time = datetime.fromtimestamp(s.get('time') / 1000.0)
                    elif s_id == "lat":
                        lat = val
                    elif s_id == "long":
                        lon = val

                if bc_val is not None:
                    rows.append({
                        'time_stamp': sensor_time if sensor_time else datetime.now(),
                        'device_id': dev,
                        'bc_880nm': bc_val,
                        'latitude': lat,
                        'longitude': lon
                    })
                    print(f"✅ Captured {dev}: BC={bc_val}")
                else:
                    print(f"⚠️ {dev}: No BC data found (Current: {bc_val})")
            else:
                print(f"❌ {dev} API Error: {res.status_code}")

        except Exception as e:
            print(f"❌ {dev} Connection Error: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Using a transaction block to ensure data lands in Aiven
        with engine.begin() as conn:
            df.to_sql('c12_master', conn, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} rows to Aiven.")
    else:
        print("--- NO DATA TO PUSH ---")

if __name__ == "__main__":
    pull_c12_from_grove()
