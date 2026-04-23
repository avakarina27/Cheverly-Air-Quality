import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# --- Configuration ---
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
GS_API_KEY = "40685f12-d3e5-316e-a274-e0a628c20c97"
ORG_ID = "23e37932-cc5d-350d-af25-38ae3fe54c3d"
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("CRITICAL: DB_URL environment variable is missing!")
        return

    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        url = f"https://grovestreams.com/api/comp/{dev}/last_value"
        params = {"api_key": GS_API_KEY, "org": ORG_ID}
        
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 200:
                streams = res.json()
                
                bc_val, lat, lon, sensor_time = None, None, None, None
                
                for s in streams:
                    s_id = str(s.get('streamId', '')).strip()
                    raw_val = s.get('data')
                    
                    try:
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

                # We only want to add a row if we actually got a Black Carbon value
                if bc_val is not None:
                    rows.append({
                        'time_stamp': sensor_time if sensor_time else datetime.now(),
                        'device_id': dev,
                        'bc_880nm': bc_val,
                        'latitude': lat,
                        'longitude': lon
                    })
                    print(f"✅ Prepared {dev}: BC={bc_val}")
                else:
                    print(f"⚠️ {dev} returned no BC data (Value: {bc_val})")
            else:
                print(f"❌ GroveStream Error {dev}: {res.status_code}")

        except Exception as e:
            print(f"❌ Network Error for {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        print(f"--- ATTEMPTING PUSH: {len(df)} rows found ---")
        
        try:
            # Force the push using 'append'
            with engine.begin() as connection:
                df.to_sql('c12_master', connection, if_exists='append', index=False)
            print("--- DATABASE PUSH COMPLETE ---")
        except Exception as e:
            print(f"❌ DATABASE ERROR: {e}")
    else:
        print("❌ NO DATA FOUND: No rows were added to the dataframe.")

if __name__ == "__main__":
    pull_c12_from_grove()
