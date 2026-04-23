import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646", "E10589"]
GS_API_KEY = "40685f12-d3e5-316e-a274-e0a628c20c97"
ORG_ID = "23e37932-cc5d-350d-af25-38ae3fe54c3d"
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("Missing DB_URL")
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
                    # BRUTE FORCE: Check every key in the dictionary for the ID
                    # Some versions of the API use 'id', 'streamId', or 'uid'
                    s_id = str(next((s[k] for k in ['streamId', 'id', 'uid'] if k in s), '')).strip().lower()
                    
                    # Same for the data value: could be 'data', 'lastValue', or 'val'
                    raw_val = next((s[k] for k in ['data', 'lastValue', 'value', 'v'] if k in s), None)
                    
                    try:
                        val = float(raw_val)
                    except (ValueError, TypeError):
                        val = None

                    if s_id == "880nm":
                        bc_val = val
                        # Pull time from any key that looks like 'time' or 't'
                        t_val = next((s[k] for k in ['time', 't', 'lastValueTime'] if k in s), None)
                        if t_val:
                            sensor_time = datetime.fromtimestamp(t_val / 1000.0)
                    elif s_id == "lat":
                        lat = val
                    elif s_id in ["long", "lon"]:
                        lon = val

                if bc_val is not None:
                    rows.append({
                        'time_stamp': sensor_time if sensor_time else datetime.now(),
                        'device_id': dev,
                        'bc_880nm': bc_val,
                        'latitude': lat,
                        'longitude': lon
                    })
                    print(f"✅ {dev} Success: BC={bc_val}")
                else:
                    # If it still fails, print the whole dictionary for the first failure
                    if dev == DEVICES[0]:
                        print(f"DEBUG {dev} Raw Data: {streams[0] if streams else 'Empty'}")
            else:
                print(f"❌ {dev} Error: {res.status_code}")

        except Exception as e:
            print(f"❌ {dev} Error: {e}")

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            df.to_sql('c12_master', conn, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} rows.")
    else:
        print("--- STILL NO DATA --- Check the DEBUG Raw Data line in logs.")

if __name__ == "__main__":
    pull_c12_from_grove()
