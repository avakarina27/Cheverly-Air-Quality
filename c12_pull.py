import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
GS_API_KEY = "40685f12-d3e5-316e-a274-e0a628c20c97"
ORG_ID = "23e37932-cc5d-350d-af25-38ae3fe54c3d"
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("Missing DB_URL environment variable.")
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
                
                bc_val, lat, lon = None, None, None
                
                for s in streams:
                    s_id = s.get('streamId', '')
                    # GroveStreams often nests the value in 'lastValue' or 'data'
                    # We check both and ensure it's not an empty string
                    raw_val = s.get('lastValue') if s.get('lastValue') is not None else s.get('data')
                    
                    if raw_val is not None:
                        try:
                            # Convert to float to ensure it's not a null string
                            val = float(raw_val)
                        except (ValueError, TypeError):
                            val = None
                    else:
                        val = None

                    if s_id == "880nm":
                        bc_val = val
                    elif s_id == "lat":
                        lat = val
                    elif s_id == "long":
                        lon = val

                rows.append({
                    'time_stamp': datetime.now(),
                    'device_id': dev,
                    'bc_880nm': bc_val,
                    'lat': lat,
                    'lon': lon
                })
                print(f"Station {dev} -> BC: {bc_val}, Lat: {lat}, Lon: {lon}")
            else:
                print(f"Error {dev}: {res.status_code}")

        except Exception as e:
            print(f"Request failed for {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Check if we have at least some non-null data before pushing
        if df['bc_880nm'].isnull().all():
            print("WARNING: All BC values are null. Check streamId names in GroveStreams.")
            
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} rows to c12_master.")

if __name__ == "__main__":
    pull_c12_from_grove()
