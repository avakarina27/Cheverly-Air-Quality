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
                
                bc_val, lat, lon, sensor_time = None, None, None, None
                
                for s in streams:
                    # Clean up the ID (e.g., '880nm', 'lat', 'long')
                    s_id = str(s.get('streamId', '')).strip()
                    raw_val = s.get('data')
                    
                    # Convert to float, safely ignoring strings like "#EXCEPTION"
                    try:
                        val = float(raw_val)
                    except (ValueError, TypeError):
                        val = None

                    if s_id == "880nm":
                        bc_val = val
                        # Get the timestamp from the BC reading
                        if s.get('time'):
                            sensor_time = datetime.fromtimestamp(s.get('time') / 1000.0)
                    elif s_id == "lat":
                        lat = val
                    elif s_id == "long":
                        lon = val

                rows.append({
                    'time_stamp': sensor_time if sensor_time else datetime.now(),
                    'device_id': dev,
                    'bc_880nm': bc_val,
                    'latitude': lat,
                    'longitude': lon
                })
                print(f"Parsed {dev}: BC={bc_val}, Lat={lat}, Lon={lon}")
            else:
                print(f"Error {dev}: {res.status_code}")

        except Exception as e:
            print(f"Request failed for {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Re-creating the table with matching column names
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Data pushed to Aiven table 'c12_master'")

if __name__ == "__main__":
    pull_c12_from_grove()
