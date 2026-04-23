import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
# Using the IDs directly from your dashboard HTML
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
# GroveStreams API Key and Org ID from your dashboard link
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
        # FIXED URL: /api/comp/ is the "Simple API" which allows using the compId
        url = f"https://grovestreams.com/api/comp/{dev}/last_value"
        params = {
            "api_key": GS_API_KEY,
            "org": ORG_ID
        }
        
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 200:
                # GroveStreams last_value returns a list of stream objects
                streams = res.json()
                
                bc_val = None
                lat, lon = None, None
                
                for s in streams:
                    s_id = s.get('streamId', '')
                    # The dashboard code looks for 'data', raw API often uses 'lastValue'
                    val = s.get('data') if s.get('data') is not None else s.get('lastValue')
                    
                    if s_id == "880nm":
                        bc_val = val
                    elif s_id == "lat":
                        lat = val
                    elif s_id == "long": # Note: Dashboard uses 'long' not 'lon'
                        lon = val

                rows.append({
                    'time_stamp': datetime.now(),
                    'device_id': dev,
                    'bc_880nm': bc_val,
                    'lat': lat,
                    'lon': lon
                })
                print(f"Fetched {dev}: BC={bc_val} ng/m³")
            else:
                # Printing the response text helps if there's a permission issue
                print(f"GroveStream Error {dev}: {res.status_code} - {res.text}")

        except Exception as e:
            print(f"Request failed for {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Ensure the table name matches what your dashboard eventually queries
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} C-12 readings to Aiven.")

if __name__ == "__main__":
    pull_c12_from_grove()
