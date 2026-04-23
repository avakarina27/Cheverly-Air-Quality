import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
# These are the Component IDs from your dashboard/spreadsheet
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
# Your GroveStreams API Key
GS_API_KEY = "40685f12-d3e5-316e-a274-e0a628c20c97"
# Your Aiven DB URL from GitHub Secrets
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("Missing DB_URL environment variable.")
        return

    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        # GroveStreams API URL to get the latest values for a component
        url = f"https://grovestreams.com/api/component?compId={dev}"
        params = {"api_key": GS_API_KEY}
        
        try:
            res = requests.get(url, params=params, timeout=15)
            if res.status_code == 200:
                data = res.json()
                
                # GroveStreams stores data in "streams"
                streams = data.get('streams', [])
                
                bc_val = None
                lat, lon = None, None
                
                for s in streams:
                    stream_id = s.get('streamId', '')
                    # Look for the 880nm stream (Black Carbon)
                    if stream_id == "880nm":
                        bc_val = s.get('lastValue')
                    elif stream_id == "lat":
                        lat = s.get('lastValue')
                    elif stream_id == "long":
                        lon = s.get('lastValue')

                rows.append({
                    'time_stamp': datetime.now(),
                    'device_id': dev,
                    'bc_880nm': bc_val,
                    'lat': lat,
                    'lon': lon
                })
                print(f"Fetched GroveStream {dev}: BC={bc_val}")
            else:
                print(f"GroveStream Error {dev}: {res.status_code}")

        except Exception as e:
            print(f"Request failed for {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Using if_exists='append' to keep your 8-hour window growing
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} GroveStream readings to Aiven.")

if __name__ == "__main__":
    pull_c12_from_grove()
