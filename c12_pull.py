import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Configuration ---
# I added E10589 just in case E10588 was a typo (the spreadsheet shows 589)
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
                data = res.json()
                
                # DIAGNOSTIC: Handle cases where the list might be inside a 'stream' key
                streams = data if isinstance(data, list) else data.get('stream', [])
                
                if not streams:
                    print(f"Empty data for {dev}. Check if Device ID is correct.")
                    continue

                bc_val, lat, lon, sensor_time = None, None, None, None
                
                # Let's see what keys are actually coming back for the first device
                if dev == DEVICES[0]:
                    available_ids = [str(s.get('streamId')) for s in streams]
                    print(f"Diagnostic for {dev}: Found stream names: {available_ids}")

                for s in streams:
                    s_id = str(s.get('streamId', '')).strip().lower()
                    # Check 'data', 'lastValue', and 'value' just in case
                    raw_val = s.get('data') if s.get('data') is not None else s.get('lastValue')
                    
                    try:
                        val = float(raw_val)
                    except (ValueError, TypeError):
                        val = None

                    # CASE-INSENSITIVE MAPPING
                    if s_id == "880nm":
                        bc_val = val
                        if s.get('time'):
                            sensor_time = datetime.fromtimestamp(s.get('time') / 1000.0)
                    elif s_id == "lat":
                        lat = val
                    elif s_id == "long":
                        lon = val

                # Even if BC is 0, we want to capture it. 
                # We only skip if it's truly None (missing from the API).
                if bc_val is not None:
                    rows.append({
                        'time_stamp': sensor_time if sensor_time else datetime.now(),
                        'device_id': dev,
                        'bc_880nm': bc_val,
                        'latitude': lat,
                        'longitude': lon
                    })
                    print(f"✅ {dev}: BC={bc_val}")
                else:
                    print(f"⚠️ {dev}: No '880nm' data found in current streams.")
            else:
                print(f"❌ {dev} Error: {res.status_code}")

        except Exception as e:
            print(f"❌ {dev} Request failed: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Ensure the column names match your DBeaver exactly
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} rows to Aiven.")
    else:
        print("--- NO DATA PUSHED --- (Check diagnostic logs above)")

if __name__ == "__main__":
    pull_c12_from_grove()
