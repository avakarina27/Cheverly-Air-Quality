import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import os

# --- Configuration ---
# Use the IDs from your dashboard HTML
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
COMET_API_KEY = os.getenv("COMET_API_KEY")
DB_URL = os.getenv("DB_URL")

def pull_c12():
    if not COMET_API_KEY or not DB_URL:
        print("Missing Environment Variables.")
        return

    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        # CORRECTED URL for Met One COMET Cloud
        url = f"https://cloud.metone.com/api/devices/{dev}/last-readings"
        headers = {"x-api-key": COMET_API_KEY}
        
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                # Met One typically returns a list of sensor parameters
                # We need to find the Black Carbon (880nm) value
                bc_val = None
                lat, lon = None, None
                
                # Check if data is a list (standard for last-readings)
                readings = data if isinstance(data, list) else data.get('data', [])

                for item in readings:
                    label = str(item.get('label', '')).lower()
                    if '880nm' in label or 'bc' in label:
                        bc_val = item.get('value')
                    elif 'lat' in label:
                        lat = item.get('value')
                    elif 'lon' in label or 'long' in label:
                        lon = item.get('value')

                rows.append({
                    'time_stamp': datetime.now(),
                    'device_id': dev,
                    'bc_880nm': bc_val,
                    'lat': lat,
                    'lon': lon
                })
                print(f"Successfully fetched {dev}: BC={bc_val}")
            else:
                print(f"Failed {dev}: Status {res.status_code}")

        except Exception as e:
            print(f"Error pulling from {dev}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        df.to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"--- SUCCESS --- Pushed {len(rows)} C-12 readings to Aiven.")

if __name__ == "__main__":
    pull_c12()
