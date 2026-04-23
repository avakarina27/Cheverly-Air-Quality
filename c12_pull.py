import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# Using the key found in your 'Cheverly C-12s.xlsx'
COMET_API_KEY = "40685f12-d3e5-316e-a274-e0a628c20c97"
DB_URL = os.getenv('DB_URL').replace("postgresql://", "postgresql+psycopg2://")

# Devices from your C12S list
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]

def pull_c12():
    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        url = f"https://api.comet.com/v1/devices/{dev}/last-readings"
        headers = {"Authorization": f"Bearer {COMET_API_KEY}"}
        res = requests.get(url, headers=headers)
        
        if res.status_code == 200:
            data = res.json() # Assuming standard Comet JSON structure
            # Logic to find the 880nm (BC) and Lat/Long
            bc_val = next((item['value'] for item in data if '880nm' in item['name']), None)
            lat = next((item['value'] for item in data if 'lat' in item['name']), None)
            lon = next((item['value'] for item in data if 'long' in item['name']), None)

            rows.append({
                'time_stamp': datetime.now(),
                'device_id': dev,
                'bc_880nm': bc_val,
                'lat': lat,
                'lon': lon
            })

    if rows:
        pd.DataFrame(rows).to_sql('c12_master', engine, if_exists='append', index=False)
        print(f"Pushed {len(rows)} C-12 readings.")

if __name__ == "__main__":
    pull_c12()
