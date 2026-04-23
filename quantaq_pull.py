import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Settings ---
# Add all your MOD serial numbers here
DEVICES = ["MOD-00745"] 
BASE_URL = "https://cheverly-air-quality.vercel.app/api/aq"
DB_URL = os.getenv("DB_URL")

def pull_quantaq():
    if not DB_URL:
        return

    engine = create_engine(DB_URL)
    rows = []

    for sn in DEVICES:
        # Note: adjust params based on how your Vercel proxy handles QuantAQ
        params = {"action": "quantaq_last", "sn": sn} 
        
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                full_response = res.json()
                
                # CRITICAL: QuantAQ puts data in a list called 'data'
                # We usually only want the most recent entry (index 0)
                data_list = full_response.get('data', [])
                
                if not data_list:
                    print(f"No data returned for {sn}")
                    continue
                
                latest = data_list[0] 

                data_point = {
                    'device_id': latest.get('sn'),
                    'pm25': latest.get('pm25'),
                    'pm10': latest.get('pm10'),
                    'no2': latest.get('no2'),
                    'co': latest.get('co'),
                    'temp': latest.get('temp'),
                    'humidity': latest.get('rh'),
                    # Using 'timestamp_local' because it's already in our timezone!
                    'time_stamp': latest.get('timestamp_local') 
                }
                
                rows.append(data_point)
                print(f"✅ Captured {sn} at {data_point['time_stamp']}")
                
        except Exception as e:
            print(f"❌ Error pulling {sn}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            df.to_sql('quantaq_master', conn, if_exists='append', index=False)
        print(f"--- Pushed {len(rows)} QuantAQ rows ---")

if __name__ == "__main__":
    pull_quantaq()
