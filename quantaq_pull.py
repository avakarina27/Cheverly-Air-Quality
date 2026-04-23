import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# --- Settings ---
# Be sure to add all serial numbers to this list
DEVICES = ["MOD-00745", "MOD-00746", "MOD-00747", "MOD-00748", "MOD-00749"] 
BASE_URL = "https://cheverly-air-quality.vercel.app/api/aq"
DB_URL = os.getenv("DB_URL")

def pull_quantaq():
    if not DB_URL:
        print("❌ DB_URL environment variable is missing.")
        return

    engine = create_engine(DB_URL)
    rows = []

    for sn in DEVICES:
        params = {"action": "quantaq_last", "sn": sn} 
        
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                full_response = res.json()
                data_list = full_response.get('data', [])
                
                if not data_list:
                    print(f"No data returned for {sn}")
                    continue
                
                # Get the most recent data point
                latest = data_list[0] 

                # MATCHING TO YOUR DBEAVER COLUMNS
                data_point = {
                    'time_stamp': latest.get('timestamp_local'), # Matches 'time_stamp'
                    'sensor_sn': latest.get('sn'),               # Matches 'sensor_sn'
                    'pm25': latest.get('pm25'),                 # Matches 'pm25'
                    'pm10': latest.get('pm10'),                 # Matches 'pm10'
                    'lat': latest.get('geo', {}).get('lat'),     # Matches 'lat'
                    'lon': latest.get('geo', {}).get('lon')      # Matches 'lon'
                }
                
                rows.append(data_point)
                print(f"✅ Prepared {sn} for push.")
                
        except Exception as e:
            print(f"❌ Error pulling {sn}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            # Pushing to your 'quantaq_master' table
            df.to_sql('quantaq_master', conn, if_exists='append', index=False)
        print(f"--- Successfully pushed {len(rows)} rows to DBeaver ---")

if __name__ == "__main__":
    pull_quantaq()
