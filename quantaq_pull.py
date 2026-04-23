import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# --- Settings ---
DEVICES = ["MOD-00745", "MOD-00746", "MOD-00747", "MOD-00748", "MOD-00749"] 
BASE_URL = "https://cheverly-air-quality.vercel.app/api/aq"
DB_URL = os.getenv("DB_URL")

def pull_quantaq():
    if not DB_URL:
        print("❌ DB_URL not found.")
        return

    engine = create_engine(DB_URL)
    rows = []

    for sn in DEVICES:
        params = {"action": "quantaq_last", "sn": sn} 
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                data_list = res.json().get('data', [])
                if not data_list:
                    continue
                
                # Take the very first entry in the 'data' list
                latest = data_list[0] 

                # Precise mapping based on your JSON snippet
                data_point = {
                    'time_stamp': latest.get('timestamp_local'),
                    'sensor_sn': latest.get('sn'),
                    'pm25': latest.get('pm25'),
                    'pm10': latest.get('pm10'),
                    # Pulling from the "geo" object as seen in your JSON
                    'lat': latest.get('geo', {}).get('lat') if latest.get('geo') else None,
                    'lon': latest.get('geo', {}).get('lon') if latest.get('geo') else None
                }
                rows.append(data_point)
        except Exception as e:
            print(f"❌ Error pulling {sn}: {e}")

    if rows:
        df = pd.DataFrame(rows)
        
        # Printing this ensures you see the data BEFORE it goes to the DB
        print("--- DATA PREVIEW ---")
        print(df.head()) 

        with engine.begin() as conn:
            # Pushing to your 'quantaq_master' table
            df.to_sql('quantaq_master', conn, if_exists='append', index=False)
        print("✅ Data successfully sent to DBeaver.")
    else:
        print("⚠️ No data was processed.")

if __name__ == "__main__":
    pull_quantaq()
