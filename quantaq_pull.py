import os
import requests
import pandas as pd
from sqlalchemy import create_engine

# --- Settings ---
DEVICES = ["MOD-00745"] 
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
                
                latest = data_list[0] 

                # Explicitly pull every field needed for your DBeaver table
                data_point = {
                    'time_stamp': latest.get('timestamp_local'),
                    'sensor_sn': latest.get('sn'),
                    'pm25': latest.get('pm25'),
                    'pm10': latest.get('pm10'),
                    'lat': latest.get('geo', {}).get('lat') if latest.get('geo') else latest.get('lat'),
                    'lon': latest.get('geo', {}).get('lon') if latest.get('geo') else latest.get('lon')
                }
                rows.append(data_point)
        except Exception as e:
            print(f"❌ Error: {e}")

    if rows:
        df = pd.DataFrame(rows)
        
        # --- DEBUG PRINT: Check your console/terminal for this! ---
        print("--- DEBUG: PREPARING TO PUSH THE FOLLOWING DATA ---")
        print(df) 
        print("---------------------------------------------------")

        with engine.begin() as conn:
            # We use 'append' so we don't delete old data, 
            # but we force the column names to match the DB
            df.to_sql('quantaq_master', conn, if_exists='append', index=False)
        print("✅ Data push complete.")
    else:
        print("⚠️ No data was collected. Check if the API is actually returning values.")

if __name__ == "__main__":
    pull_quantaq()
