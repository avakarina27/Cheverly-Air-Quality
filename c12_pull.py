import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone

# --- Settings ---
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
BASE_URL = "https://cheverly-air-quality.vercel.app/api/aq"
DB_URL = os.getenv("DB_URL")

def pull_c12_from_grove():
    if not DB_URL:
        print("Missing DB_URL")
        return

    engine = create_engine(DB_URL)
    rows = []

    for dev in DEVICES:
        params = {"action": "grove_last", "compId": dev}
        
        try:
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                streams = res.json()
                
                # Default to current time in Eastern (UTC-4)
                # This handles the "4 hours ahead" issue by shifting it back
                et_now = datetime.now(timezone.utc) - timedelta(hours=4)
                
                data_point = {
                    'device_id': dev, 
                    'bc_880nm': None, 
                    'latitude': None, 
                    'longitude': None, 
                    'time_stamp': et_now
                }
                
                for s in streams:
                    s_id = str(s.get('streamId', ''))
                    val = s.get('data')
                    
                    try:
                        clean_val = float(val)
                    except:
                        clean_val = None

                    if s_id == "880nm":
                        data_point['bc_880nm'] = clean_val
                        if s.get('time'):
                            # Convert sensor UTC time to Eastern Time
                            sensor_utc = datetime.fromtimestamp(s.get('time') / 1000.0, tz=timezone.utc)
                            data_point['time_stamp'] = sensor_utc - timedelta(hours=4)
                    elif s_id == "lat":
                        data_point['latitude'] = clean_val
                    elif s_id == "long":
                        data_point['longitude'] = clean_val

                if data_point['bc_880nm'] is not None:
                    rows.append(data_point)
                    print(f"✅ Success: {dev} | BC: {data_point['bc_880nm']} | Time: {data_point['time_stamp']}")
            else:
                print(f"❌ API Error for {dev}: {res.status_code}")
        except Exception as e:
            print(f"❌ Connection Error: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # Convert the column to string or remove timezone info so Postgres doesn't get confused
        df['time_stamp'] = df['time_stamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        with engine.begin() as conn:
            df.to_sql('c12_master', conn, if_exists='append', index=False)
        print(f"--- DATABASE UPDATED: Pushed {len(rows)} rows ---")
    else:
        print("--- NO DATA PUSHED ---")

if __name__ == "__main__":
    pull_c12_from_grove()
