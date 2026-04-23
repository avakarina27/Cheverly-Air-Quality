import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- Settings ---
# These IDs match the ones currently showing data on your dashboard
DEVICES = ["D14781", "D14645", "D17615", "E10588", "D14646"]
# This is the Vercel URL that your team's dashboard uses
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
            # We hit the proxy because it already cleaned the data for us
            res = requests.get(BASE_URL, params=params, timeout=15)
            if res.status_code == 200:
                streams = res.json()
                
                # These are our target buckets
                data_point = {'device_id': dev, 'bc_880nm': None, 'latitude': None, 'longitude': None, 'time_stamp': datetime.now()}
                
                for s in streams:
                    s_id = str(s.get('streamId', ''))
                    val = s.get('data')
                    
                    # Convert to number, but ignore text like "#EXCEPTION"
                    try:
                        clean_val = float(val)
                    except:
                        clean_val = None

                    # Direct matching based on your dashboard's keys
                    if s_id == "880nm":
                        data_point['bc_880nm'] = clean_val
                        if s.get('time'):
                            data_point['time_stamp'] = datetime.fromtimestamp(s.get('time') / 1000.0)
                    elif s_id == "lat":
                        data_point['latitude'] = clean_val
                    elif s_id == "long":
                        data_point['longitude'] = clean_val

                # Only add if we actually found a BC value
                if data_point['bc_880nm'] is not None:
                    rows.append(data_point)
                    print(f"✅ Success: {dev} | BC: {data_point['bc_880nm']}")
            else:
                print(f"❌ API Error for {dev}: {res.status_code}")
        except Exception as e:
            print(f"❌ Connection Error: {e}")

    if rows:
        df = pd.DataFrame(rows)
        # We use 'append' so your existing successful rows stay there
        with engine.begin() as conn:
            df.to_sql('c12_master', conn, if_exists='append', index=False)
        print(f"--- DATABASE UPDATED: Pushed {len(rows)} rows ---")
    else:
        print("--- STILL NO DATA --- (Check if the Vercel site is down or loading in your browser)")

if __name__ == "__main__":
    pull_c12_from_grove()
