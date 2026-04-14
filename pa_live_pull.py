import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('PURPLE_AIR_API_KEY')
DB_URL = os.getenv('DB_URL')

if DB_URL and "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://")

# 1. STATION MAPPING (Unified List)
WARD_MAP = {
    # Cheverly & FH
    '53677': 1, '57777': 2, '203601': 3, '207729': 4, '54293': 5, '203577': 6,
    '57841': 1, '52823': 2, '54239': 3, '57783': 4, '57811': 6,
    '57955': 7, '185085': 7, '203597': 7,
    # PG Stations
    '284362': 8, '160037': 8, '175563': 8, '178169': 8, '184191': 8, 
    '197937': 8, '218227': 8, '218237': 8, '218273': 8,
    # CV Stations
    '52823': 9, '203577': 9, '203601': 9, '207729': 9, '181253': 9, '211993': 9
}

SENSOR_IDS = list(WARD_MAP.keys())
# We pull the specific fields that match your DBeaver columns
# Note: PurpleAir provides 'a' and 'b' channel data separately
fields = "pm2.5_atm,pm2.5_atm_a,pm2.5_atm_b,humidity,temperature,pressure"
API_URL = f"https://api.purpleair.com/v1/sensors?fields={fields}&show_only={','.join(SENSOR_IDS)}"

def pull_and_push():
    try:
        headers = {'X-API-Key': API_KEY}
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        rows = []
        for sensor in data['data']:
            s_id = str(sensor[0])
            rows.append({
                'time_stamp': datetime.now(),
                'station_id': s_id,
                'ward_number': WARD_MAP.get(s_id, 0),
                'pm2_5_atm': sensor[1],   # Main PM2.5
                'pm2_5_atm_a': sensor[2], # Channel A
                'pm2_5_atm_b': sensor[3], # Channel B
                'humidity': sensor[4],
                'temperature': sensor[5],
                'pressure': sensor[6]
            })
        
        df = pd.DataFrame(rows)
        engine = create_engine(DB_URL)
        
        # We only send columns that exist in your DBeaver list to avoid errors
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Synced {len(df)} stations to Aiven.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
