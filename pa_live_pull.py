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

# 1. THE MASTER LABEL MAPPING
LOCATION_MAP = {
    '53677': '1', '57841': '1', '52823': '2', '57783': '2', '203601': '2',
    '54239': '3', '207729': '3', '54293': '4', '211993': '4',
    '218227': '5', '197937': '5', '57777': '6', '203577': '6', 
    '175563': '6', '218237': '6', '181253': 'TownH', 
    '57955': 'FH', '185085': 'FH', '203597': 'FH',
    '284362': 'PG', '160037': 'PG', '178169': 'PG', '184191': 'PG', '218273': 'PG',
    '57811': 'CV'
}

SENSOR_IDS = list(LOCATION_MAP.keys())
# Pulling specific fields. The order here matches the 'sensor[x]' numbers below.
fields = "pm2.5_atm,humidity,temperature,pressure"
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
            
            # THE FIX: Assigning variables to indices to prevent "Value Shifting"
            # sensor[0] = id, [1] = pm2.5, [2] = humidity, [3] = temp, [4] = pressure
            rows.append({
                'time_stamp': datetime.now(),
                'station_id': s_id,
                'ward_number': LOCATION_MAP.get(s_id, 'Other'),
                'pm2_5_atm': sensor[1],
                'humidity': sensor[2],
                'temperature': sensor[3], # Should be ~60-80F
                'pressure': sensor[4]     # Should be ~1000+
            })
        
        df = pd.DataFrame(rows)
        engine = create_engine(DB_URL)
        
        # WE ONLY SEND STABLE COLUMNS
        # This prevents the "pm10.0_ atm does not exist" crash.
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Synced {len(df)} stations. Labels and basic weather are live.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
