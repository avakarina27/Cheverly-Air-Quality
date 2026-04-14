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
# EXPLICIT ORDER: [0]=id, [1]=pm2.5, [2]=pm1.0, [3]=pm10.0, [4]=humidity, [5]=temp, [6]=pressure
fields = "pm2.5_atm,pm1.0_atm,pm10.0_atm,humidity,temperature,pressure"
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
            
            # MAPPING BY EXACT INDEX TO PREVENT "1000 degree" ERRORS
            rows.append({
                'time_stamp': datetime.now(),
                'station_id': s_id,
                'ward_number': LOCATION_MAP.get(s_id, 'Other'),
                'pm2_5_atm': sensor[1],
                'pm1_0_atm': sensor[2],     # Ensure DBeaver column is pm1_0_atm
                'pm10_0_atm': sensor[3],    # Ensure DBeaver column is pm10_0_atm
                'humidity': sensor[4],
                'temperature': sensor[5],   # Fixed: Grabbing index 5 for Temp
                'pressure': sensor[6]       # Fixed: Grabbing index 6 for Pressure
            })
        
        df = pd.DataFrame(rows)
        engine = create_engine(DB_URL)
        
        # This will now push to the database
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Synced {len(df)} stations. Temp and PM should now be aligned.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
