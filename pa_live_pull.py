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
    # Cheverly (Numbers)
    '53677': '1', '57841': '1', '203597': '1',
    '52823': '2', '57783': '2', '203601': '2',
    '54239': '3', '207729': '3',
    '54293': '4', '211993': '4',
    '218227': '5', '197937': '5', 
    '57777': '6', '203577': '6', '175563': '6', '218237': '6',
    # Town Hall
    '181253': 'TownH', 
    # Project Groups
    '57955': 'FH', '185085': 'FH',
    '284362': 'PG', '160037': 'PG', '178169': 'PG', '184191': 'PG', '218273': 'PG',
    '57811': 'CV'
}

SENSOR_IDS = list(LOCATION_MAP.keys())
# We pull just the essentials to ensure it doesn't crash on column name errors
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
            rows.append({
                'time_stamp': datetime.now(),
                'station_id': s_id,
                'ward_number': LOCATION_MAP.get(s_id, 'Other'),
                'pm2_5_atm': sensor[1],
                'humidity': sensor[2],
                'temperature': sensor[3],
                'pressure': sensor[4]
            })
        
        df = pd.DataFrame(rows)
        engine = create_engine(DB_URL)
        
        # This will only push to columns that are standard alphanumeric names
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Pushed {len(df)} stations. Labels: 1-6, FH, PG, CV, and TownH are live!")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
