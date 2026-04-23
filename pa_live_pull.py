import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('PURPLE_AIR_API_KEY')
DB_URL = os.getenv('DB_URL')

# Fix DB URL for SQLAlchemy
if DB_URL and "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://")

LOCATION_MAP = {
    '53677': '1', '57841': '1', '52823': '2', '57783': '2', '203601': '2', '156595': '2',
    '54239': '3', '207729': '3', '54293': '4', '211993': '4',
    '218227': '5', '197937': '5', '57777': '6', '203577': '6', 
    '175563': '6', '218237': '6', '181253': 'TownH', 
    '57955': 'FH', '185085': 'FH', '203597': 'FH',
    '284362': 'PG', '160037': 'PG', '178169': 'PG', '184191': 'PG', '218273': 'PG',
    '57811': 'CV'
}

def backfill_gap():
    engine = create_engine(DB_URL)
    headers = {'X-API-Key': API_KEY}
    
    # Define the 24-hour window (Start of gap to end of gap)
    start_ts = int((datetime.now() - timedelta(hours=30)).timestamp())
    
    fields = "pm2.5_atm,pm1.0_atm,pm10.0_atm,humidity,temperature,pressure"
    
    for s_id in LOCATION_MAP.keys():
        print(f"Backfilling station {s_id}...")
        url = f"https://api.purpleair.com/v1/sensors/{s_id}/history?start_timestamp={start_ts}&fields={fields}"
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            rows = []
            for entry in data['data']:
                # The history endpoint returns time_stamp as entry[0]
                rows.append({
                    'time_stamp': datetime.fromtimestamp(entry[0]),
                    'station_id': s_id,
                    'ward_number': LOCATION_MAP.get(s_id, 'Other'),
                    'pm2.5_atm': entry[1],
                    'pm1.0_atm': entry[2],
                    'pm10.0_atm': entry[3],
                    'humidity': entry[4],
                    'temperature': entry[5],
                    'pressure': entry[6]
                })
            
            if rows:
                df = pd.DataFrame(rows)
                df.to_sql('purple_air_master', engine, if_exists='append', index=False, method='multi')
                print(f"Done. Added {len(df)} historical rows.")
                
        except Exception as e:
            print(f"Failed for {s_id}: {e}")

if __name__ == "__main__":
    backfill_gap()
