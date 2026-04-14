import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# 1. SETUP & CREDENTIALS
load_dotenv()
API_KEY = os.getenv('PURPLE_AIR_API_KEY')
DB_URL = os.getenv('DB_URL')

# Fix for SQLAlchemy/Postgres compatibility
if DB_URL and "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://")

# 2. STATION LIST & API SETTINGS
SENSOR_IDS = [
    '284362', '52823', '53677', '54239', '54293', 
    '57777', '57783', '57811', '57841', '57955', 
    '160037', '175563', '178169', '181253', '184191', 
    '185085', '197937', '203577', '203597', '203601', 
    '207729', '211993', '218227', '218237', '218273'
] 

sensor_string = ','.join(SENSOR_IDS)
# We match these fields to your DBeaver column names
fields = "pm2.5_atm,humidity,temperature"
API_URL = f"https://api.purpleair.com/v1/sensors?fields={fields}&show_only={sensor_string}"

def pull_and_push():
    try:
        if not API_KEY or not DB_URL:
            print("Error: Missing API_KEY or DB_URL secrets in GitHub.")
            return

        headers = {'X-API-Key': API_KEY}
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # 3. DATA PROCESSING (Matching DBeaver exactly)
        rows = []
        for sensor in data['data']:
            rows.append({
                'station_id': sensor[0],      # Matches your station_id column
                'pm2_5_atm': sensor[1],       # Matches your pm2.5_atm column
                'humidity': sensor[2],        # Matches your humidity column
                'temperature': sensor[3],     # Matches your temperature column
                'time_stamp': datetime.now()  # Matches your time_stamp column
            })
        
        df = pd.DataFrame(rows)
        
        # 4. PUSH TO AIVEN
        engine = create_engine(DB_URL)
        # We use 'append' so we don't delete your old data work!
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Synced {len(df)} stations to Aiven at {datetime.now()}")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
