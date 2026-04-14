import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Looks for .env locally (for your laptop); ignored on GitHub
load_dotenv()

# 1. GET CREDENTIALS FROM CLOUD SECRETS
API_KEY = os.getenv('PURPLE_AIR_API_KEY')
DB_URL = os.getenv('DB_URL')

# Fix for SQLAlchemy/Postgres compatibility in Linux environments
if DB_URL and "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://")

# 2. THE FULL CHEVERLY STATION LIST (25 IDs)
SENSOR_IDS = [
    '284362', '52823', '53677', '54239', '54293', 
    '57777', '57783', '57811', '57841', '57955', 
    '160037', '175563', '178169', '181253', '184191', 
    '185085', '197937', '203577', '203597', '203601', 
    '207729', '211993', '218227', '218237', '218273'
] 

sensor_string = ','.join(SENSOR_IDS)
# We pull sensor_index and the PM2.5 atmospheric value
API_URL = f"https://api.purpleair.com/v1/sensors?fields=pm2.5_atm&show_only={sensor_string}"

def pull_and_push():
    try:
        if not API_KEY or not DB_URL:
            print("Error: Missing API_KEY or DB_URL environment variables.")
            return

        headers = {'X-API-Key': API_KEY}
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # 3. DATA PROCESSING
        rows = []
        # data['data'] is a list of lists: [[index, pm2.5], [index, pm2.5], ...]
        for sensor in data['data']:
            rows.append({
                'sensor_index': sensor[0],
                'pm2_5': sensor[1],
                'time_stamp': datetime.now()
            })
        
        df = pd.DataFrame(rows)
        
        if df.empty:
            print("Warning: No data found for the provided Sensor IDs.")
            return

        # 4. PUSH TO AIVEN POSTGRES
        engine = create_engine(DB_URL)
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        
        print(f"--- SUCCESS ---")
        print(f"Time: {datetime.now()}")
        print(f"Stations Synced: {len(df)}")
        print(f"Sent to: {DB_URL.split('@')[1]}") # Prints DB host for confirmation

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    pull_and_push()
