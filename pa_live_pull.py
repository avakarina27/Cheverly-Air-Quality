import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

# Looks for .env locally; ignored on GitHub
load_dotenv()

# 1. GET CREDENTIALS
API_KEY = os.getenv('PURPLE_AIR_API_KEY')
DB_URL = os.getenv('DB_URL')

# Fix for SQLAlchemy/Postgres compatibility
if DB_URL and "postgresql://" in DB_URL:
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg2://")

# 2. PURPLEAIR SETTINGS
SENSOR_IDS = ['181253', '184191', '185085', '175563'] 
sensor_string = ','.join(SENSOR_IDS)
API_URL = f"https://api.purpleair.com/v1/sensors?fields=pm2.5_cf_1&show_only={sensor_string}"

def pull_and_push():
    try:
        headers = {'X-API-Key': API_KEY}
        response = requests.get(API_URL, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        rows = []
        for sensor in data['data']:
            rows.append({
                'sensor_index': sensor[0],
                'pm2_5': sensor[1],
                'time_stamp': datetime.now()
            })
        
        df = pd.DataFrame(rows)
        
        # 3. PUSH TO AIVEN
        engine = create_engine(DB_URL)
        df.to_sql('purple_air_master', engine, if_exists='append', index=False)
        print(f"Success! {len(df)} rows added at {datetime.now()}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    pull_and_push()
