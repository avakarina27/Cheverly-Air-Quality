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
    '53677': '1', '57841': '1', '52823': '2', '57783': '2', '203601': '2', '156595': '2',
    '54239': '3', '207729': '3', '54293': '4', '211993': '4',
    '218227': '5', '197937': '5', '57777': '6', '203577': '6', 
    '175563': '6', '218237': '6', '181253': 'TownH', 
    '57955': 'FH', '185085': 'FH', '203597': 'FH',
    '284362': 'PG', '160037': 'PG', '178169': 'PG', '184191': 'PG', '218273': 'PG',
    '57811': 'CV'
}


SENSOR_IDS = list(LOCATION_MAP.keys())
# Order: [0]id, [1]pm2.5_atm, [2]pm1.0_atm, [3]pm10.0_atm, [4]humidity, [5]temp, [6]pressure
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
            rows.append({
                'time_stamp': datetime.now(),
                'station_id': s_id,
                'ward_number': LOCATION_MAP.get(s_id, 'Other'),
                'pm2.5_atm': sensor[1],
                'pm1.0_atm': sensor[2],
                'pm10.0_atm': sensor[3],   # Removed the space here
                'humidity': sensor[4],
                'temperature': sensor[5],
                'pressure': sensor[6]
            })
  

        df = pd.DataFrame(rows)
        engine = create_engine(DB_URL)

        

        # Use 'method=multi' to ensure all columns are treated as part of one big transaction
        df.to_sql('purple_air_master', engine, if_exists='append', index=False, method='multi')

        

        print(f"--- SUCCESS ---")
        print(f"Pushed {len(df)} stations. PM values should finally be in the table!")



    except Exception as e:
        print(f"CRITICAL ERROR: {e}")



if __name__ == "__main__":
    pull_and_push()
