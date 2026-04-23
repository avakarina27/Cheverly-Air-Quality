import os
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv('QUANTAQ_API_KEY')
DB_URL = os.getenv('DB_URL').replace("postgresql://", "postgresql+psycopg2://")

# Your specific QuantAQ SNs
SNS = ["MOD-00745", "MOD-00746", "MOD-00747", "MOD-00748", "MOD-00749"]

def pull_quantaq():
    engine = create_engine(DB_URL)
    auth = (API_KEY, '')
    rows = []

    for sn in SNS:
        url = f"https://api.quant-aq.com/device-api/v1/devices/{sn}/data/raw/?limit=1"
        res = requests.get(url, auth=auth)
        if res.status_code == 200:
            data = res.json().get('data', [])
            if data:
                latest = data[0]
                rows.append({
                    'time_stamp': latest['timestamp'],
                    'sensor_sn': sn,
                    'pm25': latest.get('pm25'),
                    'pm10': latest.get('pm10'),
                    'lat': latest.get('geo', {}).get('lat'),
                    'lon': latest.get('geo', {}).get('lon')
                })
    
    if rows:
        pd.DataFrame(rows).to_sql('quantaq_master', engine, if_exists='append', index=False)
        print(f"Pushed {len(rows)} QuantAQ readings.")

if __name__ == "__main__":
    pull_quantaq()
