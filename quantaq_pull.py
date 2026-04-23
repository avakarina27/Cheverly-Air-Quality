import requests
import json
from datetime import datetime, timedelta

# Configuration
API_KEY = "QC2TTD7QPKL1GXSTHDXAXOC3"  # Replace with your actual key
BASE_URL = "https://api.quantaq.com/v1"

def get_quantaq_data(sn, date_str=None):
    """
    Fetches PM2.5 data for a specific QuantAQ sensor.
    If no date is provided, it defaults to the last 24 hours.
    """
    if not date_str:
        # Default to last 24 hours if no date provided
        start_time = (datetime.utcnow() - timedelta(days=1)).isoformat() + "Z"
    else:
        # Expecting date_str in 'YYYY-MM-DD'
        start_time = f"{date_str}T00:00:00Z"

    endpoint = f"{BASE_URL}/devices/{sn}/data/"
    
    params = {
        "start": start_time,
        "limit": 1000,
        "sort": "timestamp,asc"
    }

    try:
        response = requests.get(
            endpoint, 
            auth=(API_KEY, ""), 
            params=params, 
            timeout=10
        )
        
        if response.status_code == 200:
            raw_data = response.json()
            # Extracting only the relevant fields for the dashboard
            processed_list = []
            
            for entry in raw_data.get("data", []):
                # Handle cases where keys might be pm25 or pm2_5
                pm25_val = entry.get("pm25") or entry.get("pm2_5")
                
                processed_list.append({
                    "timestamp": entry.get("timestamp"),
                    "pm25": pm25_val,
                    "sn": sn
                })
            
            return {"status": "success", "data": processed_list}
        else:
            print(f"Error: Received status code {response.status_code}")
            return {"status": "error", "message": response.text}

    except Exception as e:
        print(f"Connection failed: {e}")
        return {"status": "error", "message": str(e)}

def compute_aqi(pm25):
    """
    Helper function to calculate AQI from PM2.5 concentration.
    Matches the breakpoints used in your JavaScript dashboard.
    """
    if pm25 is None: return None
    
    if pm25 <= 12.0:
        return round(((50 - 0) / (12.0 - 0.0)) * (pm25 - 0.0) + 0)
    elif pm25 <= 35.4:
        return round(((100 - 51) / (35.4 - 12.1)) * (pm25 - 12.1) + 51)
    elif pm25 <= 55.4:
        return round(((150 - 101) / (55.4 - 35.5)) * (pm25 - 35.5) + 101)
    elif pm25 <= 150.4:
        return round(((200 - 151) / (150.4 - 55.5)) * (pm25 - 55.5) + 151)
    else:
        # Simple cap for hazardous values
        return 301

if __name__ == "__main__":
    # Test with one of your known SNs
    test_sn = "MOD-00745"
    print(f"--- Fetching data for {test_sn} ---")
    
    result = get_quantaq_data(test_sn)
    
    if result["status"] == "success":
        data_points = result["data"]
        print(f"Retrieved {len(data_points)} data points.")
        
        if data_points:
            latest = data_points[-1]
            aqi = compute_aqi(latest['pm25'])
            print(f"Latest Timestamp: {latest['timestamp']}")
            print(f"Latest PM2.5: {latest['pm25']} µg/m³")
            print(f"Calculated AQI: {aqi}")
    else:
        print(f"Failed to fetch: {result['message']}")
