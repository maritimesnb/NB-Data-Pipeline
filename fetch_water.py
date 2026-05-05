import requests 
import json 
import os 
from datetime import datetime, timezone 
 
# ============================================================ 
# Environment Canada MSC GeoMet API — NB Hydrometric Stations 
# ============================================================ 
 
# Saint John River stations (add more as needed) 
STATIONS = [ 
    '01AK007',  # Saint John River at Fort Kent 
    '01AD002',  # Saint John River at Edmundston 
    '01AJ010',  # Saint John River at Grand Falls 
    '01AK001',  # Saint John River at Aroostook 
    '01AJ006',  # Saint John River at Aroostook (Mouth) 
    '01AO001',  # Saint John River at Fredericton 
    '01AP006',  # Saint John River at Maugerville 
    '01AQ001',  # Saint John River at Sussex 
    '01AP004',  # Nashwaak River at Marysville (near Fredericton) 
] 
 
BASE_URL = 'https://api.weather.gc.ca/collections/hydrometric-realtime/items' 
 
def fetch_station_data(station_id): 
    """Fetch last 30 days of real-time data for one station.""" 
    params = { 
        'STATION_NUMBER': station_id, 
        'f': 'json', 
        'limit': 720,  # ~30 days of hourly readings 
    } 
    try: 
        resp = requests.get(BASE_URL, params=params, timeout=30) 
        resp.raise_for_status() 
        return resp.json() 
    except Exception as e: 
        print(f'ERROR fetching {station_id}: {e}') 
        return None 
 
def main(): 
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%SZ') 
    os.makedirs('data/raw', exist_ok=True) 
 
    all_data = { 
        'fetched_at': timestamp, 
        'stations': {} 
    } 
 
    for station in STATIONS: 
        print(f'Fetching {station}...') 
        data = fetch_station_data(station) 
        if data: 
            all_data['stations'][station] = data 
            print(f'  Got {len(data.get("features", []))} records') 
 
    # Save timestamped file 
    filename = f'data/raw/water_{timestamp}.json' 
    with open(filename, 'w') as f: 
        json.dump(all_data, f, indent=2) 
 
    # Also save as 'latest.json' for easy Databricks access 
    with open('data/raw/latest.json', 'w') as f: 
        json.dump(all_data, f, indent=2) 
 
    print(f'Saved: {filename}') 
 
if __name__ == '__main__': 
    main() 
