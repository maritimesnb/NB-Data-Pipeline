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
import csv

def flatten_to_csv(all_data, filename):
    rows = []
    for station_id, station_data in all_data['stations'].items():
        for feature in station_data.get('features', []):
            props = feature.get('properties', {})
            geom  = feature.get('geometry', {}).get('coordinates', [None, None])
            level = props.get('LEVEL')
            
            # Flood risk classification
            thresholds = {
                '01AO001': (5.5, 6.0, 6.4),  # Fredericton
                '01AJ010': (4.0, 4.8, 5.5),  # Grand Falls
            }.get(station_id, (4.0, 5.0, 6.0))
            
            if level is None:
                status = 'UNKNOWN'
            elif level >= thresholds[2]:
                status = 'FLOOD'
            elif level >= thresholds[1]:
                status = 'WARNING'
            elif level >= thresholds[0]:
                status = 'WATCH'
            else:
                status = 'NORMAL'

            rows.append({
                'station_id':    station_id,
                'station_name':  props.get('STATION_NAME', ''),
                'timestamp':     props.get('DATETIME', ''),
                'water_level_m': level,
                'discharge_cms': props.get('DISCHARGE'),
                'longitude':     geom[0] if geom else None,
                'latitude':      geom[1] if geom else None,
                'flood_status':  status,
                'fetched_at':    all_data['fetched_at']
            })

    os.makedirs('data/clean', exist_ok=True)
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f'Clean CSV saved: {filename}')

# Add these two lines inside your main() function after saving the JSON:
flatten_to_csv(all_data, 'data/clean/latest_clean.csv')
flatten_to_csv(all_data, f'data/clean/water_clean_{timestamp}.csv')
