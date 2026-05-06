import requests
import json
import os
import csv
from datetime import datetime, timezone

# ============================================================
# Environment Canada MSC GeoMet API — NB Hydrometric Stations
# ============================================================

STATIONS = [
    '01AK007',
    '01AD002',
    '01AJ010',
    '01AK001',
    '01AJ006',
    '01AO001',
    '01AP006',
    '01AQ001',
    '01AP004',
]

BASE_URL = 'https://api.weather.gc.ca/collections/hydrometric-realtime/items'


def fetch_station_data(station_id):
    params = {
        'STATION_NUMBER': station_id,
        'f': 'json',
        'limit': 720,
    }
    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f'ERROR fetching {station_id}: {e}')
        return None


def flatten_to_csv(all_data, filename):
    rows = []

    for station_id, station_data in all_data['stations'].items():
        for feature in station_data.get('features', []):
            props = feature.get('properties', {})
            geom = feature.get('geometry', {}).get('coordinates', [None, None])
            level = props.get('LEVEL')

            thresholds = {
                '01AO001': (5.5, 6.0, 6.4),
                '01AJ010': (4.0, 4.8, 5.5),
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
                'station_id': station_id,
                'station_name': props.get('STATION_NAME', ''),
                'timestamp': props.get('DATETIME', ''),
                'water_level_m': level,
                'discharge_cms': props.get('DISCHARGE'),
                'longitude': geom[0] if geom else None,
                'latitude': geom[1] if geom else None,
                'flood_status': status,
                'fetched_at': all_data['fetched_at']
            })

    os.makedirs('data/clean', exist_ok=True)

    # ✅ FIX: handle empty data safely
    if not rows:
        print("No data returned — writing empty CSV with headers.")
        fieldnames = [
            'station_id', 'station_name', 'timestamp',
            'water_level_m', 'discharge_cms',
            'longitude', 'latitude',
            'flood_status', 'fetched_at'
        ]
    else:
        fieldnames = rows[0].keys()

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)

    print(f'Clean CSV saved: {filename} ({len(rows)} rows)')


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

        if data and data.get("features"):
            all_data['stations'][station] = data
            print(f'  Got {len(data.get("features", []))} records')
        else:
            print(f'  No data for {station}')

    print(f"Stations with data: {len(all_data['stations'])}/{len(STATIONS)}")

    # ✅ Optional: fail job if nothing worked
    if not all_data['stations']:
        raise Exception("No station data fetched — failing job.")

    # Save JSON
    filename = f'data/raw/water_{timestamp}.json'
    with open(filename, 'w') as f:
        json.dump(all_data, f, indent=2)

    with open('data/raw/latest.json', 'w') as f:
        json.dump(all_data, f, indent=2)

    print(f'Saved JSON: {filename}')

    # Save CSVs
    flatten_to_csv(all_data, 'data/clean/latest_clean.csv')
    flatten_to_csv(all_data, f'data/clean/water_clean_{timestamp}.csv')


if __name__ == '__main__':
    main()
