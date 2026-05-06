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

RAW_DIR = 'data/raw'
CLEAN_DIR = 'data/clean'


# ------------------------------------------------------------
# Fetch data
# ------------------------------------------------------------
def fetch_station_data(station_id):
    params = {
        'STATION_NUMBER': station_id,
        'f': 'json',
        'limit': 720,
    }

    try:
        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        features = data.get("features", [])
        print(f"{station_id}: {len(features)} records")

        return data if features else None

    except Exception as e:
        print(f"ERROR fetching {station_id}: {e}")
        return None


# ------------------------------------------------------------
# Flood classification
# ------------------------------------------------------------
def classify_level(station_id, level):
    thresholds = {
        '01AO001': (5.5, 6.0, 6.4),
        '01AJ010': (4.0, 4.8, 5.5),
    }.get(station_id, (4.0, 5.0, 6.0))

    if level is None:
        return 'UNKNOWN'
    elif level >= thresholds[2]:
        return 'FLOOD'
    elif level >= thresholds[1]:
        return 'WARNING'
    elif level >= thresholds[0]:
        return 'WATCH'
    return 'NORMAL'


# ------------------------------------------------------------
# Transform to rows
# ------------------------------------------------------------
def build_rows(all_data):
    rows = []

    for station_id, station_data in all_data['stations'].items():
        for feature in station_data.get('features', []):
            props = feature.get('properties', {})
            coords = feature.get('geometry', {}).get('coordinates', [None, None])

            level = props.get('LEVEL')

            rows.append({
                'station_id': station_id,
                'station_name': props.get('STATION_NAME', ''),
                'timestamp': props.get('DATETIME', ''),
                'water_level_m': level,
                'discharge_cms': props.get('DISCHARGE'),
                'longitude': coords[0] if coords else None,
                'latitude': coords[1] if coords else None,
                'flood_status': classify_level(station_id, level),
                'fetched_at': all_data['fetched_at']
            })

    return rows


# ------------------------------------------------------------
# Write CSV
# ------------------------------------------------------------
def write_csv(rows, filepath):
    fieldnames = [
        'station_id',
        'station_name',
        'timestamp',
        'water_level_m',
        'discharge_cms',
        'longitude',
        'latitude',
        'flood_status',
        'fetched_at'
    ]

    with open(filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)

    print(f"CSV written: {filepath} ({len(rows)} rows)")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H%M%SZ')

    # Ensure directories exist
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(CLEAN_DIR, exist_ok=True)

    all_data = {
        'fetched_at': timestamp,
        'stations': {}
    }

    print("---- FETCHING DATA ----")

    for station in STATIONS:
        data = fetch_station_data(station)
        if data:
            all_data['stations'][station] = data

    print(f"Stations with data: {len(all_data['stations'])}/{len(STATIONS)}")

    if not all_data['stations']:
        raise RuntimeError("No station data fetched — aborting.")

    # --------------------------------------------------------
    # Save JSON
    # --------------------------------------------------------
    raw_file = f"{RAW_DIR}/water_{timestamp}.json"
    latest_file = f"{RAW_DIR}/latest.json"

    with open(raw_file, 'w') as f:
        json.dump(all_data, f, indent=2)

    with open(latest_file, 'w') as f:
        json.dump(all_data, f, indent=2)

    print(f"JSON written: {raw_file}")

    # --------------------------------------------------------
    # Build and save CSV
    # --------------------------------------------------------
    rows = build_rows(all_data)

    write_csv(rows, f"{CLEAN_DIR}/water_clean_{timestamp}.csv")
    write_csv(rows, f"{CLEAN_DIR}/latest_clean.csv")

    # Optional: also write CSV to raw for easier commits
    write_csv(rows, f"{RAW_DIR}/latest_clean.csv")

    print("---- DONE ----")


if __name__ == '__main__':
    main()
