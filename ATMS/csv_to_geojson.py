#!/usr/bin/env python3
"""
Convert CSV with full postal address into GeoJSON Points.
Uses free Nominatim (OpenStreetMap) geocoder – 1 request / s to be nice.
"""

import pandas as pd
import geopandas as gpd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time, sys

# ---------- CONFIG – change to match your file ----------
CSV_FILE    = 'atms_raw.csv'   # your raw CSV export
ENCODING    = 'utf-8'          # or 'latin1' / 'cp1252'
DELIMITER   = ','              # or '\t' for TSV

# columns that form the postal address (must exist in CSV)
ADDRESS_COLS = ['Address', 'City', 'State/Province', 'Postal Code', 'Country']
# --------------------------------------------------------

geocoder = Nominatim(user_agent='atm_geo_script')
geocode  = RateLimiter(geocoder.geocode, min_delay_seconds=1)

def main():
    print('Reading CSV …')
    df = pd.read_csv(CSV_FILE, delimiter=DELIMITER, encoding=ENCODING)

    # build full address string – force strings
    # make a tidy address line
    df['full_address'] = (
        df['Address'].astype(str).str.strip() + ', ' +
        df['City'].astype(str).str.strip() + ', ' +
        df['State/Province'].astype(str).str.strip() + ', ' +
        df['Country'].astype(str).str.strip()
    ).str.replace(r'[, ]+', ' ', regex=True)

    print('Geocoding (this will take a while for 7 k rows) …')
    coords = []
    for addr in df['full_address']:
        if pd.isna(addr) or addr.strip() == '':
            coords.append((None, None)); continue          # always a 2-tuple
        try:
            loc = geocode(addr, timeout=15)                # 15 s per call
            c   = (loc.latitude, loc.longitude) if loc else (None, None)
        except Exception as e:                              # any geopy error
            print('  geocode error:', e); c = (None, None)
        coords.append(c)
        print(f'{addr[:50]:<50} → {c}')
        time.sleep(1.2)        # be extra gentle

    df[['latitude', 'longitude']] = pd.DataFrame(coords, columns=['lat', 'lon'])

    # drop failures
    df = df.dropna(subset=['latitude', 'longitude'])
    print(f'\nSuccessfully geocoded {len(df)} / {len(coords)} rows')

    # rename standard address fields for the map (lower-case aliases)
    rename_map = {
        'Address'       : 'address',
        'City'          : 'city',
        'State/Province': 'province',
        'Postal Code'   : 'postalcode',
        'Country'       : 'country'
    }
    # use lower-case aliases where they exist; keep everything else
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # build GeoDataFrame – keep every original column as properties
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs='EPSG:4326'
    )

    gdf.to_file('atms.geojson', driver='GeoJSON')
    print(f'Saved → atms.geojson  ({len(gdf)} points)')

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit('\nAborted by user')