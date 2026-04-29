"""Build data/pickups.json from yellow_tripdata parquet (zone IDs) + TLC zone shapefile."""
import json
import math
import os
import random

import shapefile  # pyshp
from pyproj import Transformer
import pyarrow.parquet as pq

DATA = os.path.join(os.path.dirname(__file__), 'data')
SHP = os.path.join(DATA, 'taxi_zones', 'taxi_zones', 'taxi_zones.shp')
PARQUET = os.path.join(DATA, 'yellow_tripdata_2015-01.parquet')
OUT = os.path.join(DATA, 'pickups.json')

NYC = dict(min_lon=-74.26, max_lon=-73.70, min_lat=40.49, max_lat=40.92)
SAMPLE = 10000
SEED = 42

print('Reading shapefile...')
sf = shapefile.Reader(SHP)
fields = [f[0] for f in sf.fields[1:]]
loc_idx = fields.index('LocationID')
transformer = Transformer.from_crs('EPSG:2263', 'EPSG:4326', always_xy=True)

zones = {}  # LocationID -> (cx, cy, half_w, half_h) in lon/lat
for rec, shp in zip(sf.records(), sf.shapes()):
    pts = shp.points
    if not pts:
        continue
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    cx_ft = sum(xs) / len(xs)
    cy_ft = sum(ys) / len(ys)
    minx_ft, maxx_ft = min(xs), max(xs)
    miny_ft, maxy_ft = min(ys), max(ys)
    cx, cy = transformer.transform(cx_ft, cy_ft)
    minx, miny = transformer.transform(minx_ft, miny_ft)
    maxx, maxy = transformer.transform(maxx_ft, maxy_ft)
    zones[rec[loc_idx]] = (cx, cy, (maxx - minx) * 0.35, (maxy - miny) * 0.35)

print(f'  {len(zones)} zones loaded')

print('Reading parquet...')
tbl = pq.read_table(PARQUET, columns=['PULocationID', 'fare_amount'])
pu = tbl.column('PULocationID').to_pylist()
fare = tbl.column('fare_amount').to_pylist()
print(f'  {len(pu)} rows')

rng = random.Random(SEED)
pre_indices = list(range(len(pu)))
rng.shuffle(pre_indices)

points = []
for idx in pre_indices:
    if len(points) >= SAMPLE:
        break
    z = zones.get(pu[idx])
    if not z:
        continue
    f = fare[idx]
    if f is None or f < 2.5 or f > 200:
        continue
    cx, cy, hw, hh = z
    # jitter inside zone bbox so points don't all stack on centroid
    lon = cx + (rng.random() - 0.5) * 2 * hw
    lat = cy + (rng.random() - 0.5) * 2 * hh
    if not (NYC['min_lon'] <= lon <= NYC['max_lon'] and NYC['min_lat'] <= lat <= NYC['max_lat']):
        continue
    points.append({'lon': round(lon, 6), 'lat': round(lat, 6), 'fare': round(f, 2)})

print(f'  sampled {len(points)} points')

with open(OUT, 'w') as f:
    json.dump(points, f, separators=(',', ':'))
print(f'Wrote {OUT} ({os.path.getsize(OUT)//1024} KB)')
