# GeoBeam

A distributed spatial query engine built on Apache Beam, with a browser front-end that visualises four canonical spatial operations over real **NYC TLC Yellow Taxi** trip records (January 2015, 12.7 M rows sampled to 10 000 pickups).

**Live:** https://geobeam.ms-raaghavan.workers.dev

---

## Overview

GeoBeam implements range queries, spatial joins, k-nearest neighbours, and partition counts as composable Beam `PTransform`s, with three pluggable partitioners (uniform grid, quadtree, sample-driven adaptive). The same operations are mirrored client-side in JavaScript so the demo can be served as a 100 % static site and explored interactively from any device.

## Operations

| Operation | Pipeline strategy | UI gesture |
| --- | --- | --- |
| **Range query** | bbox-pruned scan over partitioned cells | click two corners on the map |
| **Spatial join** | broadcast-join points against zone polygons | colours each pickup by NYC borough |
| **k-NN** | local top-k per cell, then global merge | click a query point on the map |
| **Partition counts** | per-cell aggregation rendered as a heat-map | runs against the active partitioner |

## Partitioners

- **Grid** — uniform 10 × 10 grid over the NYC bounding box.
- **Quadtree** — recursive subdivision to depth 4; cells split on point density.
- **Adaptive** — sample-driven (5 % reservoir) boundaries, so dense areas receive finer cells than sparse ones.

## Architecture

```
        Leaflet UI                       Flask + Beam back-end
   ┌──────────────────────┐         ┌────────────────────────────────┐
   │ templates/index.html │ ──GET──►│ /data/pickups.json             │
   │  • partitioners      │         │ /api/{range,join,knn,stats}    │
   │  • point-in-polygon  │ ◄─POST──│  → spawns run_pipeline.py      │
   │  • local k-NN        │         └────────────────┬───────────────┘
   └──────────────────────┘                          │
                                                     ▼
                                          ┌─────────────────────────┐
                                          │ run_pipeline.py (Beam)  │
                                          │  RangeQuery PTransform  │
                                          │  SpatialJoin            │
                                          │  KNN  (local → global)  │
                                          │  CountPerCell           │
                                          └────────────┬────────────┘
                                                       ▼
                                            data/output_<op>.tsv
```

The reusable engine lives in `geobeam/`:

```
geobeam/
├── partitioner.py    GridPartitioner, QuadtreePartitioner, make_partitioner()
├── sampler.py        AdaptiveGridPartitioner, sample_input()
├── transforms.py     AssignCell, RangeQuery, SpatialJoin, KNN
├── combiners.py      CountPerCell
├── index.py          SpatialIndex, IndexManager
├── pcollection.py    SpatialPCollection wrapper
├── pipeline.py       SpatialPipeline orchestration
├── metrics.py        CountedDoFn, collect_metrics()
└── config.py         GeoBeamConfig
```

## Data preparation

NYC TLC re-released pre-2017 trip files using `PULocationID` (taxi-zone identifiers) instead of latitude / longitude. To recover point geometry, [`build_pickups.py`](build_pickups.py):

1. reads the parquet (`pyarrow`),
2. joins each `PULocationID` against the official TLC zone shapefile (`pyshp`),
3. reprojects EPSG:2263 → WGS 84 (`pyproj`),
4. jitters each pickup inside its zone bounding box, and
5. emits a 10 000-row `data/pickups.json` (≈ 460 KB) used by both back-end and front-end.

```bash
python build_pickups.py
```

## Running locally

```bash
python -m venv venv
venv\Scripts\activate            # Windows
pip install -r requirements.txt
python build_pickups.py          # one-time data prep
python app.py                    # http://localhost:5000
```

Invoke a Beam operation directly:

```bash
python run_pipeline.py --op range --min_lon -74.02 --min_lat 40.70 \
                       --max_lon -73.97 --max_lat 40.78 --partitioner quadtree
python run_pipeline.py --op knn   --lon -73.985 --lat 40.748 --k 10
python run_pipeline.py --op join  --partitioner adaptive
python run_pipeline.py --op stats --partitioner grid
```

Outputs are written to `data/output_<op>.tsv`; throughput counters are printed as `METRIC …` lines.

## Streaming

[`streaming_pipeline.py`](streaming_pipeline.py) is a single-file demonstration of windowed aggregation: it consumes `data/taxi_stream.csv` with event-time timestamps and emits per-window counts.

## REST API

| Method | Path                  | Body                                              | Returns                         |
| ------ | --------------------- | ------------------------------------------------- | ------------------------------- |
| GET    | `/`                   | —                                                 | demo UI                         |
| GET    | `/data/pickups.json`  | —                                                 | sampled pickups                 |
| GET    | `/api/geofences`      | —                                                 | borough polygons (TSV)          |
| POST   | `/api/range`          | `{ bbox: [lon,lat,lon,lat], partitioner }`        | matching trips                  |
| POST   | `/api/join`           | `{ partitioner }`                                 | pickups labelled by zone        |
| POST   | `/api/knn`            | `{ lon, lat, k, partitioner }`                    | k nearest pickups               |
| POST   | `/api/stats`          | `{ partitioner }`                                 | per-cell counts                 |

Responses are plain TSV with a `MODE / COUNT / METRICS` header so they remain greppable.

## Static deployment

Because the front-end's only network dependency is `/data/pickups.json`, the entire UI deploys as a 100 % static site behind a global CDN — no server, no cold starts, no credentials.

## Repository layout

```
.
├── app.py                  Flask server and REST routes
├── build_pickups.py        TLC parquet → pickups.json
├── run_pipeline.py         Batch CLI for the four operations
├── streaming_pipeline.py   Windowed streaming demo
├── geobeam/                Reusable spatial-Beam library
├── templates/index.html    Leaflet UI (no Jinja, deployable as static)
├── data/
│   ├── geofences.geojson   borough polygons
│   ├── pickups.json        10 k real pickups (built artefact)
│   └── output_*.tsv        Beam pipeline outputs
└── requirements.txt
```

## Tech stack

Apache Beam · Python 3.11 · Flask · `pyarrow` · `pyshp` · `pyproj` · Leaflet · OpenStreetMap

## License

MIT
