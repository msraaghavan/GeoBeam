# GeoBeam

Distributed spatial query engine on Apache Beam, with a live Leaflet front-end over real **NYC TLC Yellow Taxi** trip data (Jan 2015).

**Live demo:** https://geobeam.pages.dev

![status](https://img.shields.io/badge/python-3.11-blue) ![beam](https://img.shields.io/badge/Apache%20Beam-DirectRunner-orange) ![flask](https://img.shields.io/badge/Flask-3.x-lightgrey)

---

## What it does

GeoBeam runs four canonical spatial operations as Beam pipelines and exposes them through a browser UI:

| Op | Pipeline | UI |
| --- | --- | --- |
| **Range** | bbox-pruned scan over partitioned cells | click two corners on the map |
| **Spatial Join** | broadcast-join points against zone polygons | colours each pickup by NYC borough |
| **k-NN** | local top-k per cell, then global merge | click a query point on the map |
| **Counts** | partition counts (heat-map) | renders cell density as a colour ramp |

Three partitioners are pluggable from the dropdown:

- **Grid** — uniform 10×10 grid over the NYC bbox
- **Quadtree** — recursive subdivision to depth 4
- **Adaptive** — sample-driven boundaries (5 % reservoir) so dense areas get finer cells

## Architecture

```
┌─────────────────┐    /data/pickups.json   ┌─────────────────────────┐
│  Leaflet UI     │ ───────────────────────►│  Flask app (app.py)     │
│  templates/     │                         │  serves static + REST   │
│  index.html     │ ◄─── /api/{range,join,  │                         │
└────────┬────────┘      knn,stats}         └────────────┬────────────┘
         │                                               │
         │  client-side JS                               │  subprocess
         │  (Grid / Quadtree                             ▼
         │   / Adaptive partitioners,                ┌─────────────────────────┐
         │   point-in-polygon, k-NN)                 │ run_pipeline.py (Beam)  │
         │                                           │  • RangeQuery PTransform│
         │                                           │  • SpatialJoin          │
         │                                           │  • KNN (local→global)   │
         │                                           │  • CountPerCell         │
         │                                           └────────────┬────────────┘
         │                                                        │
         ▼                                                        ▼
   render results                                       data/output_<op>.tsv
```

The `geobeam/` package is the reusable core:

```
geobeam/
├── partitioner.py    # GridPartitioner, QuadtreePartitioner, make_partitioner()
├── sampler.py        # AdaptiveGridPartitioner + sample_input()
├── transforms.py     # AssignCell, RangeQuery, SpatialJoin, KNN PTransforms
├── combiners.py      # CountPerCell
├── index.py          # SpatialIndex, IndexManager
├── pcollection.py    # SpatialPCollection wrapper
├── pipeline.py       # SpatialPipeline orchestration
├── metrics.py        # CountedDoFn + collect_metrics
└── config.py         # GeoBeamConfig
```

## Data pipeline

The demo uses **real** NYC TLC Yellow Taxi data, not synthetic. Because TLC re-released pre-2017 trip files using `PULocationID` (taxi zones) instead of lat/lon, [`build_pickups.py`](build_pickups.py) joins the parquet against the official **TLC zone shapefile**, reprojects EPSG:2263 → WGS84, jitters each pickup inside its zone bbox, and emits a 10 000-row sample at `data/pickups.json`.

```bash
# One-time prep (downloads ~175 MB parquet + ~1 MB shapefile if missing)
python build_pickups.py
```

## Running locally

```bash
python -m venv venv
venv\Scripts\activate            # Windows
pip install -r requirements.txt
python app.py                    # Flask on http://localhost:5000
```

To run a Beam op directly:

```bash
python run_pipeline.py --op range --min_lon -74.02 --min_lat 40.70 \
  --max_lon -73.97 --max_lat 40.78 --partitioner quadtree
python run_pipeline.py --op knn   --lon -73.985 --lat 40.748 --k 10
python run_pipeline.py --op join  --partitioner adaptive
python run_pipeline.py --op stats --partitioner grid
```

Outputs land in `data/output_<op>.tsv`; throughput metrics print as `METRIC …` lines.

## Streaming

[`streaming_pipeline.py`](streaming_pipeline.py) consumes `data/taxi_stream.csv` with event-time timestamps and emits per-window counts — a single-file demonstration of windowed aggregation.

## REST API

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| GET  | `/`                  | —                                              | demo UI |
| GET  | `/data/pickups.json` | —                                              | sampled pickups |
| GET  | `/api/geofences`     | —                                              | borough polygons (TSV) |
| POST | `/api/range`         | `{bbox:[lon,lat,lon,lat], partitioner}`        | matching trips |
| POST | `/api/join`          | `{partitioner}`                                | pickups labelled by zone |
| POST | `/api/knn`           | `{lon, lat, k, partitioner}`                   | k nearest pickups |
| POST | `/api/stats`         | `{partitioner}`                                | per-cell counts |

Responses are plain TSV with a `MODE / COUNT / METRICS` header so they're greppable.

## Static deploy

Because the front-end only fetches `/data/pickups.json`, the demo deploys as a 100 % static site to Cloudflare Pages / GitHub Pages — no backend required. See [Cloudflare Pages](https://geobeam.pages.dev) for the live build.

## Repo layout

```
.
├── app.py                  Flask server + REST routes
├── build_pickups.py        TLC parquet → JSON sample
├── run_pipeline.py         Batch CLI for the four ops
├── streaming_pipeline.py   Windowed streaming demo
├── geobeam/                Reusable spatial-Beam library
├── templates/index.html    Leaflet UI (no Jinja, deployable as static)
├── data/
│   ├── geofences.geojson   borough polygons
│   ├── pickups.json        10 k real pickups (built artefact)
│   └── output_*.tsv        Beam pipeline outputs
└── ...
```

## License

MIT
