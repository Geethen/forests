#!/usr/bin/env python
"""
Extract Google AlphaEarth (Satellite Embedding V1 Annual) embeddings for *every
10 m pixel* inside each polygon of the olivinskog / kalklindeskog GeoPackages.

For each polygon, for each available year (2017-2025), every pixel that falls
inside the polygon is sampled, returning its 64 embedding bands (A00..A63) and
its lon/lat. The result is a long-format table: one row per
(layer, feature_id, year, pixel) with the 64 embedding values + lon/lat.

AlphaEarth embeddings are unit-normalised 64-d vectors designed to be compared
with dot products / cosine similarity. Per-pixel, per-year extraction lets you:
  * detect deforestation / degradation as year-to-year embedding change per pixel
    (cosine / euclidean distance), and map *where* inside a stand it happens;
  * quantify changed-area as the count of changed pixels x 100 m^2;
  * flag spruce encroachment by similarity of pixel embeddings to a spruce
    reference embedding.

Fast / reliable GEE patterns (after RECOVER/abandoned_ag_extract.py):
  * high-volume endpoint
  * ee.data.computeFeatures(fileFormat='PANDAS_DATAFRAME') instead of getInfo()
  * image.sample() per (polygon, year), sharded with randomColumn to keep each
    request payload small (avoids toList O(N^2) and >5 MB responses)
  * threaded shards with @retry exponential backoff
  * resumable CheckpointManager keyed on (layer, fid, year)
  * DuckDB buffer -> ZSTD Parquet output

Run with the `geo` pixi env:
  ~/.pixi/envs/geo/bin/python extract_alphaearth_embeddings.py --project ee-gsingh
"""

import os
import time
import json
import argparse
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import ee
import geopandas as gpd
from pyogrio import list_layers
from shapely.geometry import mapping
from tqdm.auto import tqdm
import duckdb

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
EMBED_COLLECTION = "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL"
EMBED_SCALE = 10  # native resolution of AlphaEarth (m)
BAND_NAMES = [f"A{i:02d}" for i in range(64)]
EE_CRS = "EPSG:4326"  # EE samples in lon/lat

# A stable per-feature id column is chosen per layer in load_features().
ID_CANDIDATES = [
    "identifikasjon_lokalId",  # MI / HB13
    "Kartleggin",              # NiN5k
    "objectid",                # naturtiltak
]
# Attributes carried through (joined locally; only those present are kept).
ATTR_CANDIDATES = [
    "områdenavn", "omradenavn", "omraadenavn", "Område5ki",
    "naturtype", "naturtype_navn", "naturtypeKode", "Kartlegg_1",
    "tilstand", "year", "kommune", "kommuner", "Kommuner", "Fylker",
    "hovedformaal", "tiltakstype",
]


# ----------------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------------
def retry(max_retries=4, backoff_factor=2):
    """Retry a GEE call with exponential backoff."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # noqa: BLE001
                    retries += 1
                    if retries >= max_retries:
                        raise
                    time.sleep(backoff_factor ** retries)
        return wrapper
    return decorator


class CheckpointManager:
    """JSON-backed set of completed (layer|fid|year) units, for resumable runs."""
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.done = self._load()
        self.lock = Lock()

    def _load(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file) as f:
                    return set(json.load(f))
            except json.JSONDecodeError:
                return set()
        return set()

    def is_done(self, key):
        return key in self.done

    def mark(self, key):
        with self.lock:
            self.done.add(key)
            with open(self.checkpoint_file, "w") as f:
                json.dump(list(self.done), f)


# ----------------------------------------------------------------------------
# Feature loading
# ----------------------------------------------------------------------------
def load_features(gpkg_path, layer):
    """Read a layer, reproject to EPSG:4326, pick an id column and attributes.

    Returns (GeoDataFrame[_fid, *attrs, geometry], attrs list). _fid is unique
    within the layer.
    """
    gdf = gpd.read_file(gpkg_path, layer=layer)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)  # fix slightly-invalid polygons
    gdf = gdf.to_crs(EE_CRS)

    id_col = next((c for c in ID_CANDIDATES if c in gdf.columns), None)
    if id_col is None:
        gdf["_fid"] = [str(i) for i in range(len(gdf))]
    else:
        fid = gdf[id_col].astype(str)
        if fid.duplicated().any():
            fid = fid + "_" + gdf.groupby(id_col).cumcount().astype(str)
        gdf["_fid"] = fid.values

    attrs = [c for c in ATTR_CANDIDATES if c in gdf.columns]
    return gdf[["_fid"] + attrs + ["geometry"]], attrs


# ----------------------------------------------------------------------------
# Extraction
# ----------------------------------------------------------------------------
def year_embedding(year):
    """Annual AlphaEarth mosaic for `year`, 64 bands selected."""
    start = ee.Date.fromYMD(year, 1, 1)
    end = ee.Date.fromYMD(year + 1, 1, 1)
    return (
        ee.ImageCollection(EMBED_COLLECTION)
        .filterDate(start, end)
        .mosaic()
        .select(BAND_NAMES)
    )


@retry(max_retries=4, backoff_factor=2)
def fetch_shard(shard_fc):
    """computeFeatures on one shard -> pandas DataFrame (or empty)."""
    return ee.data.computeFeatures(
        {"expression": shard_fc, "fileFormat": "PANDAS_DATAFRAME"}
    )


def sample_polygon_year(geom, image, n_shards):
    """Build the list of sharded sample FeatureCollections for one polygon/year.

    Pixels are sampled at native scale with lon/lat geometry, then split into
    `n_shards` random shards so each computeFeatures payload stays small.
    """
    samp = image.sample(
        region=geom,
        scale=EMBED_SCALE,
        projection=EE_CRS,
        geometries=True,
        tileScale=4,
        dropNulls=True,
    )
    # lon/lat as explicit columns, drop server geometry to shrink payload
    samp = samp.map(
        lambda f: f.set(
            "lon", f.geometry().coordinates().get(0),
            "lat", f.geometry().coordinates().get(1),
        ).setGeometry(None)
    )
    samp = samp.randomColumn("shard_rand", seed=42)
    shards, step = [], 1.0 / n_shards
    for k in range(n_shards):
        shards.append(
            samp.filter(ee.Filter.And(
                ee.Filter.gte("shard_rand", k * step),
                ee.Filter.lt("shard_rand", (k + 1) * step),
            ))
        )
    return shards


def write_df(df, db_conn, lock):
    """Append df to the buffer table, evolving the table schema as needed.

    Different gpkg layers carry different attribute columns (e.g. NiN5k_*
    uses 'Område5ki'/'Kommuner' while MI_* uses 'områdenavn'/'kommuner'), so
    new columns can appear at any point in the run. `INSERT ... BY NAME`
    matches by name and requires df's columns to exist in the table, so add
    any missing ones first. DuckDB column names are case-insensitive, so a
    case-only difference (e.g. 'Kommuner' vs 'kommuner') is the *same*
    column to it; rename df's column to the existing casing in that case.
    """
    with lock:
        try:
            existing = [c[0] for c in db_conn.execute("DESCRIBE data").fetchall()]
        except duckdb.CatalogException:
            db_conn.execute("CREATE TABLE data AS SELECT * FROM df")
            return

        existing_lower = {c.lower(): c for c in existing}
        rename = {}
        for col in df.columns:
            if col in existing:
                continue
            match = existing_lower.get(col.lower())
            if match:
                rename[col] = match  # same column to DuckDB, different case
            else:
                db_conn.execute(f'ALTER TABLE data ADD COLUMN "{col}" VARCHAR')
                existing_lower[col.lower()] = col
        if rename:
            df = df.rename(columns=rename)

        db_conn.execute("INSERT INTO data BY NAME SELECT * FROM df")


def process_unit(layer, fid, geom, year, attrs_row, n_shards,
                 shard_workers, db_conn, lock):
    """Extract all pixels for one (polygon, year). Returns rows written."""
    image = year_embedding(year)
    shards = sample_polygon_year(geom, image, n_shards)

    pending = shards
    rows = 0
    for attempt in range(4):
        if not pending:
            break
        if attempt:
            time.sleep(2 ** attempt)
        failed = []
        with ThreadPoolExecutor(max_workers=shard_workers) as ex:
            fut = {ex.submit(fetch_shard, s): s for s in pending}
            for f in as_completed(fut):
                try:
                    df = f.result()
                except Exception:  # noqa: BLE001
                    failed.append(fut[f])
                    continue
                if df is None or df.empty:
                    continue
                for k, v in attrs_row.items():
                    # carry attributes; rename a gpkg 'year' attribute so it
                    # never collides with the image year key below.
                    df["survey_year" if k == "year" else k] = v
                df.insert(0, "year", year)   # image (embedding) year key
                df.insert(0, "_fid", fid)
                df.insert(0, "layer", layer)
                drop = [c for c in ("shard_rand", "system:index", "geo")
                        if c in df.columns]
                if drop:
                    df = df.drop(columns=drop)
                write_df(df, db_conn, lock)
                rows += len(df)
        pending = failed
    if pending:
        raise RuntimeError(f"{len(pending)}/{n_shards} shards failed")
    return rows


def process_layer(gpkg_path, layer, years, db_conn, ckpt, lock,
                  n_shards, shard_workers, poly_workers):
    gdf, attrs = load_features(gpkg_path, layer)
    if len(gdf) == 0:
        print(f"  [skip] {layer}: no valid geometries")
        return 0

    # build (fid, geom, year, attrs) work units not yet checkpointed
    units = []
    for _, row in gdf.iterrows():
        fid = row["_fid"]
        attrs_row = {a: row[a] for a in attrs}
        geom = ee.Geometry(mapping(row.geometry), proj=EE_CRS, geodesic=False)
        for year in years:
            key = f"{layer}|{fid}|{year}"
            if not ckpt.is_done(key):
                units.append((key, fid, geom, year, attrs_row))
    if not units:
        print(f"  [done] {layer}: already complete ({len(gdf)} polys)")
        return 0

    total_rows = 0

    def run(unit):
        key, fid, geom, year, attrs_row = unit
        n = process_unit(layer, fid, geom, year, attrs_row,
                         n_shards, shard_workers, db_conn, lock)
        ckpt.mark(key)
        return n

    with ThreadPoolExecutor(max_workers=poly_workers) as ex:
        futures = {ex.submit(run, u): u for u in units}
        with tqdm(total=len(units), desc=layer, ncols=100, leave=True) as pbar:
            for fut in as_completed(futures):
                try:
                    total_rows += fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"  [ERROR] {futures[fut][0]}: {str(e)[:160]}")
                pbar.update(1)
    return total_rows


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(
        os.path.dirname(base_dir), "Data", "Olivinskog_kalklindeskog"
    )
    default_out = os.path.join(base_dir, "data", "alphaearth_pixels.parquet")

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--project", default="ee-gsingh", help="GEE project id")
    ap.add_argument("--data_dir", default=default_data)
    ap.add_argument("--gpkgs", nargs="*",
                    default=["skog_kartlegginger.gpkg", "skog_naturtiltak.gpkg"])
    ap.add_argument("--output", default=default_out, help="Output .parquet path")
    ap.add_argument("--start_year", type=int, default=2017)
    ap.add_argument("--end_year", type=int, default=2025)
    ap.add_argument("--n_shards", type=int, default=10,
                    help="Random shards per (polygon, year) request")
    ap.add_argument("--shard_workers", type=int, default=10,
                    help="Parallel shard fetches within a polygon/year")
    ap.add_argument("--poly_workers", type=int, default=6,
                    help="Parallel (polygon, year) units")
    ap.add_argument("--layers", nargs="*", default=None,
                    help="Restrict to these layer names")
    ap.add_argument("--limit", type=int, default=None,
                    help="Test mode: only first N polygons per layer")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    try:
        ee.Initialize(project=args.project,
                      opt_url="https://earthengine-highvolume.googleapis.com")
        print(f"[OK] GEE high-volume endpoint (project={args.project})")
    except Exception as e:  # noqa: BLE001
        print(f"[warn] high-volume init failed ({e}); using standard endpoint")
        ee.Initialize(project=args.project)

    years = list(range(args.start_year, args.end_year + 1))

    targets = []
    for fname in args.gpkgs:
        path = os.path.join(args.data_dir, fname)
        if not os.path.exists(path):
            print(f"[warn] missing {path}")
            continue
        for lname, gtype in list_layers(path):
            if "Polygon" not in gtype:
                continue
            if args.layers and lname not in args.layers:
                continue
            targets.append((path, lname))
    print(f"[OK] {len(targets)} polygon layer(s) x {len(years)} years "
          f"({years[0]}-{years[-1]})")

    db_path = args.output.replace(".parquet", ".duckdb")
    db_conn = duckdb.connect(db_path)
    try:
        n_existing = db_conn.execute("SELECT count(*) FROM data").fetchone()[0]
        print(f"  [info] resuming existing buffer ({n_existing:,} rows)")
    except duckdb.CatalogException:
        if os.path.exists(args.output):
            db_conn.execute(f"CREATE TABLE data AS SELECT * FROM '{args.output}'")
            print(f"  [info] loaded existing output into buffer")

    ckpt = CheckpointManager(args.output + ".checkpoint.json")
    lock = Lock()

    total_rows = 0
    t0 = time.time()
    for path, layer in targets:
        if args.limit:  # test mode: subset polygons before processing
            gdf, _ = load_features(path, layer)
            keep = set(gdf["_fid"].head(args.limit))
            total_rows += process_layer_subset(
                path, layer, years, db_conn, ckpt, lock,
                args.n_shards, args.shard_workers, args.poly_workers, keep)
        else:
            total_rows += process_layer(
                path, layer, years, db_conn, ckpt, lock,
                args.n_shards, args.shard_workers, args.poly_workers)

    print("\n[export] writing parquet...")
    tmp = args.output + ".tmp"
    db_conn.execute(f"COPY data TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    total = db_conn.execute("SELECT count(*) FROM data").fetchone()[0]
    db_conn.close()
    if os.path.exists(args.output):
        os.remove(args.output)
    os.rename(tmp, args.output)
    if os.path.exists(db_path):
        os.remove(db_path)
    print(f"[OK] {total:,} pixel-rows in {args.output} "
          f"(+{total_rows:,} this run) in {time.time()-t0:.0f}s")


def process_layer_subset(gpkg_path, layer, years, db_conn, ckpt, lock,
                         n_shards, shard_workers, poly_workers, keep_fids):
    """Like process_layer but only for _fid in keep_fids (test mode)."""
    gdf, attrs = load_features(gpkg_path, layer)
    gdf = gdf[gdf["_fid"].isin(keep_fids)]
    units = []
    for _, row in gdf.iterrows():
        fid = row["_fid"]
        attrs_row = {a: row[a] for a in attrs}
        geom = ee.Geometry(mapping(row.geometry), proj=EE_CRS, geodesic=False)
        for year in years:
            key = f"{layer}|{fid}|{year}"
            if not ckpt.is_done(key):
                units.append((key, fid, geom, year, attrs_row))
    total = 0
    for key, fid, geom, year, attrs_row in tqdm(units, desc=f"{layer}(test)",
                                                ncols=100):
        total += process_unit(layer, fid, geom, year, attrs_row,
                              n_shards, shard_workers, db_conn, lock)
        ckpt.mark(key)
    return total


if __name__ == "__main__":
    main()
