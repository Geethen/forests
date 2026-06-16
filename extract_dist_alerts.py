#!/usr/bin/env python
"""
Extract UMD/GLAD DIST-ALERT vegetation-disturbance alerts for *every alert pixel*
inside each polygon of the olivinskog / kalklindeskog GeoPackage, via the Global
Forest Watch (GFW) Data API.

DIST-ALERT (`umd_glad_dist_alerts`, ~30 m, refreshed every few days, coverage from
late 2023) flags generic vegetation disturbance (logging, dieback, ...). For each
polygon we query the GFW Data API over a date range and keep one row per alert
pixel: lon/lat, alert date, confidence and intensity. The result is a long-format
table mirroring `alphaearth_pixels.parquet`, so the two join on (layer, _fid):

  * see *where and when* a stand was recently disturbed (near-real-time signal to
    complement the slow annual AlphaEarth embedding change);
  * filter by date / confidence for prioritising field checks.

Sibling of `extract_alphaearth_embeddings.py`; reuses its `retry`, `load_features`
and schema-evolving `write_df`, plus the DuckDB-buffer -> ZSTD-Parquet export.

Durability (single source of truth = the DuckDB buffer):
  * the persistent `.duckdb` survives crashes; it is deleted only on a clean final
    export. No separate checkpoint.json (which can drift from the data on a kill).
  * two tables: `data` (alert-pixel rows) and `done` (layer, _fid -- every finished
    polygon, including zero-alert ones).
  * per polygon, under the lock: write rows -> record in `done` -> CHECKPOINT (flush
    WAL). A polygon is only in `done` after its rows are committed, so a kill can at
    worst lose the *current* polygon (re-run), never a recorded one.
  * resume = skip any (layer, _fid) already in `done`.

Auth: needs a GFW Data API key (see get_gfw_apikey.py). The key is read from
$GFW_API_KEY or the gitignored cache data/.gfw_apikey.json, and sent as x-api-key.

Run with the `geo` pixi env:
  ~/.pixi/envs/geo/bin/python extract_dist_alerts.py --limit 1 --layers MI_olivinskog
"""

import os
import time
import json
import argparse
import datetime as dt
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import geopandas as gpd
import pandas as pd
from pyogrio import list_layers
from shapely.geometry import mapping
from tqdm.auto import tqdm
import duckdb

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
GFW_API_BASE = "https://data-api.globalforestwatch.org"
DATASET = "umd_glad_dist_alerts"
DEFAULT_VERSION = "v20260613"  # fallback if "latest" can't be resolved
EE_CRS = "EPSG:4326"  # GFW expects GeoJSON in lon/lat

# DIST-ALERT fields (verified against the live dataset schema).
F_DATE = f"{DATASET}__date"
F_CONF = f"{DATASET}__confidence"
F_INTENSITY = f"{DATASET}__intensity"
CONF_LEVELS = ["nominal", "high", "highest"]  # ascending strength

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APIKEY_CACHE = os.path.join(BASE_DIR, "data", ".gfw_apikey.json")

# A stable per-feature id column is chosen per layer in load_features().
ID_CANDIDATES = [
    "polygon_id",              # all_schemes parquets
    "identifikasjon_lokalId",  # MI / HB13
    "Kartleggin",              # NiN5k
    "objectid",                # naturtiltak
]
# Attributes carried through (only those present are kept).
ATTR_CANDIDATES = [
    "forest_type", "source", "navn", "naturtype", "tilstand",  # all_schemes parquets
    "områdenavn", "omradenavn", "omraadenavn", "Område5ki",
    "naturtype_navn", "naturtypeKode", "Kartlegg_1",
    "year", "kommune", "kommuner", "Kommuner", "Fylker",
    "hovedformaal", "tiltakstype",
]


# ----------------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------------
def retry(max_retries=4, backoff_factor=2):
    """Retry a call with exponential backoff."""
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


def load_apikey(cache_path=APIKEY_CACHE):
    """Return the GFW API key from $GFW_API_KEY or the gitignored cache."""
    key = os.environ.get("GFW_API_KEY")
    if key:
        return key
    if os.path.exists(cache_path):
        try:
            with open(cache_path) as f:
                key = json.load(f).get("api_key")
            if key:
                return key
        except (json.JSONDecodeError, OSError):
            pass
    raise SystemExit(
        "[ERROR] No GFW API key. Set $GFW_API_KEY or run get_gfw_apikey.py first "
        f"(expected cache at {cache_path})."
    )


def resolve_latest_version(session):
    """Pick the current vYYYYMMDD for the dataset, falling back to DEFAULT_VERSION."""
    try:
        resp = session.get(f"{GFW_API_BASE}/dataset/{DATASET}/latest", timeout=60)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        version = data.get("version")
        if version:
            return version
    except Exception:  # noqa: BLE001
        pass
    try:
        resp = session.get(f"{GFW_API_BASE}/dataset/{DATASET}", timeout=60)
        resp.raise_for_status()
        versions = resp.json().get("data", {}).get("versions", [])
        dated = sorted(v for v in versions if v.startswith("v"))
        if dated:
            return dated[-1]
    except Exception:  # noqa: BLE001
        pass
    print(f"[warn] could not resolve latest version; using {DEFAULT_VERSION}")
    return DEFAULT_VERSION


def build_sql(start_date, end_date, min_confidence):
    """SQL for the GFW query endpoint, over the requested date range.

    The GFW query engine rejects the `IN` operator (422 Unsupported filter
    operator), so the confidence filter is expressed as OR-ed equalities over
    the allowed (>= min_confidence) levels.
    """
    where = [f"{F_DATE} >= '{start_date}'", f"{F_DATE} <= '{end_date}'"]
    if min_confidence and min_confidence != "nominal":
        allowed = CONF_LEVELS[CONF_LEVELS.index(min_confidence):]
        ors = " OR ".join(f"{F_CONF} = '{c}'" for c in allowed)
        where.append(f"({ors})")
    cols = f"latitude, longitude, {F_DATE}, {F_CONF}, {F_INTENSITY}"
    return f"SELECT {cols} FROM results WHERE " + " AND ".join(where)


# ----------------------------------------------------------------------------
# Feature loading  (identical to extract_alphaearth_embeddings.py)
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


def load_features_parquet(parquet_path):
    """Read an all_schemes parquet, reproject to EPSG:4326, assign _fid.

    Returns (GeoDataFrame[_fid, *attrs, geometry], attrs list, layer_name).
    """
    gdf = gpd.read_parquet(parquet_path)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)
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
    layer_name = os.path.splitext(os.path.basename(parquet_path))[0]
    return gdf[["_fid"] + attrs + ["geometry"]], attrs, layer_name


# ----------------------------------------------------------------------------
# GFW query
# ----------------------------------------------------------------------------
@retry(max_retries=4, backoff_factor=2)
def query_polygon(geom_geojson, sql, version, apikey, session):
    """POST one polygon query to the GFW Data API. Returns the `data` list.

    Retryable on 429 / 5xx (raised -> backoff); 4xx (bad SQL/geometry) surfaces
    immediately as a non-retried RuntimeError.
    """
    url = f"{GFW_API_BASE}/dataset/{DATASET}/{version}/query/json"
    resp = session.post(
        url,
        json={"sql": sql, "geometry": geom_geojson},
        headers={"x-api-key": apikey},
        timeout=180,
    )
    if resp.status_code == 429 or resp.status_code >= 500:
        resp.raise_for_status()  # retryable
    if resp.status_code >= 400:
        # client error: don't retry, fail loudly with the server message.
        raise RuntimeError(
            f"GFW query {resp.status_code}: {resp.text[:300]}"
        ) from None
    return resp.json().get("data", [])


# ----------------------------------------------------------------------------
# DuckDB buffer  (write_df identical to extract_alphaearth_embeddings.py)
# ----------------------------------------------------------------------------
def write_df(df, db_conn, lock):
    """Append df to the `data` table, evolving the table schema as needed.

    Different gpkg layers carry different attribute columns, so new columns can
    appear at any point in the run. `INSERT ... BY NAME` matches by name and
    requires df's columns to exist in the table, so add any missing ones first.
    DuckDB column names are case-insensitive, so a case-only difference is the
    *same* column to it; rename df's column to the existing casing in that case.

    NOTE: the caller already holds `lock` (it also records `done` + CHECKPOINT in
    the same critical section), so this does not re-acquire it.
    """
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


def load_done(db_conn):
    """Set of (layer, _fid) already finished, derived from the buffer."""
    db_conn.execute(
        "CREATE TABLE IF NOT EXISTS done (layer VARCHAR, _fid VARCHAR)"
    )
    rows = db_conn.execute("SELECT layer, _fid FROM done").fetchall()
    return {(r[0], r[1]) for r in rows}


# ----------------------------------------------------------------------------
# Extraction
# ----------------------------------------------------------------------------
def process_polygon(layer, fid, geom_geojson, attrs_row, sql, version,
                    apikey, session, db_conn, lock):
    """Query one polygon, persist its alert rows, record it done. Returns row count."""
    data = query_polygon(geom_geojson, sql, version, apikey, session)

    df = None
    if data:
        df = pd.DataFrame(data).rename(columns={
            "latitude": "lat",
            "longitude": "lon",
            F_DATE: "alert_date",
            F_CONF: "confidence",
            F_INTENSITY: "intensity",
        })
        for k, v in attrs_row.items():
            # carry polygon attributes; rename a gpkg 'year' attr so it is clearly
            # the survey year (there is no image-year dimension here).
            df["survey_year" if k == "year" else k] = v
        df["dist_version"] = version
        df.insert(0, "_fid", fid)
        df.insert(0, "layer", layer)

    # Single critical section: write rows -> record done -> flush. Recording in
    # `done` only after the rows are committed is what makes resume crash-safe.
    with lock:
        if df is not None and not df.empty:
            write_df(df, db_conn, lock)
        db_conn.execute("INSERT INTO done VALUES (?, ?)", [layer, fid])
        db_conn.execute("CHECKPOINT")
    return 0 if df is None else len(df)


def process_layer(gpkg_path, layer, sql, version, apikey, session,
                  db_conn, done, lock, poly_workers, keep_fids=None, gdf=None, attrs=None):
    """Process all (or a subset of) polygons in a layer not already in `done`."""
    if gdf is None:
        gdf, attrs = load_features(gpkg_path, layer)
    if keep_fids is not None:
        gdf = gdf[gdf["_fid"].isin(keep_fids)]
    if len(gdf) == 0:
        print(f"  [skip] {layer}: no valid geometries")
        return 0

    units = []
    for _, row in gdf.iterrows():
        fid = row["_fid"]
        if (layer, fid) in done:
            continue
        attrs_row = {a: row[a] for a in attrs}
        units.append((fid, mapping(row.geometry), attrs_row))
    if not units:
        print(f"  [done] {layer}: already complete ({len(gdf)} polys)")
        return 0

    total_rows = 0

    def run(unit):
        fid, geom_geojson, attrs_row = unit
        return process_polygon(layer, fid, geom_geojson, attrs_row, sql,
                               version, apikey, session, db_conn, lock)

    with ThreadPoolExecutor(max_workers=poly_workers) as ex:
        futures = {ex.submit(run, u): u for u in units}
        with tqdm(total=len(units), desc=layer, ncols=100, leave=True) as pbar:
            for fut in as_completed(futures):
                try:
                    total_rows += fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"  [ERROR] {layer}|{futures[fut][0]}: {str(e)[:160]}")
                pbar.update(1)
    return total_rows


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    default_data = os.path.join(
        os.path.dirname(BASE_DIR), "Data", "Olivinskog_kalklindeskog"
    )
    default_out = os.path.join(BASE_DIR, "data", "dist_alerts.parquet")
    default_parquets = [
        os.path.join(BASE_DIR, "data", "olivinskog_all_schemes.parquet"),
        os.path.join(BASE_DIR, "data", "kalklindeskog_all_schemes.parquet"),
    ]

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--parquets", nargs="*", default=default_parquets,
                    help="all_schemes .parquet files to use as input (default: both "
                         "olivinskog and kalklindeskog). Takes priority over --gpkgs.")
    ap.add_argument("--data_dir", default=default_data)
    ap.add_argument("--gpkgs", nargs="*", default=["skog_kartlegginger.gpkg"],
                    help="GeoPackage files (used only when --parquets is empty)")
    ap.add_argument("--output", default=default_out, help="Output .parquet path")
    ap.add_argument("--start_date", default="2023-12-01",
                    help="Earliest alert date (YYYY-MM-DD)")
    ap.add_argument("--end_date", default=dt.date.today().isoformat(),
                    help="Latest alert date (YYYY-MM-DD), default today")
    ap.add_argument("--min_confidence", default="nominal", choices=CONF_LEVELS,
                    help="Keep alerts at this confidence or stronger")
    ap.add_argument("--version", default=None,
                    help="Dataset version override (default: resolve latest)")
    ap.add_argument("--poly_workers", type=int, default=6,
                    help="Parallel polygon queries (keep modest for the REST API)")
    ap.add_argument("--layers", nargs="*", default=None,
                    help="Restrict to these layer names (applies to gpkg mode only)")
    ap.add_argument("--limit", type=int, default=None,
                    help="Test mode: only first N polygons per layer")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    apikey = load_apikey()
    session = requests.Session()
    version = args.version or resolve_latest_version(session)
    sql = build_sql(args.start_date, args.end_date, args.min_confidence)
    print(f"[OK] GFW {DATASET} {version}  ({args.start_date}..{args.end_date}, "
          f">= {args.min_confidence})")

    # targets: (path_or_None, layer_name, pre_loaded_gdf_or_None, pre_loaded_attrs_or_None)
    targets = []
    if args.parquets:
        for pq in args.parquets:
            if not os.path.exists(pq):
                print(f"[warn] missing parquet {pq}")
                continue
            gdf_pq, attrs_pq, lname = load_features_parquet(pq)
            targets.append((None, lname, gdf_pq, attrs_pq))
        print(f"[OK] {len(targets)} parquet source(s)")
    else:
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
                targets.append((path, lname, None, None))
        print(f"[OK] {len(targets)} polygon layer(s)")

    # DuckDB buffer is the single source of truth (see module docstring).
    db_path = args.output.replace(".parquet", ".duckdb")
    db_conn = duckdb.connect(db_path)
    try:
        n_existing = db_conn.execute("SELECT count(*) FROM data").fetchone()[0]
        print(f"  [info] resuming existing buffer ({n_existing:,} rows)")
    except duckdb.CatalogException:
        if os.path.exists(args.output):
            db_conn.execute(f"CREATE TABLE data AS SELECT * FROM '{args.output}'")
            print("  [info] loaded existing output into buffer")
    done = load_done(db_conn)
    # If we only had a parquet (clean prior run), `done` is empty; rebuild it from
    # the rows present so polygons that DID have alerts aren't re-queried. (Zero-
    # alert polygons leave no rows, so they get re-queried once -- cheap, harmless.)
    if not done:
        try:
            db_conn.execute(
                "INSERT INTO done SELECT DISTINCT layer, _fid FROM data"
            )
            done = load_done(db_conn)
            if done:
                print(f"  [info] rebuilt done-set from buffer ({len(done):,} polys)")
        except duckdb.CatalogException:
            pass
    lock = Lock()

    total_rows = 0
    t0 = time.time()
    for path, layer, gdf_pre, attrs_pre in targets:
        keep = None
        if args.limit:  # test mode: subset polygons before processing
            src_gdf = gdf_pre if gdf_pre is not None else load_features(path, layer)[0]
            keep = set(src_gdf["_fid"].head(args.limit))
        total_rows += process_layer(
            path, layer, sql, version, apikey, session,
            db_conn, done, lock, args.poly_workers, keep_fids=keep,
            gdf=gdf_pre, attrs=attrs_pre)

    print("\n[export] writing parquet...")
    tmp = args.output + ".tmp"
    try:
        db_conn.execute(
            f"COPY data TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        total = db_conn.execute("SELECT count(*) FROM data").fetchone()[0]
    except duckdb.CatalogException:
        total = 0  # no alerts at all -> no `data` table
        print("  [info] no alert rows extracted; nothing to write")
    db_conn.close()
    if total:
        if os.path.exists(args.output):
            os.remove(args.output)
        os.rename(tmp, args.output)
    if os.path.exists(db_path):
        os.remove(db_path)
    print(f"[OK] {total:,} alert-pixel rows in {args.output} "
          f"(+{total_rows:,} this run) in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
