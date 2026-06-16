#!/usr/bin/env python
"""
Extract NASA OPERA L3 DIST-ALERT-HLS vegetation-disturbance values for *every
disturbed 30 m pixel* inside each polygon of the olivinskog / kalklindeskog
input layers, straight from the source rasters on NASA Earthdata Cloud.

This is the raster source behind GFW's `umd_glad_dist_alerts` (see
extract_dist_alerts.py). OPERA gives the full 19-layer detail (anomaly
magnitude, duration, count, status, ...) that GFW flattens to point alerts, as
Cloud-Optimized GeoTIFFs, 30 m, daily, 2022-present.

Access is cloud-native: earthaccess does the CMR granule search + Earthdata
Login, then each layer COG is read *windowed* with rioxarray over the polygon's
bounding box. By default this streams with earthaccess.open(); for larger,
repeatable runs, use --access_mode download to cache COGs locally first and read
them from fast local storage. One OPERA granule = one MGRS tile for one
acquisition, so a polygon's time series spans many granules; we keep one row per
(polygon, granule, disturbed pixel) -> a long-format observation table.

We keep only pixels with VEG-DIST-STATUS >= 1 (some level of disturbance). Dates
(VEG-DIST-DATE / VEG-LAST-DATE) are decoded from the OPERA epoch (days since
2020-12-31) to ISO dates. Output mirrors dist_alerts.parquet so the GFW and
OPERA tables, and the AlphaEarth table, all join on (layer, _fid).

Sibling of extract_dist_alerts.py / extract_alphaearth_embeddings.py: same
`load_features`, same DuckDB-buffer-is-single-source-of-truth durability
(no checkpoint.json; `done` table written + CHECKPOINT-flushed per work unit),
same --limit / --layers / resume behaviour.

Auth: needs an Earthdata Login. earthaccess reads `~/.netrc`
(machine urs.earthdata.nasa.gov), or $EARTHDATA_USERNAME/$EARTHDATA_PASSWORD.

Run with the `geo` pixi env:
  ~/.pixi/envs/geo/bin/python extract_opera_dist_alerts.py --limit 1 --layers MI_olivinskog
"""

import os
import time
import datetime as dt
from pathlib import Path
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import numpy as np
import pandas as pd
import geopandas as gpd
from pyogrio import list_layers
from shapely.geometry import Polygon, box
from tqdm.auto import tqdm
import duckdb

import earthaccess
import rioxarray  # noqa: F401  (registers the .rio accessor)
import rasterio

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
SHORT_NAME = "OPERA_L3_DIST-ALERT-HLS_V1"
WGS84 = "EPSG:4326"

# OPERA dates are stored as days since this epoch; -1 means "no date".
OPERA_EPOCH = dt.date(2020, 12, 31)

# Layers to extract (suffix of the granule asset filename, before .tif).
# Core VEG set + duration/history + the QA mask. STATUS drives the keep filter.
LAYERS = [
    "VEG-DIST-STATUS",   # 0 none; >=1 disturbed (provisional/confirmed/finished)
    "VEG-DIST-CONF",
    "VEG-ANOM-MAX",      # max anomaly % (0-100)
    "VEG-DIST-COUNT",
    "VEG-DIST-DATE",     # days since epoch of first detection
    "VEG-DIST-DUR",
    "VEG-LAST-DATE",     # days since epoch of most recent detection
    "VEG-HIST",
    "VEG-IND",
    "DATA-MASK",
]
STATUS_LAYER = "VEG-DIST-STATUS"
DATE_LAYERS = {"VEG-DIST-DATE", "VEG-LAST-DATE"}  # decoded to ISO date columns
# Per-layer nodata is read from each COG; STATUS nodata is commonly 255.

# A stable per-feature id column is chosen per layer in load_features().
ID_CANDIDATES = [
    "union_id",                # processed union parquets
    "polygon_id",              # all_schemes parquets
    "identifikasjon_lokalId",  # MI / HB13
    "Kartleggin",              # NiN5k
    "objectid",                # naturtiltak
]
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
                except Exception:  # noqa: BLE001
                    retries += 1
                    if retries >= max_retries:
                        raise
                    time.sleep(backoff_factor ** retries)
        return wrapper
    return decorator


def col_name(layer_suffix):
    """COG asset suffix -> tidy lowercase column name, e.g. VEG-DIST-STATUS -> veg_dist_status."""
    return layer_suffix.lower().replace("-", "_")


def decode_opera_date(days):
    """OPERA day-count -> ISO date string; None for the -1 / fill 'no date'."""
    if days is None or days < 0:
        return None
    return (OPERA_EPOCH + dt.timedelta(days=int(days))).isoformat()


# ----------------------------------------------------------------------------
# Feature loading  (identical to the sibling extractors)
# ----------------------------------------------------------------------------
def load_features(gpkg_path, layer):
    """Read a layer, reproject to EPSG:4326, pick an id column and attributes."""
    gdf = gpd.read_file(gpkg_path, layer=layer)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)
    gdf = gdf.to_crs(WGS84)

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
    """Read an input parquet, reproject to EPSG:4326, assign _fid.

    Returns (GeoDataFrame[_fid, *attrs, geometry], attrs list, layer_name).
    """
    gdf = gpd.read_parquet(parquet_path)
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)
    gdf = gdf.to_crs(WGS84)

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
# Earthdata / CMR
# ----------------------------------------------------------------------------
def earthdata_login():
    """Log in to Earthdata via netrc (falls back to env vars)."""
    try:
        auth = earthaccess.login(strategy="netrc")
    except Exception:  # noqa: BLE001
        auth = earthaccess.login(strategy="environment")
    if not getattr(auth, "authenticated", False):
        raise SystemExit(
            "[ERROR] Earthdata login failed. Add a `machine urs.earthdata.nasa.gov`"
            " entry to ~/.netrc, or set EARTHDATA_USERNAME / EARTHDATA_PASSWORD."
        )
    return auth


@retry(max_retries=4, backoff_factor=2)
def search_granules(bbox, start, end):
    """CMR search: OPERA DIST-ALERT-HLS granules intersecting bbox over [start,end]."""
    return earthaccess.search_data(
        short_name=SHORT_NAME,
        bounding_box=tuple(bbox),  # (minx, miny, maxx, maxy)
        temporal=(start, end),
    )


def granule_layer_urls(granule):
    """Map layer-suffix -> https COG url for the layers we want, for one granule."""
    urls = {}
    for u in granule.data_links(access="external"):
        if not u.endswith(".tif"):
            continue
        suffix = u.rsplit("_", 1)[-1][:-4]  # ..._VEG-DIST-STATUS.tif -> VEG-DIST-STATUS
        if suffix in LAYERS:
            urls[suffix] = u
    return urls


def granule_acq_date(granule):
    """ISO acquisition date (start) for a granule, for the row's `obs_date`."""
    try:
        t = granule["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
        return t[:10]
    except Exception:  # noqa: BLE001
        return None


def granule_geometry(granule):
    """Shapely footprint from a CMR granule's UMM spatial metadata."""
    geom = (granule.get("umm", {})
            .get("SpatialExtent", {})
            .get("HorizontalSpatialDomain", {})
            .get("Geometry", {}))
    polys = []
    for gpoly in geom.get("GPolygons", []):
        points = gpoly.get("Boundary", {}).get("Points", [])
        coords = [(p["Longitude"], p["Latitude"]) for p in points]
        if len(coords) >= 4:
            polys.append(Polygon(coords))
    if polys:
        return polys[0] if len(polys) == 1 else gpd.GeoSeries(polys, crs=WGS84).union_all()

    rects = geom.get("BoundingRectangles", [])
    if rects:
        bounds = [
            box(r["WestBoundingCoordinate"], r["SouthBoundingCoordinate"],
                r["EastBoundingCoordinate"], r["NorthBoundingCoordinate"])
            for r in rects
        ]
        return bounds[0] if len(bounds) == 1 else gpd.GeoSeries(bounds, crs=WGS84).union_all()
    return None


def granule_candidate(granule):
    """Small in-memory record used for layer-level CMR search reuse."""
    return {
        "umm": granule.get("umm", {}),
        "_geometry": granule_geometry(granule),
        "_urls": granule_layer_urls(granule),
    }


def granule_urls(granule):
    """Layer URL mapping for either an earthaccess granule or a cached candidate."""
    return granule.get("_urls") or granule_layer_urls(granule)


# ----------------------------------------------------------------------------
# Windowed COG read
#
# Reads can use either earthaccess.open() fsspec file handles or local files
# populated by earthaccess.download(). Streaming avoids full-granule downloads for
# quick probes; downloaded local COGs are usually faster and more reliable for
# repeated/large runs because the same assets are reused from local storage.
# ----------------------------------------------------------------------------
@retry(max_retries=4, backoff_factor=2)
def read_window_source(source, geom_4326):
    """Clip one COG source to the polygon bbox. Returns (sub, nodata)."""
    with rioxarray.open_rasterio(source, masked=False) as src:
        da = src.squeeze("band", drop=True)
        bnds = gpd.GeoSeries([geom_4326], crs=WGS84).to_crs(da.rio.crs).total_bounds
        sub = da.rio.clip_box(minx=bnds[0], miny=bnds[1],
                              maxx=bnds[2], maxy=bnds[3]).load()
    return sub, sub.rio.nodata


def url_cache_path(url, cache_dir):
    """Local cache path for one Earthdata asset URL."""
    return Path(cache_dir) / Path(urlparse(url).path).name


def local_sources(urls, cache_dir, download_workers):
    """Download missing URLs into cache_dir and return local file paths in URL order."""
    if not urls:
        return []
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    wanted = [url_cache_path(url, cache_dir) for url in urls]
    missing = [url for url, path in zip(urls, wanted) if not path.exists()]
    if missing:
        earthaccess.download(
            missing,
            local_path=cache_dir,
            threads=download_workers,
            show_progress=False,
        )
    return [str(path) for path in wanted]


def status_keep_mask(status, status_nd, geom_4326, mask_cache):
    """Boolean keep-mask (inside polygon AND STATUS>=1 AND not-nodata) + pixel idx.

    Returns (keep, lon, lat) where keep is the 2-D mask and lon/lat are the WGS84
    centres of the kept pixels. `mask_cache` maps the COG CRS -> the polygon
    inside-mask for that grid, reused across a tile's granules (same grid)."""
    crs = status.rio.crs
    transform = status.rio.transform()
    ny, nx = status.shape

    cache_key = (str(crs), ny, nx, tuple(np.round(transform[:6], 6)))
    inside = mask_cache.get(cache_key)
    if inside is None:
        from rasterio.features import geometry_mask
        geom_proj = gpd.GeoSeries([geom_4326], crs=WGS84).to_crs(crs).iloc[0]
        inside = ~geometry_mask([geom_proj], out_shape=(ny, nx),
                                transform=transform, invert=False)
        mask_cache[cache_key] = inside

    vals = status.values
    keep = inside & (vals >= 1)
    if status_nd is not None:
        keep &= (vals != status_nd)
    if not keep.any():
        return None, None, None

    rows_idx, cols_idx = np.where(keep)
    xs, ys = rasterio.transform.xy(transform, rows_idx, cols_idx, offset="center")
    pts = gpd.GeoSeries(gpd.points_from_xy(np.asarray(xs), np.asarray(ys), crs=crs)
                        ).to_crs(WGS84)
    return keep, pts.x.to_numpy(), pts.y.to_numpy()


def build_granule_df(layer, fid, attrs_row, granule, status_win, other_windows,
                     geom_4326, mask_cache):
    """Rows for one granule: STATUS window (already read) + the other layer windows.

    `status_win` is (sub, nodata) for VEG-DIST-STATUS; `other_windows` maps
    suffix -> (sub, nodata) for the remaining layers (read only because STATUS
    found disturbance here). Returns a DataFrame or None.
    """
    if status_win is None or status_win[0] is None or status_win[0].size == 0:
        return None
    status, status_nd = status_win
    keep, lon, lat = status_keep_mask(status, status_nd, geom_4326, mask_cache)
    if keep is None:
        return None
    ny, nx = status.shape

    out = {"lon": lon, "lat": lat, col_name(STATUS_LAYER): status.values[keep]}
    for suffix, win in other_windows.items():
        sub = win[0] if win is not None else None
        nd = win[1] if win is not None else None
        if sub is not None and sub.shape != (ny, nx):
            sub = sub.rio.reproject_match(status)  # align rare grid mismatch
        if sub is None:
            vals = np.full(keep.sum(), np.nan)
        else:
            vals = sub.values[keep].astype("float64")
            if nd is not None:
                vals[vals == nd] = np.nan  # mask this layer's own fill, not STATUS's
        if suffix in DATE_LAYERS:
            out[col_name(suffix)] = [decode_opera_date(v) for v in vals]
        else:
            out[col_name(suffix)] = vals

    df = pd.DataFrame(out)
    df["obs_date"] = granule_acq_date(granule)
    df["granule"] = granule["umm"]["GranuleUR"]
    df["dist_version"] = SHORT_NAME
    for k, v in attrs_row.items():
        df["survey_year" if k == "year" else k] = v
    df.insert(0, "_fid", fid)
    df.insert(0, "layer", layer)
    return df


# ----------------------------------------------------------------------------
# DuckDB buffer  (write_df / load_done identical to extract_dist_alerts.py)
# ----------------------------------------------------------------------------
def write_df(df, db_conn):
    """Append df to `data`, evolving the schema as needed. Caller holds the lock."""
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
            rename[col] = match
        else:
            db_conn.execute(f'ALTER TABLE data ADD COLUMN "{col}" VARCHAR')
            existing_lower[col.lower()] = col
    if rename:
        df = df.rename(columns=rename)
    db_conn.execute("INSERT INTO data BY NAME SELECT * FROM df")


def load_done(db_conn):
    """Set of (layer, _fid) already finished, derived from the buffer."""
    db_conn.execute("CREATE TABLE IF NOT EXISTS done (layer VARCHAR, _fid VARCHAR)")
    return {(r[0], r[1]) for r in db_conn.execute("SELECT layer, _fid FROM done").fetchall()}


# ----------------------------------------------------------------------------
# Extraction
# ----------------------------------------------------------------------------
def process_polygon(layer, fid, geom_4326, attrs_row, start, end, db_conn, lock,
                    read_workers=64, access_mode="stream", cache_dir=None,
                    download_workers=8, open_workers=64, granule_candidates=None):
    """Search granules for one polygon, read all layer windows, persist, record done.

    All wanted layer assets across all of the polygon's granules are opened or
    downloaded in one batch and read with one thread pool, then grouped back per
    granule.
    """
    def finish(n):
        with lock:
            db_conn.execute("INSERT INTO done VALUES (?, ?)", [layer, fid])
            db_conn.execute("CHECKPOINT")
        return n

    bbox = geom_4326.bounds  # (minx, miny, maxx, maxy)
    if granule_candidates is None:
        granules = search_granules(bbox, start, end)
    else:
        granules = [
            g for g in granule_candidates
            if g.get("_geometry") is None or g["_geometry"].intersects(geom_4326)
        ]
    if not granules:
        return finish(0)

    per_granule_urls = [granule_urls(g) for g in granules]
    mask_cache = {}  # COG-CRS grid -> polygon inside-mask (reused across tile granules)

    def read_batch(urls):
        """Open/download urls, then threaded windowed reads -> list of (sub, nodata)|None."""
        if not urls:
            return []
        if access_mode == "download":
            sources = local_sources(urls, cache_dir, download_workers)
        else:
            # earthaccess.open() defaults to pqdm n_jobs=8, which throttles handle
            # creation (each open does an fs.info HEAD + open). Raising it is the
            # single biggest streaming win: 270 URLs go 80s@8 -> 11s@64 -> 10s@128.
            sources = earthaccess.open(urls, pqdm_kwargs={"n_jobs": open_workers})

        def rd(i):
            try:
                return i, read_window_source(sources[i], geom_4326)
            except Exception:  # noqa: BLE001 -- a failed layer is None, not fatal
                return i, None

        out = [None] * len(sources)
        with ThreadPoolExecutor(max_workers=read_workers) as ex:
            for i, win in ex.map(rd, range(len(sources))):
                out[i] = win
        return out

    # Phase 1: read STATUS for every granule; skip granules missing STATUS.
    status_idx = [gi for gi, u in enumerate(per_granule_urls) if STATUS_LAYER in u]
    status_wins = read_batch([per_granule_urls[gi][STATUS_LAYER] for gi in status_idx])
    status_by_gi = dict(zip(status_idx, status_wins))

    # Phase 2: only for granules whose STATUS has disturbed pixels in the polygon,
    # read the remaining layers (the big saving: clean granules cost 1 read, not 10).
    other_tasks = []  # (gi, suffix) parallel to other_url_list
    other_url_list = []
    for gi in status_idx:
        sw = status_by_gi[gi]
        if sw is None or sw[0] is None:
            continue
        keep, _, _ = status_keep_mask(sw[0], sw[1], geom_4326, mask_cache)
        if keep is None:
            continue  # clean granule: no second-phase reads
        for suffix, url in per_granule_urls[gi].items():
            if suffix == STATUS_LAYER:
                continue
            other_tasks.append((gi, suffix))
            other_url_list.append(url)

    other_wins = read_batch(other_url_list)
    other_by_gi = {}
    for (gi, suffix), win in zip(other_tasks, other_wins):
        other_by_gi.setdefault(gi, {})[suffix] = win

    # Assemble per-granule disturbed-pixel DataFrames (only disturbed granules have entries)
    frames = []
    for gi in other_by_gi:
        df = build_granule_df(layer, fid, attrs_row, granules[gi],
                              status_by_gi[gi], other_by_gi[gi], geom_4326, mask_cache)
        if df is not None and not df.empty:
            frames.append(df)

    n = 0
    with lock:
        for df in frames:
            write_df(df, db_conn)
            n += len(df)
        db_conn.execute("INSERT INTO done VALUES (?, ?)", [layer, fid])
        db_conn.execute("CHECKPOINT")
    return n


def process_layer(gpkg_path, layer, start, end, db_conn, done, lock,
                  poly_workers, keep_fids=None, read_workers=64,
                  access_mode="stream", cache_dir=None, download_workers=8,
                  open_workers=64, search_scope="polygon", gdf=None, attrs=None):
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
        units.append((fid, row.geometry, {a: row[a] for a in attrs}))
    if not units:
        print(f"  [done] {layer}: already complete ({len(gdf)} polys)")
        return 0

    granule_candidates = None
    if search_scope == "layer":
        layer_bounds = tuple(gdf.total_bounds)
        layer_granules = search_granules(layer_bounds, start, end)
        granule_candidates = [granule_candidate(g) for g in layer_granules]
        print(f"  [info] {layer}: {len(granule_candidates):,} granules from one layer-level CMR search")

    total = 0

    def run(unit):
        fid, geom, attrs_row = unit
        return process_polygon(layer, fid, geom, attrs_row, start, end,
                               db_conn, lock, read_workers=read_workers,
                               access_mode=access_mode, cache_dir=cache_dir,
                               download_workers=download_workers,
                               open_workers=open_workers,
                               granule_candidates=granule_candidates)

    with ThreadPoolExecutor(max_workers=poly_workers) as ex:
        futures = {ex.submit(run, u): u for u in units}
        with tqdm(total=len(units), desc=layer, ncols=100, leave=True) as pbar:
            for fut in as_completed(futures):
                try:
                    total += fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"  [ERROR] {layer}|{futures[fut][0]}: {str(e)[:160]}")
                pbar.update(1)
    return total


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------
def main():
    import argparse
    base_dir = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(os.path.dirname(base_dir), "Data",
                                "Olivinskog_kalklindeskog")
    default_out = os.path.join(base_dir, "data", "opera_dist_alerts.parquet")
    default_parquets = [
        os.path.join(base_dir, "data", "processed", "olivinskog_union.parquet"),
        os.path.join(base_dir, "data", "processed", "kalklindeskog_union.parquet"),
    ]

    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--parquets", nargs="*", default=default_parquets,
                    help="input .parquet files to use (default: processed union "
                         "parquets for olivinskog and kalklindeskog). Takes priority over --gpkgs.")
    ap.add_argument("--data_dir", default=default_data)
    ap.add_argument("--gpkgs", nargs="*", default=["skog_kartlegginger.gpkg"],
                    help="GeoPackage files (used only when --parquets is empty)")
    ap.add_argument("--output", default=default_out)
    ap.add_argument("--start_date", default="2022-01-01")
    ap.add_argument("--end_date", default=dt.date.today().isoformat())
    ap.add_argument("--poly_workers", type=int, default=2,
                    help="Parallel polygons (each opens many fsspec COG handles)")
    ap.add_argument("--read_workers", type=int, default=64,
                    help="Parallel COG window reads per polygon (fsspec, latency-bound). "
                         "Default 64; reads scale near-linearly (601 reads 65s@16 -> 18s@64).")
    ap.add_argument("--open_workers", type=int, default=64,
                    help="Parallel earthaccess.open() handle creation (stream mode). "
                         "Default 64; 270 URLs go 80s@8 -> 11s@64. Biggest streaming win.")
    ap.add_argument("--access_mode", choices=["stream", "download"], default="stream",
                    help="stream via earthaccess.open(), or cache assets locally before reading")
    ap.add_argument("--cache_dir", default=os.path.join(base_dir, "data", "raw", "opera_cogs"),
                    help="Local COG cache used when --access_mode download")
    ap.add_argument("--download_workers", type=int, default=8,
                    help="Parallel earthaccess.download workers for --access_mode download")
    ap.add_argument("--search_scope", choices=["polygon", "layer"], default="polygon",
                    help="CMR search per polygon, or once per layer extent and filter locally")
    ap.add_argument("--layers", nargs="*", default=None,
                    help="Restrict to these layer names (applies to gpkg mode only)")
    ap.add_argument("--fids", nargs="*", default=None,
                    help="Restrict to these computed _fid values within each selected layer")
    ap.add_argument("--limit", type=int, default=None,
                    help="Test mode: only first N polygons per layer")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    earthdata_login()
    print(f"[OK] Earthdata login; {SHORT_NAME} ({args.start_date}..{args.end_date}); "
            f"access={args.access_mode}; search={args.search_scope}")

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
    if not done:
        try:
            db_conn.execute("INSERT INTO done SELECT DISTINCT layer, _fid FROM data")
            done = load_done(db_conn)
            if done:
                print(f"  [info] rebuilt done-set from buffer ({len(done):,} polys)")
        except duckdb.CatalogException:
            pass
    lock = Lock()

    total = 0
    t0 = time.time()
    for path, layer, gdf_pre, attrs_pre in targets:
        keep = None
        if args.fids:
            keep = set(args.fids)
        if args.limit:
            src_gdf = gdf_pre if gdf_pre is not None else load_features(path, layer)[0]
            limited = set(src_gdf["_fid"].head(args.limit))
            keep = limited if keep is None else keep & limited
        total += process_layer(path, layer, args.start_date, args.end_date,
                               db_conn, done, lock, args.poly_workers,
                               keep_fids=keep, read_workers=args.read_workers,
                               access_mode=args.access_mode, cache_dir=args.cache_dir,
                               download_workers=args.download_workers,
                               open_workers=args.open_workers,
                               search_scope=args.search_scope,
                               gdf=gdf_pre, attrs=attrs_pre)

    print("\n[export] writing parquet...")
    tmp = args.output + ".tmp"
    try:
        db_conn.execute(f"COPY data TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = db_conn.execute("SELECT count(*) FROM data").fetchone()[0]
    except duckdb.CatalogException:
        n = 0
        print("  [info] no disturbed pixels extracted; nothing to write")
    db_conn.close()
    if n:
        if os.path.exists(args.output):
            os.remove(args.output)
        os.rename(tmp, args.output)
    if os.path.exists(db_path):
        os.remove(db_path)
    print(f"[OK] {n:,} disturbed-pixel rows in {args.output} "
          f"(+{total:,} this run) in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
