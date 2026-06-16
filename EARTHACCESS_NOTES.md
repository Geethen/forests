# earthaccess / NASA Earthdata Cloud — field notes

Hard-won lessons from per-pixel windowed extraction of OPERA L3 DIST-ALERT-HLS COGs
from outside us-west-2 (server in Norway). Most apply to any earthaccess + COG +
many-files workflow. Verified 2026-06.

## Auth
- Use `~/.netrc` (`machine urs.earthdata.nasa.gov` / `login` / `password`), then
  `earthaccess.login(strategy="netrc")`. No creds in code, CLI args, or git.
- A single `login()` per process; the authed session is reused by `open()`/`download()`.

## Reading COGs: the big one — `open()` is throttled by default
- **`earthaccess.open(urls)` defaults to pqdm `n_jobs=8`.** Each open does an
  `fs.info()` HEAD **plus** the actual open = 2 round-trips/URL. At 8-wide that
  serializes badly: ~0.3 s/URL.
- **Fix: `earthaccess.open(urls, pqdm_kwargs={"n_jobs": 64})`.** Measured on 270 URLs:
  80 s @8 → 20 s @32 → 11 s @64 → 9.8 s @128. ~8×. Diminishing past 64; use 64–128.
  This is usually the single biggest win and costs one kwarg.
- `open()` returns fsspec file handles (persistent authed HTTPS session + block cache).
  **These DO parallelize** under a `ThreadPoolExecutor` for the subsequent windowed
  reads (0.02–0.3 s/read at 8–16 workers). Two separate knobs:
  `open` concurrency (pqdm n_jobs) vs `read` concurrency (your thread pool).
- Close handles (`with rioxarray.open_rasterio(fh) as src: ...; .load()`) or you get
  `sys.excepthook` teardown noise at interpreter exit.

## Do NOT use GDAL `/vsicurl` for many remote COGs
- `/vsicurl` is ~4.8 s/open and **does not parallelize under Python threads** (GDAL
  internal locks). A thread pool gives zero speedup. Switching to `earthaccess.open()`
  fsspec handles was ~7.5× on a real job. Always prefer fsspec handles for batch reads.

## S3 direct access is region-locked
- `earthaccess.get_s3_credentials()` only issues working creds **in us-west-2**. From
  elsewhere it returns `{}` / `KeyError: accessKeyId`. Use HTTPS links, not `s3://`.
- Consequence: **s5cmd / S3 tools are not an option cross-region.** They're also the
  wrong tool for windowed reads anyway (whole-file copy vs <1% byte-range reads).

## stream vs download (local cache)
- **stream** (`open()` + windowed read): stores nothing but your output. Right for
  one-shot full runs and when you must avoid large-file storage.
- **download** (`earthaccess.download()` to a cache dir): pays a cold full-file
  download once, then reads from disk (CPU-bound, fast). Warm re-reads are ~10×+
  faster — great for **dev iteration on a few granules**, but the cache is the full
  file size × count. Budget it: e.g. ~6 MB/granule × 300k granules ≈ 1.8 TB. Usually
  too big for a full archive job; reserve for repeated work on a small subset.

## CMR search: scope it once, reuse it
- Searching CMR per work-unit (per polygon) is wasteful. Search **once over the whole
  layer/AOI extent**, parse granule footprints, then filter candidates locally per
  unit by `geometry.intersects()`. Verified identical row-sets, materially faster.
- Granule counts scale with time: one MGRS tile ≈ ~110 granules/year (OPERA HLS).
  Plan opens/reads accordingly — full multi-year per-AOI runs open hundreds/tile.

## Read only what you need (two-phase, for status-gated products)
- For products with a cheap "is anything here?" layer (e.g. VEG-DIST-STATUS), do
  **Phase 1**: read only that layer for all granules. **Phase 2**: read the other
  layers ONLY for granules with a positive hit inside the AOI. Most AOIs are clean →
  1 read/granule instead of N. Huge saving at scale.
- Mask each layer's **own** nodata when sampling (not just the gate layer's).
- Cache the AOI inside-pixel mask per (CRS, grid, transform) — reused across a tile's
  granules.

## virtualizarr / kerchunk for COG time-stacks — usually NOT worth it
- virtualizarr (2.6.x) has **no TIFF/COG parser** (only DMRPP/FITS/HDF/NetCDF3/Zarr/
  Kerchunk). `kerchunk.tiff` needs `tifffile` (not a default dep).
- Even if built: making references **reads each COG's TIFF header** = the same per-file
  open round-trip you already optimized with pqdm n_jobs. It relocates the cost, doesn't
  remove it.
- COGs across MGRS tiles have **different CRSs / grids**; kerchunk's `MultiZarrToZarr`
  assumes one aligned grid, so you'd hand-write tile/CRS alignment. High effort.
- The kerchunk "record-store" discourse trick targets **millions** of references
  (memory/file-count pressure) — irrelevant at hundreds–thousands of assets.
- Bottom line: prefer `open(n_jobs=64)` + threaded windowed reads + two-phase + layer-
  scope CMR. Consider a **persisted per-tile reference catalog** only if you re-read the
  same tiles many times and per-unit open cost is proven to dominate.

## Icechunk / VirtualiZarr (Earthmover) — for a different workload than one-shot extraction
- The flagship win (NASA IMERG: month time-series 3s vs 5min) is **point time-series
  across a huge logical cube** — read one (lat,lon), sweep all time, repeatedly. That is
  the OPPOSITE of one-shot windowed extraction (spatial window per date, each granule's
  bytes touched ~once). Nothing to amortize → no win for extract-once-to-parquet jobs.
- VirtualiZarr requires **homogeneous files** (same grid/encoding). Multi-MGRS-tile COGs
  (different CRSs, mixed sensors, irregular dates) break this → one store per tile/layer
  plus cross-tile handling. High effort.
- Building the chunk manifest **scans every file's metadata up front** = the same per-file
  open round-trip you already optimized with pqdm n_jobs. It relocates the cost.
- "Secure virtual chunks" = access control for stores pointing at private buckets; needs
  **Icechunk + Arraylake (commercial)**; does NOT address per-request auth like Earthdata
  Login; security only, no read speedup.
- **When Icechunk IS right:** an ongoing, re-queried archive (e.g. a dashboard where many
  users pull time-series for arbitrary points), built once + incrementally appended as new
  granules land → each query becomes a ~3s cube slice. A different product, not a batch job.

## Durability for long resumable jobs
- Single source of truth = the output store (e.g. a DuckDB buffer with a `done(key...)`
  table). Per unit, under one lock: write rows → INSERT into `done` → `CHECKPOINT`.
  No separate `checkpoint.json` (it drifts from the data on a kill). Resume = skip keys
  already in `done`.

## Misc gotchas
- `DataGranule.size` is becoming an attribute (was `.size()`); CMR often reports 0 for
  OPERA — measure real bytes via an HTTP HEAD `Content-Length` if you need sizes.
- geopandas here uses **pyogrio**, not fiona (`pyogrio.list_layers`, not
  `fiona.listlayers`).
- OPERA date layers (VEG-DIST-DATE etc.) = **days since 2020-12-31**; `-1` = none.
