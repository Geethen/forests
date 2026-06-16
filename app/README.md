# Forest Monitor Dashboard

A static, client-side dashboard for monitoring **olivinskog** and **kalklindeskog**
forest cover and deforestation/degradation alerts over time.

Stack: [Vite](https://vitejs.dev/), [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview)
(in-browser SQL over Parquet), [MapLibre GL JS](https://maplibre.org/) (map), and
[Observable Plot](https://observablehq.com/plot/) (charts). No backend, no server-side
database - everything runs in the browser.

## Running locally

```bash
cd app
npm run dev
```

This starts the Vite dev server (default port 5180, see terminal output for the exact
URL). Open it in a normal desktop browser (Chrome, Firefox, Edge).

Note: `npm install` was run with `--no-bin-links` because this project lives on a CIFS
network mount that doesn't support symlinks, so `node_modules/.bin` doesn't exist. The
`dev`/`build`/`preview` scripts in `package.json` invoke
`node node_modules/vite/bin/vite.js` directly instead of `vite`.

The required `Cross-Origin-Opener-Policy: same-origin` /
`Cross-Origin-Embedder-Policy: require-corp` headers for DuckDB-WASM are already
configured in `vite.config.js` for both the dev server and `preview`.

DuckDB-WASM is configured to use the single-threaded **mvp** bundle
(`app/src/duck.js`). This is more broadly compatible than the threaded **eh** bundle
and is plenty fast for this app's dataset sizes.

## Data layout

All data lives in `public/data/`:

- `forest_polygons.geojson` - polygon geometries for both forest types (EPSG:4326),
  with `forest_type`, `source_layer`, `polygon_id`, `area_ha` properties.
- `alerts.parquet` - point-level deforestation/degradation/spruce-encroachment alerts.
- `area_cover_monthly.parquet` - monthly area-cover time series per forest type.
- `area_cover_annual.parquet` - annual (December snapshot) area-cover time series per
  forest type.
- `meta.json` - metadata shown in the UI: `data_status` (`"MOCK"` or `"LIVE"`),
  `generated` timestamp, `polygon_count`, `alert_count`, `period_range`.

## Data update workflow

Regenerate all of the above (currently mock data, derived from the real polygon
geometries) with:

```bash
~/.pixi/envs/geo/bin/python app/data-prep/prepare_mock_data.py
```

This produces both the monthly and annual cadence files in one run, so it can be
scheduled monthly (covers both update cadences - the annual file is just the December
rows of the monthly series).

## Swapping mock data for real data

When a real alerts/area-cover pipeline is available:

1. Replace `alerts.parquet`, `area_cover_monthly.parquet`, and
   `area_cover_annual.parquet` with the real pipeline's output, keeping the **same
   column names and types** as the mock files (see `data-prep/prepare_mock_data.py`
   for the exact schema).
2. Replace `forest_polygons.geojson` if the mapped polygons change.
3. Set `meta.json`'s `data_status` field to `"LIVE"` (instead of `"MOCK"`) - this
   updates the status badge and removes the "MOCK placeholder" note in the footer.

No code changes are required as long as the schema matches.

## Known limitations

- `npm run build` (production build) currently fails on this CIFS-mounted filesystem
  with `EAGAIN` errors during Node's ESM module resolution. The dev server works fine
  here. Run `npm run build` on local disk or in CI when ready to deploy.
- Verify the app in a normal desktop browser - headless/sandboxed browser automation
  in this environment cannot reliably run dedicated Web Workers to completion, which
  DuckDB-WASM depends on.

## Credentials / permissions

The app as built is fully static and client-side - **no credentials are required** to
run or host it.

If real alert/area-cover data derived from AlphaEarth/Earth Engine is wired in later,
that pipeline would reuse the existing Earth Engine credentials at
`~/.config/earthengine/credentials`. For public hosting (e.g. GitHub Pages), no special
credentials are needed beyond push access to the repository.
