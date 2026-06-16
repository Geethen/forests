#!/usr/bin/env python
"""
Build a self-contained DIST-ALERT inspector HTML from the extracted alert tables.

Reads the two long-format alert tables produced by the extraction scripts and
embeds them into a single static `dist_alert_inspector.html` (no server, no
build step -- open the file directly):

  * GFW UMD/GLAD DIST-ALERT     (data/dist_alerts.parquet)        -- sparse point alerts
  * NASA OPERA L3 DIST-ALERT    (data/opera_dist_alerts.parquet)  -- dense per-pixel detail

Both join on (layer, _fid) to the same polygons. The page shows each source as
its own toggleable map layer; clicking a pixel opens imagery time-series links
(Google Earth, Esri Wayback, Esri World Imagery, Sentinel Hub) and a context
panel with that pixel's own attributes plus nearby alerts from the *other*
source. The link patterns follow the DegreeofRecovery buffer_inspector.html
(earth.google.com/web/search pin + livingatlas.arcgis.com/wayback time-series).

Regenerate after re-running either extraction:
  ~/.pixi/envs/geo/bin/python build_dist_alert_inspector.py
"""

import os
import json
import math
import argparse

import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _clean(records):
    """Make rows JSON-safe: NaN/inf -> None, numpy scalars -> python scalars."""
    out = []
    for r in records:
        row = {}
        for k, v in r.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
            elif hasattr(v, "item"):  # numpy scalar
                row[k] = v.item()
            else:
                row[k] = v
        out.append(row)
    return out


def load_gfw(path):
    """GFW point alerts -> compact records (one per alert pixel)."""
    if not os.path.exists(path):
        print(f"[warn] missing {path}; GFW layer will be empty")
        return []
    df = pd.read_parquet(path)
    keep = [
        "lat", "lon", "alert_date", "confidence", "intensity", "_fid", "layer",
        "områdenavn", "naturtype", "naturtypeKode", "tilstand", "survey_year",
        "kommuner",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    # normalise the Norwegian column name to ASCII for the JS side
    if "områdenavn" in df.columns:
        df = df.rename(columns={"områdenavn": "omradenavn"})
    df["lat"] = df["lat"].round(6)
    df["lon"] = df["lon"].round(6)
    return _clean(df.to_dict("records"))


def load_opera(path):
    """OPERA per-pixel detail -> compact records (one per disturbed pixel)."""
    if not os.path.exists(path):
        print(f"[warn] missing {path}; OPERA layer will be empty")
        return []
    df = pd.read_parquet(path)
    keep = [
        "lat", "lon", "_fid", "layer",
        "veg_dist_status", "veg_anom_max", "veg_dist_conf", "veg_dist_count",
        "veg_dist_dur", "veg_dist_date", "veg_last_date", "obs_date",
        "områdenavn", "naturtype", "naturtypeKode", "tilstand", "survey_year",
        "kommuner",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()
    if "områdenavn" in df.columns:
        df = df.rename(columns={"områdenavn": "omradenavn"})
    df["lat"] = df["lat"].round(6)
    df["lon"] = df["lon"].round(6)
    # integer-ish OPERA fields read cleaner without a trailing .0
    for c in ["veg_dist_status", "veg_anom_max", "veg_dist_conf",
              "veg_dist_count", "veg_dist_dur"]:
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    recs = df.to_dict("records")
    # Int64 NA -> None for json
    for r in recs:
        for k, v in list(r.items()):
            if v is pd.NA:
                r[k] = None
    return _clean(recs)


HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DIST-ALERT Inspector</title>
<script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
<link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
<style>
  :root {
    --ink: #111; --muted: #666; --line: #e2e4e8;
    --nominal: #f59e0b; --high: #ef4444; --highest: #7c3aed;
    --opera: #2563eb;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Helvetica Neue", Arial, sans-serif; color: var(--ink);
         background: #f0f2f5; display: flex; flex-direction: column; height: 100vh; }

  header {
    padding: 11px 18px; background: #fff; border-bottom: 1px solid var(--line);
    flex-shrink: 0; display: flex; align-items: baseline; gap: 14px;
  }
  header h1 { font-size: 15px; font-weight: 700; }
  header p  { font-size: 12px; color: var(--muted); }

  .body { display: flex; flex: 1 1 0; min-height: 0; }

  #map { flex: 1 1 auto; position: relative; }

  /* layer toggle, top-right over the map */
  #layer-toggle {
    position: absolute; top: 10px; right: 10px; z-index: 2;
    background: #fff; border: 1px solid var(--line); border-radius: 8px;
    padding: 8px 11px; font-size: 12px; box-shadow: 0 1px 4px rgba(0,0,0,.12);
  }
  #layer-toggle label { display: flex; align-items: center; gap: 7px;
                        cursor: pointer; padding: 2px 0; white-space: nowrap; }
  #layer-toggle .sw { width: 11px; height: 11px; border-radius: 50%;
                      border: 1.5px solid #fff; box-shadow: 0 0 0 1px #bbb; }

  #panel {
    flex: 0 0 312px; background: #fff; border-left: 1px solid var(--line);
    display: flex; flex-direction: column; overflow: hidden;
  }
  #panel-scroll { flex: 1; overflow-y: auto; padding: 16px 16px 20px; }

  .hint { color: var(--muted); font-size: 12.5px; line-height: 1.65; padding-top: 4px; }

  h2 { font-size: 13.5px; font-weight: 700; line-height: 1.4; margin-bottom: 3px; }
  .fid-line { font-family: ui-monospace, monospace; font-size: 10.5px; color: var(--muted);
              margin-bottom: 9px; word-break: break-all; }

  .chip {
    display: inline-block; padding: 2px 9px; border-radius: 11px; color: #fff;
    font-size: 11px; font-weight: 700; letter-spacing: .04em; margin-bottom: 11px;
  }
  .chip.src { background: #334155; margin-left: 5px; }

  table.kv { width: 100%; border-collapse: collapse; font-size: 12.5px; margin-bottom: 14px; }
  table.kv td { padding: 4px 2px; border-bottom: 1px solid #f2f3f5; vertical-align: top; }
  table.kv td.k { color: var(--muted); width: 46%; }
  table.kv td.v { text-align: right; font-variant-numeric: tabular-nums; }

  .section-label {
    font-size: 10px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .07em; color: var(--muted); margin: 12px 0 6px;
  }

  a.maplink {
    display: flex; align-items: center; gap: 9px;
    padding: 9px 11px; margin: 4px 0; border-radius: 7px;
    text-decoration: none; font-size: 12.5px; font-weight: 600;
    border: 1px solid var(--line); background: #f7f9ff; color: #1a56db;
    transition: background .13s, border-color .13s;
  }
  a.maplink:hover { background: #e8eeff; border-color: #b0c0f0; }
  a.maplink .icon { font-size: 18px; flex-shrink: 0; line-height: 1; }
  a.maplink .ltext { flex: 1; }
  a.maplink .sub   { font-size: 11px; font-weight: 400; color: var(--muted);
                     display: block; margin-top: 1px; }

  .nearby { margin-top: 4px; }
  .nearby .nb-row {
    display: flex; justify-content: space-between; gap: 8px;
    font-size: 11.5px; padding: 5px 0; border-bottom: 1px solid #f2f3f5;
  }
  .nearby .nb-row .nb-meta { color: var(--muted); font-variant-numeric: tabular-nums; }
  .nearby .nb-empty { color: var(--muted); font-size: 11.5px; padding: 4px 0; }
  .nb-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
            margin-right: 5px; vertical-align: middle; }

  .status { font-size: 11px; color: var(--muted); line-height: 1.5;
            margin-top: 9px; padding-top: 8px; border-top: 1px solid var(--line); }

  .legend {
    border-top: 1px solid var(--line); padding: 9px 16px 12px; flex-shrink: 0;
  }
  .legend-title { font-size: 11px; font-weight: 700; margin: 6px 0 5px; }
  .legend-row { display: flex; align-items: center; gap: 7px;
                font-size: 11.5px; margin: 3px 0; }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot.sq { border-radius: 2px; }
</style>
</head>
<body>

<header>
  <h1>DIST-ALERT Inspector</h1>
  <p>UMD/GLAD &amp; NASA OPERA vegetation-disturbance alerts &middot; __OMRADE__ &middot; click a point</p>
</header>

<div class="body">
  <div id="map">
    <div id="layer-toggle">
      <label><input type="checkbox" id="tg-gfw" checked>
        <span class="sw" style="background:var(--nominal)"></span> GFW DIST-ALERT (<span id="n-gfw">0</span>)</label>
      <label><input type="checkbox" id="tg-opera" checked>
        <span class="sw" style="background:var(--opera)"></span> OPERA DIST-ALERT (<span id="n-opera">0</span>)</label>
    </div>
  </div>

  <aside id="panel">
    <div id="panel-scroll">
      <p class="hint">Click any alert point on the map to see its disturbance
      metadata, nearby alerts from the other source, and open imagery
      time-series links for visual inspection.</p>
    </div>
    <div class="legend">
      <div class="legend-title">GFW confidence</div>
      <div class="legend-row"><div class="dot" style="background:var(--nominal)"></div>Nominal</div>
      <div class="legend-row"><div class="dot" style="background:var(--high)"></div>High</div>
      <div class="legend-row"><div class="dot" style="background:var(--highest)"></div>Highest</div>
      <div class="legend-title">OPERA status</div>
      <div class="legend-row"><div class="dot sq" style="background:#93c5fd"></div>Provisional (1)</div>
      <div class="legend-row"><div class="dot sq" style="background:#2563eb"></div>Confirmed (5&ndash;6)</div>
      <div class="legend-row"><div class="dot sq" style="background:#1e3a8a"></div>Finished (7&ndash;8)</div>
    </div>
  </aside>
</div>

<script>
// ── Embedded alert data (generated by build_dist_alert_inspector.py) ────────
const GFW   = __GFW_DATA__;
const OPERA = __OPERA_DATA__;

// ── GeoJSON builders ────────────────────────────────────────────────────────
function toGeo(rows, src) {
  return {
    type: 'FeatureCollection',
    features: rows.map((d, i) => ({
      type: 'Feature',
      id: i,
      geometry: { type: 'Point', coordinates: [d.lon, d.lat] },
      properties: { ...d, _idx: i, _src: src }
    }))
  };
}
const gfwGeo   = toGeo(GFW,   'gfw');
const operaGeo = toGeo(OPERA, 'opera');

document.getElementById('n-gfw').textContent   = GFW.length;
document.getElementById('n-opera').textContent = OPERA.length;

// ── Map init ───────────────────────────────────────────────────────────────
const map = new maplibregl.Map({
  container: 'map',
  style: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
  center: [6.3, 62.1],
  zoom: 8,
  attributionControl: { compact: true }
});
map.addControl(new maplibregl.NavigationControl(), 'top-left');
map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-left');

map.on('load', () => {
  map.addSource('gfw',   { type: 'geojson', data: gfwGeo });
  map.addSource('opera', { type: 'geojson', data: operaGeo });

  // selection halo (shared; hidden until a point is clicked)
  map.addSource('halo', { type: 'geojson', data: { type: 'FeatureCollection', features: [] } });
  map.addLayer({
    id: 'sel-halo', type: 'circle', source: 'halo',
    paint: {
      'circle-radius': 15, 'circle-color': '#fff', 'circle-opacity': 0.8,
      'circle-stroke-width': 2.5, 'circle-stroke-color': '#333'
    }
  });

  // OPERA pixels: square-ish, coloured by VEG-DIST-STATUS stage
  map.addLayer({
    id: 'opera-points', type: 'circle', source: 'opera',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 3.2, 14, 8],
      'circle-color': [
        'step', ['to-number', ['get', 'veg_dist_status']],
        '#93c5fd',           // 1: provisional
        5, '#2563eb',        // 5-6: confirmed
        7, '#1e3a8a'         // 7-8: finished
      ],
      'circle-stroke-width': 0.6, 'circle-stroke-color': '#fff',
      'circle-opacity': 0.85
    }
  });

  // GFW alerts: coloured by confidence, drawn on top
  map.addLayer({
    id: 'gfw-points', type: 'circle', source: 'gfw',
    paint: {
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 6, 5, 14, 10],
      'circle-color': [
        'match', ['get', 'confidence'],
        'nominal', '#f59e0b', 'high', '#ef4444', 'highest', '#7c3aed', '#888'
      ],
      'circle-stroke-width': 1.5, 'circle-stroke-color': '#fff',
      'circle-opacity': 0.92
    }
  });

  function wireClicks(layerId, src) {
    map.on('click', layerId, e => {
      const p = e.features[0].properties;
      const d = (src === 'gfw' ? GFW : OPERA)[p._idx];
      map.getSource('halo').setData({
        type: 'FeatureCollection',
        features: [{ type: 'Feature', geometry: { type: 'Point', coordinates: [d.lon, d.lat] }, properties: {} }]
      });
      showPanel(src, p._idx);
    });
    map.on('mouseenter', layerId, () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', layerId, () => { map.getCanvas().style.cursor = ''; });
  }
  wireClicks('gfw-points', 'gfw');
  wireClicks('opera-points', 'opera');

  // layer toggles
  document.getElementById('tg-gfw').addEventListener('change', e => {
    map.setLayoutProperty('gfw-points', 'visibility', e.target.checked ? 'visible' : 'none');
  });
  document.getElementById('tg-opera').addEventListener('change', e => {
    map.setLayoutProperty('opera-points', 'visibility', e.target.checked ? 'visible' : 'none');
  });

  // fit to all points from both sources
  const all = GFW.concat(OPERA);
  if (all.length) {
    const lons = all.map(d => d.lon), lats = all.map(d => d.lat);
    map.fitBounds(
      [[Math.min(...lons) - 0.05, Math.min(...lats) - 0.03],
       [Math.max(...lons) + 0.05, Math.max(...lats) + 0.03]],
      { padding: 50, duration: 0, maxZoom: 13 }
    );
  }
});

// ── External imagery URLs ──────────────────────────────────────────────────
function googleEarthUrl(lat, lon) {
  // Google Earth Web search — drops a pin at the exact pixel (robust framing).
  return 'https://earth.google.com/web/search/' + lat.toFixed(6) + ',' + lon.toFixed(6) + '/';
}
function waybackUrl(lat, lon) {
  // Esri Wayback — full Esri World Imagery time-series (every archived capture).
  const e = 0.004;
  return 'https://livingatlas.arcgis.com/wayback/?ext='
       + (lon - e).toFixed(6) + ',' + (lat - e).toFixed(6) + ','
       + (lon + e).toFixed(6) + ',' + (lat + e).toFixed(6);
}
function esriUrl(lat, lon) {
  return 'https://www.arcgis.com/apps/mapviewer/index.html?center='
       + lon + ',' + lat + '&level=17&basemapUrl='
       + encodeURIComponent('https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer');
}
function sentinelHubUrl(lat, lon, date) {
  // Sentinel-2 true-colour, window centred ±2 months around the alert date.
  const ref  = date ? new Date(date) : new Date();
  const from = new Date(ref); from.setMonth(from.getMonth() - 2);
  const to   = new Date(ref); to.setMonth(to.getMonth() + 2);
  const iso  = x => x.toISOString().slice(0, 10);
  const p = new URLSearchParams({
    datasetId: 'S2L2A', lat: lat, lng: lon, zoom: 14,
    fromTime: iso(from) + 'T00:00:00.000Z', toTime: iso(to) + 'T23:59:59.999Z',
    themeId: 'DEFAULT-THEME'
  });
  return 'https://apps.sentinel-hub.com/eo-browser/?' + p.toString();
}

// ── Helpers ────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s).replace(/[&<>"]/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;' }[c]));
}
function fmt(v) { return (v === null || v === undefined || v === '') ? '&ndash;' : esc(v); }

// Haversine distance in metres.
function distM(a, b) {
  const R = 6371000, rad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * rad, dLon = (b.lon - a.lon) * rad;
  const la1 = a.lat * rad, la2 = b.lat * rad;
  const h = Math.sin(dLat/2)**2 + Math.cos(la1)*Math.cos(la2)*Math.sin(dLon/2)**2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

const OPERA_STAGE = { dot(s) {
  s = Number(s);
  if (s >= 7) return '#1e3a8a';
  if (s >= 5) return '#2563eb';
  return '#93c5fd';
}, label(s) {
  s = Number(s);
  if (s >= 7) return 'finished';
  if (s >= 5) return 'confirmed';
  return 'provisional';
}};
const CONF_COLOR = { nominal: '#f59e0b', high: '#ef4444', highest: '#7c3aed' };

// Nearby alerts from the *other* source, within `radius` m, nearest first.
function nearbyFrom(rows, here, radius, src) {
  return rows
    .map(d => ({ d, m: distM(here, d) }))
    .filter(x => x.m <= radius)
    .sort((a, b) => a.m - b.m)
    .slice(0, 8)
    .map(x => {
      const d = x.d, m = Math.round(x.m);
      if (src === 'gfw') {
        const col = CONF_COLOR[d.confidence] || '#888';
        return '<div class="nb-row"><span><span class="nb-dot" style="background:'+col+'"></span>'
             + esc(d.alert_date) + ' &middot; ' + esc(d.confidence) + '</span>'
             + '<span class="nb-meta">' + m + ' m</span></div>';
      }
      return '<div class="nb-row"><span><span class="nb-dot" style="background:'+OPERA_STAGE.dot(d.veg_dist_status)+'"></span>'
           + fmt(d.veg_dist_date) + ' &middot; ' + OPERA_STAGE.label(d.veg_dist_status)
           + ' &middot; anom ' + fmt(d.veg_anom_max) + '</span>'
           + '<span class="nb-meta">' + m + ' m</span></div>';
    }).join('');
}

// ── Side panel ─────────────────────────────────────────────────────────────
function showPanel(src, idx) {
  const d = (src === 'gfw' ? GFW : OPERA)[idx];
  const name = d.omradenavn || d._fid;
  const here = { lat: d.lat, lon: d.lon };
  const refDate = src === 'gfw' ? d.alert_date : (d.veg_last_date || d.veg_dist_date);

  let head, table;
  if (src === 'gfw') {
    const col = CONF_COLOR[d.confidence] || '#888';
    head = '<span class="chip" style="background:' + col + '">' + esc((d.confidence||'').toUpperCase())
         + '</span><span class="chip src">GFW</span>';
    table =
      '<tr><td class="k">Alert date</td><td class="v"><b>' + fmt(d.alert_date) + '</b></td></tr>'
    + '<tr><td class="k">Confidence</td><td class="v">' + fmt(d.confidence) + '</td></tr>'
    + '<tr><td class="k">Intensity</td><td class="v">' + fmt(d.intensity) + '</td></tr>';
  } else {
    const col = OPERA_STAGE.dot(d.veg_dist_status);
    head = '<span class="chip" style="background:' + col + '">' + esc(OPERA_STAGE.label(d.veg_dist_status).toUpperCase())
         + '</span><span class="chip src">OPERA</span>';
    table =
      '<tr><td class="k">First detected</td><td class="v"><b>' + fmt(d.veg_dist_date) + '</b></td></tr>'
    + '<tr><td class="k">Last detected</td><td class="v">' + fmt(d.veg_last_date) + '</td></tr>'
    + '<tr><td class="k">Status code</td><td class="v">' + fmt(d.veg_dist_status) + '</td></tr>'
    + '<tr><td class="k">Anomaly max (%)</td><td class="v">' + fmt(d.veg_anom_max) + '</td></tr>'
    + '<tr><td class="k">Confidence</td><td class="v">' + fmt(d.veg_dist_conf) + '</td></tr>'
    + '<tr><td class="k">Detections</td><td class="v">' + fmt(d.veg_dist_count) + '</td></tr>'
    + '<tr><td class="k">Duration (days)</td><td class="v">' + fmt(d.veg_dist_dur) + '</td></tr>'
    + '<tr><td class="k">Observed</td><td class="v">' + fmt(d.obs_date) + '</td></tr>';
  }

  // context from the other source near this pixel
  const otherRows = src === 'gfw' ? OPERA : GFW;
  const otherSrc  = src === 'gfw' ? 'opera' : 'gfw';
  const otherName = src === 'gfw' ? 'OPERA' : 'GFW';
  const nb = nearbyFrom(otherRows, here, 150, otherSrc);
  const nearbyHtml = nb
    ? '<div class="nearby">' + nb + '</div>'
    : '<div class="nearby"><div class="nb-empty">No ' + otherName + ' alerts within 150 m.</div></div>';

  document.getElementById('panel-scroll').innerHTML =
    '<h2>' + esc(name) + '</h2>'
  + '<div class="fid-line">' + esc(d.layer) + ' &middot; ' + esc(d._fid) + '</div>'
  + head
  + '<table class="kv">' + table
  + '<tr><td class="k">Naturtype</td><td class="v">' + fmt(d.naturtype) + '</td></tr>'
  + '<tr><td class="k">Tilstand</td><td class="v">' + fmt(d.tilstand) + '</td></tr>'
  + '<tr><td class="k">Kommune</td><td class="v">' + fmt(d.kommuner) + '</td></tr>'
  + '<tr><td class="k">Lat / Lon</td><td class="v">' + d.lat.toFixed(5) + ', ' + d.lon.toFixed(5) + '</td></tr>'
  + '</table>'

  + '<div class="section-label">Imagery time-series &amp; viewers</div>'
  + '<a class="maplink" href="' + waybackUrl(d.lat, d.lon) + '" target="_blank" rel="noopener">'
  +   '<span class="icon">🕑</span><span class="ltext">Esri Wayback'
  +     '<span class="sub">Every archived World Imagery capture (time-series)</span></span></a>'
  + '<a class="maplink" href="' + googleEarthUrl(d.lat, d.lon) + '" target="_blank" rel="noopener">'
  +   '<span class="icon">🌍</span><span class="ltext">Google Earth Web'
  +     '<span class="sub">Pin at pixel · use the history slider for time-series</span></span></a>'
  + '<a class="maplink" href="' + sentinelHubUrl(d.lat, d.lon, refDate) + '" target="_blank" rel="noopener">'
  +   '<span class="icon">🛰️</span><span class="ltext">Sentinel Hub EO Browser'
  +     '<span class="sub">S2 true-colour ±2 months around the alert</span></span></a>'
  + '<a class="maplink" href="' + esriUrl(d.lat, d.lon) + '" target="_blank" rel="noopener">'
  +   '<span class="icon">📍</span><span class="ltext">Esri World Imagery'
  +     '<span class="sub">Current high-res basemap at zoom 17</span></span></a>'

  + '<div class="section-label">Nearby ' + otherName + ' alerts (&le;150 m)</div>'
  + nearbyHtml

  + '<div class="status">Tip: Ctrl/Cmd-click a link to force a new tab. '
  +   'Google Earth opens with a pin; in Wayback, pick dates from the left-hand list to step through time.</div>';

  // Fly to the clicked point (only zoom in, never out).
  map.flyTo({ center: [d.lon, d.lat], zoom: Math.max(map.getZoom(), 14), duration: 700 });
}
</script>
</body>
</html>
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--gfw", default=os.path.join(BASE_DIR, "data", "dist_alerts.parquet"))
    ap.add_argument("--opera", default=os.path.join(BASE_DIR, "data", "opera_dist_alerts.parquet"))
    ap.add_argument("--output", default=os.path.join(BASE_DIR, "dist_alert_inspector.html"))
    args = ap.parse_args()

    gfw = load_gfw(args.gfw)
    opera = load_opera(args.opera)

    # header subtitle: distinct område names across both sources, if few
    names = sorted({r.get("omradenavn") for r in (gfw + opera) if r.get("omradenavn")})
    omrade = (", ".join(names[:3]) + (" …" if len(names) > 3 else "")) if names else "Olivinskog sites"

    html = (HTML_TEMPLATE
            .replace("__GFW_DATA__", json.dumps(gfw, ensure_ascii=False))
            .replace("__OPERA_DATA__", json.dumps(opera, ensure_ascii=False))
            .replace("__OMRADE__", omrade))

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] wrote {args.output}")
    print(f"     GFW alerts:   {len(gfw):>6,}")
    print(f"     OPERA alerts: {len(opera):>6,}")
    print(f"     size:         {os.path.getsize(args.output)/1024:.0f} KB")


if __name__ == "__main__":
    main()
