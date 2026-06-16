"""
Prepare data for the deforestation-alerts dashboard.

Generates:
  - app/public/data/forest_polygons.geojson   real polygon geometries (EPSG:4326),
                                               tagged with forest_type (olivinskog / kalklindeskog)
  - app/public/data/area_cover_monthly.parquet  mock monthly forest-cover area per forest_type
  - app/public/data/area_cover_annual.parquet   mock annual forest-cover area per forest_type
  - app/public/data/alerts.parquet              mock deforestation alert points (monthly cadence)

Real data lives in:
  ../../../Data/Olivinskog_kalklindeskog/skog_kartlegginger.gpkg

Run with the `geo` pixi env:
  ~/.pixi/envs/geo/bin/python app/data-prep/prepare_mock_data.py
"""

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
APP_DIR = HERE.parent
OUT_DIR = APP_DIR / "public" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SRC_GPKG = HERE.parent.parent.parent / "Data" / "Olivinskog_kalklindeskog" / "skog_kartlegginger.gpkg"

LAYERS_BY_TYPE = {
    "olivinskog": ["MI_olivinskog", "NiN5k_olivinskog", "HB13_olivinskog"],
    "kalklindeskog": ["MI_kalklindeskog", "NiN5k_kalklindeskog", "HB13_kalklindeskog"],
}

rng = np.random.default_rng(42)


def load_polygons() -> gpd.GeoDataFrame:
    """Load and merge all source layers, tagged by forest type and source layer."""
    frames = []
    for forest_type, layers in LAYERS_BY_TYPE.items():
        for layer in layers:
            gdf = gpd.read_file(SRC_GPKG, layer=layer)
            gdf = gdf[["geometry"]].copy()
            gdf["forest_type"] = forest_type
            gdf["source_layer"] = layer
            gdf["polygon_id"] = [f"{layer}_{i}" for i in range(len(gdf))]
            frames.append(gdf)

    merged = pd.concat(frames, ignore_index=True)
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=frames[0].crs)

    # area in the source projected CRS (metres) -> hectares
    merged["area_ha"] = merged.geometry.area / 10_000

    # reproject to WGS84 for web mapping, simplify a touch to keep the geojson light
    merged = merged.to_crs(4326)
    merged["geometry"] = merged.geometry.simplify(0.00005, preserve_topology=True)

    return merged


def write_polygons(gdf: gpd.GeoDataFrame) -> None:
    out = gdf[["polygon_id", "forest_type", "source_layer", "area_ha", "geometry"]]
    out_path = OUT_DIR / "forest_polygons.geojson"
    out.to_file(out_path, driver="GeoJSON")
    print(f"wrote {out_path} ({len(out)} polygons)")


def make_area_cover_series(gdf: gpd.GeoDataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Mock monthly + annual forest cover area per forest_type.

    Baseline area = sum of real polygon areas per forest type. We then simulate
    a slow declining trend (cumulative loss) with seasonal noise, consistent
    with the cumulative alert areas generated in make_alerts().
    """
    baseline = gdf.groupby("forest_type")["area_ha"].sum().to_dict()

    months = pd.date_range("2017-01-01", "2025-12-01", freq="MS")
    rows = []
    for forest_type, base_area in baseline.items():
        # total cumulative loss over the period ~ 1.5-3% of baseline
        total_loss_frac = rng.uniform(0.015, 0.03)
        total_loss = base_area * total_loss_frac

        # monotonically increasing cumulative loss curve with random monthly increments
        n = len(months)
        increments = rng.exponential(scale=1.0, size=n)
        increments = increments / increments.sum() * total_loss
        cum_loss = np.cumsum(increments)

        area = base_area - cum_loss
        for m, a, cl in zip(months, area, cum_loss):
            rows.append(
                {
                    "period": m,
                    "forest_type": forest_type,
                    "area_ha": round(float(a), 2),
                    "cumulative_loss_ha": round(float(cl), 2),
                    "baseline_area_ha": round(float(base_area), 2),
                }
            )

    monthly = pd.DataFrame(rows)

    # annual = December value of each year (end-of-year snapshot)
    annual = monthly.copy()
    annual["year"] = annual["period"].dt.year
    annual = (
        annual[annual["period"].dt.month == 12]
        .drop(columns=["period"])
        .reset_index(drop=True)
    )

    return monthly, annual


def make_alerts(gdf: gpd.GeoDataFrame, monthly_cover: pd.DataFrame) -> pd.DataFrame:
    """Mock deforestation alert points, one row per detected alert.

    Each alert is placed at the centroid (with small jitter) of a randomly
    chosen polygon, dated to a month between 2017-2025, with an alert area
    (ha) and confidence score. Alert counts roughly follow the cumulative
    loss curve so charts/maps are visually consistent.
    """
    monthly_increment = (
        monthly_cover.sort_values("period")
        .groupby("forest_type")["cumulative_loss_ha"]
        .diff()
        .fillna(monthly_cover["cumulative_loss_ha"])
    )
    cover = monthly_cover.copy()
    cover["loss_increment_ha"] = monthly_increment.values

    alerts = []
    alert_id = 0
    for _, row in cover.iterrows():
        forest_type = row["forest_type"]
        period = row["period"]
        loss_ha = row["loss_increment_ha"]
        if loss_ha <= 0:
            continue

        # split the month's loss into 1-4 discrete alerts
        n_alerts = int(rng.integers(0, 4)) if loss_ha < 1 else int(rng.integers(1, 5))
        if n_alerts == 0:
            continue

        polys = gdf[gdf["forest_type"] == forest_type]
        chosen = polys.sample(n=min(n_alerts, len(polys)), replace=False, random_state=rng.integers(1e9))

        # split loss_ha across the chosen polygons
        weights = rng.dirichlet(np.ones(len(chosen)))
        for (_, poly), w in zip(chosen.iterrows(), weights):
            centroid = poly.geometry.centroid
            jitter_lon = rng.normal(0, 0.0008)
            jitter_lat = rng.normal(0, 0.0008)

            alert_area = max(0.01, float(loss_ha * w))
            confidence = float(np.clip(rng.normal(0.82, 0.1), 0.4, 0.99))
            alert_type = rng.choice(
                ["deforestation", "degradation", "spruce_encroachment"],
                p=[0.5, 0.35, 0.15],
            )

            alerts.append(
                {
                    "alert_id": alert_id,
                    "period": period,
                    "forest_type": forest_type,
                    "polygon_id": poly["polygon_id"],
                    "lon": float(centroid.x + jitter_lon),
                    "lat": float(centroid.y + jitter_lat),
                    "area_ha": round(alert_area, 3),
                    "confidence": round(confidence, 2),
                    "alert_type": str(alert_type),
                }
            )
            alert_id += 1

    return pd.DataFrame(alerts)


def main():
    print(f"reading source polygons from {SRC_GPKG}")
    gdf = load_polygons()
    write_polygons(gdf)

    monthly, annual = make_area_cover_series(gdf)
    monthly.to_parquet(OUT_DIR / "area_cover_monthly.parquet", index=False)
    annual.to_parquet(OUT_DIR / "area_cover_annual.parquet", index=False)
    print(f"wrote area_cover_monthly.parquet ({len(monthly)} rows)")
    print(f"wrote area_cover_annual.parquet ({len(annual)} rows)")

    alerts = make_alerts(gdf, monthly)
    alerts.to_parquet(OUT_DIR / "alerts.parquet", index=False)
    print(f"wrote alerts.parquet ({len(alerts)} rows)")

    # small metadata file the app can show (data provenance / freshness)
    meta = {
        "generated": pd.Timestamp.now().isoformat(),
        "data_status": "MOCK",
        "forest_types": list(LAYERS_BY_TYPE.keys()),
        "polygon_count": int(len(gdf)),
        "alert_count": int(len(alerts)),
        "period_range": [str(monthly["period"].min().date()), str(monthly["period"].max().date())],
    }
    with open(OUT_DIR / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print(f"wrote meta.json")


if __name__ == "__main__":
    main()
