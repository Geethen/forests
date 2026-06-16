import maplibregl from "maplibre-gl";

const FOREST_COLORS = {
  olivinskog: "#6aa84f",
  kalklindeskog: "#d6b656",
};

const ALERT_COLORS = {
  deforestation: "#e06666",
  degradation: "#f6b26b",
  spruce_encroachment: "#9966cc",
};

export function createMap(container) {
  const map = new maplibregl.Map({
    container,
    style: {
      version: 8,
      sources: {
        osm: {
          type: "raster",
          tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
          tileSize: 256,
          attribution: "&copy; OpenStreetMap contributors",
        },
      },
      layers: [{ id: "osm", type: "raster", source: "osm" }],
    },
    center: [9.5, 60.5],
    zoom: 5.5,
  });
  map.addControl(new maplibregl.NavigationControl(), "top-right");
  return map;
}

export async function setForestPolygons(map, geojson, { fit = false } = {}) {
  await waitForLoad(map);

  if (map.getSource("forest-polygons")) {
    map.getSource("forest-polygons").setData(geojson);
    if (fit) fitToGeojson(map, geojson);
    return;
  }

  map.addSource("forest-polygons", {
    type: "geojson",
    data: geojson,
  });

  map.addLayer({
    id: "forest-fill",
    type: "fill",
    source: "forest-polygons",
    paint: {
      "fill-color": [
        "match",
        ["get", "forest_type"],
        "olivinskog",
        FOREST_COLORS.olivinskog,
        "kalklindeskog",
        FOREST_COLORS.kalklindeskog,
        "#999999",
      ],
      "fill-opacity": 0.35,
    },
  });

  map.addLayer({
    id: "forest-outline",
    type: "line",
    source: "forest-polygons",
    paint: {
      "line-color": [
        "match",
        ["get", "forest_type"],
        "olivinskog",
        FOREST_COLORS.olivinskog,
        "kalklindeskog",
        FOREST_COLORS.kalklindeskog,
        "#999999",
      ],
      "line-width": 1,
      "line-opacity": 0.8,
    },
  });

  fitToGeojson(map, geojson);

  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
  map.on("mousemove", "forest-fill", (e) => {
    map.getCanvas().style.cursor = "pointer";
    const f = e.features[0];
    popup
      .setLngLat(e.lngLat)
      .setHTML(
        `<strong>${f.properties.forest_type}</strong><br/>` +
          `${f.properties.source_layer}<br/>` +
          `${Number(f.properties.area_ha).toFixed(2)} ha`
      )
      .addTo(map);
  });
  map.on("mouseleave", "forest-fill", () => {
    map.getCanvas().style.cursor = "";
    popup.remove();
  });
}

export async function setAlertPoints(map, geojson) {
  await waitForLoad(map);

  if (map.getSource("alert-points")) {
    map.getSource("alert-points").setData(geojson);
    return;
  }

  map.addSource("alert-points", {
    type: "geojson",
    data: geojson,
  });

  map.addLayer({
    id: "alert-points",
    type: "circle",
    source: "alert-points",
    paint: {
      "circle-color": [
        "match",
        ["get", "alert_type"],
        "deforestation",
        ALERT_COLORS.deforestation,
        "degradation",
        ALERT_COLORS.degradation,
        "spruce_encroachment",
        ALERT_COLORS.spruce_encroachment,
        "#e06666",
      ],
      "circle-radius": [
        "interpolate",
        ["linear"],
        ["get", "area_ha"],
        0, 3,
        1, 5,
        5, 9,
        20, 16,
      ],
      "circle-opacity": 0.75,
      "circle-stroke-color": "#fff",
      "circle-stroke-width": 0.5,
    },
  });

  const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
  map.on("mousemove", "alert-points", (e) => {
    map.getCanvas().style.cursor = "pointer";
    const f = e.features[0];
    const p = f.properties;
    popup
      .setLngLat(e.lngLat)
      .setHTML(
        `<strong>${p.alert_type.replace("_", " ")}</strong><br/>` +
          `${p.forest_type} &mdash; ${p.period}<br/>` +
          `${Number(p.area_ha).toFixed(2)} ha, confidence ${Number(p.confidence).toFixed(2)}`
      )
      .addTo(map);
  });
  map.on("mouseleave", "alert-points", () => {
    map.getCanvas().style.cursor = "";
    popup.remove();
  });
}

function fitToGeojson(map, geojson) {
  const bounds = new maplibregl.LngLatBounds();
  for (const feature of geojson.features) {
    extendBoundsWithGeometry(bounds, feature.geometry);
  }
  if (!bounds.isEmpty()) {
    map.fitBounds(bounds, { padding: 30, maxZoom: 12 });
  }
}

function waitForLoad(map) {
  if (map.loaded()) return Promise.resolve();
  return new Promise((resolve) => map.once("load", resolve));
}

function extendBoundsWithGeometry(bounds, geometry) {
  const extend = (coords) => {
    if (typeof coords[0] === "number") {
      bounds.extend(coords);
    } else {
      coords.forEach(extend);
    }
  };
  extend(geometry.coordinates);
}
