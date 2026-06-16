import "./style.css";
import { getDb, registerParquet, query } from "./duck.js";
import { createMap, setForestPolygons, setAlertPoints } from "./map.js";
import { renderCoverChart, renderAlertsChart } from "./charts.js";

const DATA_BASE = `${import.meta.env.BASE_URL}data`;

const state = {
  forestType: "all",
  alertType: "all",
  dateFrom: "2017-01",
  dateTo: "2025-12",
  coverResolution: "annual",
};

let map;
let polygons;
let alerts;
let coverMonthly;
let coverAnnual;
let meta;

async function loadData() {
  await getDb();

  await registerParquet(`${DATA_BASE}/alerts.parquet`, "alerts.parquet");
  await registerParquet(`${DATA_BASE}/area_cover_monthly.parquet`, "area_cover_monthly.parquet");
  await registerParquet(`${DATA_BASE}/area_cover_annual.parquet`, "area_cover_annual.parquet");

  alerts = await query(`SELECT * FROM 'alerts.parquet' ORDER BY period`);
  coverMonthly = await query(`SELECT * FROM 'area_cover_monthly.parquet' ORDER BY period, forest_type`);
  coverAnnual = await query(`SELECT * FROM 'area_cover_annual.parquet' ORDER BY year, forest_type`);

  // normalize timestamps to ISO strings (Arrow may return bigint millis or Date)
  for (const row of alerts) row.period = toIsoMonth(row.period);
  for (const row of coverMonthly) row.period = toIsoMonth(row.period);

  polygons = await fetch(`${DATA_BASE}/forest_polygons.geojson`).then((r) => r.json());

  meta = await fetch(`${DATA_BASE}/meta.json`).then((r) => r.json());
}

function toIsoMonth(value) {
  let date;
  if (value instanceof Date) {
    date = value;
  } else if (typeof value === "bigint") {
    date = new Date(Number(value));
  } else {
    date = new Date(value);
  }
  return date.toISOString().slice(0, 7); // YYYY-MM
}

function setupStatus() {
  const el = document.getElementById("data-status");
  if (meta.data_status === "MOCK") {
    el.textContent = `Mock data — generated ${new Date(meta.generated).toLocaleString()}`;
    el.classList.add("mock");
  } else {
    el.textContent = `Live data — updated ${new Date(meta.generated).toLocaleString()}`;
  }

  const footer = document.getElementById("footer-note");
  footer.textContent =
    `${meta.polygon_count} mapped polygons · ${meta.alert_count} alerts · ` +
    `period ${meta.period_range[0]} to ${meta.period_range[1]}. ` +
    (meta.data_status === "MOCK"
      ? "All alert and time-series figures are MOCK placeholders pending real pipeline output."
      : "");
}

function setupControls() {
  document.getElementById("filter-forest-type").addEventListener("change", (e) => {
    state.forestType = e.target.value;
    refresh();
  });
  document.getElementById("filter-alert-type").addEventListener("change", (e) => {
    state.alertType = e.target.value;
    refresh();
  });
  document.getElementById("filter-date-from").addEventListener("change", (e) => {
    state.dateFrom = e.target.value;
    refresh();
  });
  document.getElementById("filter-date-to").addEventListener("change", (e) => {
    state.dateTo = e.target.value;
    refresh();
  });
  document.getElementById("cover-resolution").addEventListener("change", (e) => {
    state.coverResolution = e.target.value;
    refresh();
  });
}

function filterAlerts() {
  return alerts.filter((a) => {
    if (state.forestType !== "all" && a.forest_type !== state.forestType) return false;
    if (state.alertType !== "all" && a.alert_type !== state.alertType) return false;
    if (a.period < state.dateFrom || a.period > state.dateTo) return false;
    return true;
  });
}

function filterPolygons() {
  if (state.forestType === "all") return polygons;
  return {
    ...polygons,
    features: polygons.features.filter((f) => f.properties.forest_type === state.forestType),
  };
}

function filterCover() {
  const data = state.coverResolution === "annual" ? coverAnnual : coverMonthly;
  return data.filter((d) => {
    if (state.forestType !== "all" && d.forest_type !== state.forestType) return false;
    const period = state.coverResolution === "annual" ? `${d.year}-12` : d.period;
    if (period < state.dateFrom || period > state.dateTo) return false;
    return true;
  });
}

function alertsToGeojson(rows) {
  return {
    type: "FeatureCollection",
    features: rows.map((r) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [r.lon, r.lat] },
      properties: r,
    })),
  };
}

function renderSummary(filteredAlerts, filteredCover) {
  const container = document.getElementById("summary-cards");
  container.innerHTML = "";

  const totalAlertArea = filteredAlerts.reduce((sum, a) => sum + a.area_ha, 0);
  const byType = {};
  for (const a of filteredAlerts) {
    byType[a.forest_type] = (byType[a.forest_type] || 0) + a.area_ha;
  }

  const latestByType = {};
  for (const row of filteredCover) {
    const key = row.forest_type;
    const period = state.coverResolution === "annual" ? row.year : row.period;
    if (!latestByType[key] || period > latestByType[key].period) {
      latestByType[key] = { period, area_ha: row.area_ha };
    }
  }

  const cards = [
    {
      label: "Total alerts",
      value: filteredAlerts.length.toLocaleString(),
      sub: `${totalAlertArea.toFixed(1)} ha affected`,
    },
    {
      label: "Olivinskog cover (latest)",
      value: latestByType.olivinskog ? `${latestByType.olivinskog.area_ha.toFixed(1)} ha` : "—",
      sub: latestByType.olivinskog ? `as of ${latestByType.olivinskog.period}` : "",
    },
    {
      label: "Kalklindeskog cover (latest)",
      value: latestByType.kalklindeskog ? `${latestByType.kalklindeskog.area_ha.toFixed(1)} ha` : "—",
      sub: latestByType.kalklindeskog ? `as of ${latestByType.kalklindeskog.period}` : "",
    },
    {
      label: "Alert area by type",
      value: Object.entries(byType)
        .map(([k, v]) => `${k}: ${v.toFixed(1)} ha`)
        .join(" / ") || "—",
      sub: "selected filters",
    },
  ];

  for (const card of cards) {
    const div = document.createElement("div");
    div.className = "summary-card";
    div.innerHTML = `
      <div class="summary-card__label">${card.label}</div>
      <div class="summary-card__value">${card.value}</div>
      <div class="summary-card__sub">${card.sub}</div>
    `;
    container.append(div);
  }
}

function refresh() {
  const filteredAlerts = filterAlerts();
  const filteredCover = filterCover();

  setAlertPoints(map, alertsToGeojson(filteredAlerts));
  setForestPolygons(map, filterPolygons(), { fit: state.forestType !== "all" });

  renderCoverChart(document.getElementById("chart-cover"), filteredCover);
  renderAlertsChart(document.getElementById("chart-alerts"), filteredAlerts);

  renderSummary(filteredAlerts, filteredCover);
}

async function init() {
  await loadData();
  setupStatus();
  setupControls();

  map = createMap(document.getElementById("map"));
  await setForestPolygons(map, filterPolygons(), { fit: true });

  refresh();

  window.addEventListener("resize", () => {
    renderCoverChart(document.getElementById("chart-cover"), filterCover());
    renderAlertsChart(document.getElementById("chart-alerts"), filterAlerts());
  });
}

init().catch((err) => {
  console.error(err);
  document.getElementById("data-status").textContent = `Error loading data: ${err.message}`;
});
