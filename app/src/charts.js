import * as Plot from "@observablehq/plot";

const FOREST_COLORS = {
  olivinskog: "#6aa84f",
  kalklindeskog: "#d6b656",
};

const ALERT_COLORS = {
  deforestation: "#e06666",
  degradation: "#f6b26b",
  spruce_encroachment: "#9966cc",
};

function toDate(row) {
  return row.period instanceof Date ? row.period : new Date(row.period);
}

export function renderCoverChart(container, data) {
  container.innerHTML = "";

  const prepared = data.map((d) => ({
    ...d,
    date: toDate(d),
  }));

  const plot = Plot.plot({
    width: container.clientWidth || 480,
    height: 240,
    marginLeft: 55,
    y: {
      label: "Area (ha)",
      grid: true,
    },
    x: {
      label: null,
    },
    color: {
      domain: ["olivinskog", "kalklindeskog"],
      range: [FOREST_COLORS.olivinskog, FOREST_COLORS.kalklindeskog],
      legend: true,
    },
    marks: [
      Plot.lineY(prepared, {
        x: "date",
        y: "area_ha",
        stroke: "forest_type",
        strokeWidth: 2,
        tip: true,
      }),
      Plot.dot(prepared, {
        x: "date",
        y: "area_ha",
        fill: "forest_type",
        r: 2.5,
      }),
      Plot.ruleY([0]),
    ],
  });

  container.append(plot);
}

export function renderAlertsChart(container, data) {
  container.innerHTML = "";

  const prepared = data.map((d) => ({
    ...d,
    date: toDate(d),
  }));

  const plot = Plot.plot({
    width: container.clientWidth || 480,
    height: 240,
    marginLeft: 55,
    y: {
      label: "Alert area (ha)",
      grid: true,
    },
    x: {
      label: null,
    },
    color: {
      domain: ["deforestation", "degradation", "spruce_encroachment"],
      range: [ALERT_COLORS.deforestation, ALERT_COLORS.degradation, ALERT_COLORS.spruce_encroachment],
      legend: true,
    },
    marks: [
      Plot.barY(prepared, {
        x: "date",
        y: "area_ha",
        fill: "alert_type",
        tip: true,
      }),
      Plot.ruleY([0]),
    ],
  });

  container.append(plot);
}
