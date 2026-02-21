const map = L.map("map", { zoomControl: true });

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 18,
}).addTo(map);

const statsEl = document.getElementById("stats");

function formatValue(value) {
  if (value === null || value === undefined) return "N/A";
  if (typeof value === "number") return Number.isFinite(value) ? value.toString() : "N/A";
  const text = String(value).trim();
  if (text === "" || text.toLowerCase() === "n/a") return "N/A";
  return text;
}

function row(label, value) {
  const formatted = formatValue(value);
  if (formatted === "N/A") return "";
  return `
    <div class="row">
      <span class="label">${label}</span>
      <span class="value">${formatted}</span>
    </div>
  `;
}

function buildPopup(props) {
  const title = formatValue(props.well_name || props.api || "Well");
  const url = formatValue(props.web_source_url);
  const urlRow =
    url !== "N/A"
      ? `<a href="${url}" target="_blank" rel="noopener">Source</a>`
      : "N/A";

  return `
    <div class="popup">
      <h3>${title}</h3>

      <div class="section">
        <strong>Well Information</strong>
        ${row("API", props.api)}
        ${row("Operator", props.operator)}
        ${row("County", props.county)}
        ${row("State", props.state)}
        ${row("Latitude (decimal)", props.latitude_decimal)}
        ${row("Longitude (decimal)", props.longitude_decimal)}
      </div>

      <div class="section">
        <strong>Stimulation</strong>
        ${row("Date", props.date_stimulated)}
        ${row("Formation", props.stimulated_formation)}
        ${row("Stages", props.stimulation_stages)}
        ${row("Top (ft)", props.top_ft)}
        ${row("Bottom (ft)", props.bottom_ft)}
        ${row("Volume", props.volume)}
        ${row("Proppant (lbs)", props.lbs_proppant)}
      </div>

      <div class="section">
        <strong>Web Data</strong>
        ${row("Status", props.web_well_status)}
        ${row("Type", props.web_well_type)}
        ${row("Closest City", props.web_closest_city)}
        ${row("Oil (bbls)", props.web_oil_bbls)}
        ${row("Oil Month", props.web_oil_prod_month)}
        ${row("Gas (mcf)", props.web_gas_mcf)}
        ${row("Gas Month", props.web_gas_prod_month)}
        ${row("Source", urlRow)}
      </div>
    </div>
  `;
}

function updateStats(featureCount) {
  statsEl.textContent = `Loaded ${featureCount} wells with valid coordinates.`;
}

fetch("data/wells.geojson")
  .then((response) => {
    if (!response.ok) {
      throw new Error("Failed to load GeoJSON");
    }
    return response.json();
  })
  .then((data) => {
    const markers = L.geoJSON(data, {
      pointToLayer: (feature, latlng) => {
        const status = (feature.properties.web_well_status || "").toLowerCase();

        let fill = "#999";
        if (status.includes("active")) fill = "#2e8b57";
        else if (status.includes("plugged")) fill = "#b22222";

        return L.circleMarker(latlng, {
          radius: 6,
          weight: 1,
          color: fill,
          fillColor: fill,
          fillOpacity: 0.85,
        });
      },
      onEachFeature: (feature, layer) => {
        layer.bindPopup(buildPopup(feature.properties));
      },
    }).addTo(map);

    if (markers.getBounds().isValid()) {
      map.fitBounds(markers.getBounds(), { padding: [24, 24] });
    } else {
      map.setView([47.5, -102.9], 6);
    }

    updateStats(markers.getLayers().length);
  })
  .catch((error) => {
    statsEl.textContent = `Failed to load data: ${error.message}`;
    map.setView([47.5, -102.9], 6);
  });