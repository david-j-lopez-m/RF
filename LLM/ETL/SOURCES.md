# Data Sources for Scientific and Civil Alert Feeds

This document describes the data sources used for retrieving meteorological, geophysical, and space weather alerts for analysis and training purposes.

---

## 1. NOAA SWPC – Space Weather Prediction Center

- **URL**: https://services.swpc.noaa.gov/json/alerts.json
- **Type**: Solar activity, geomagnetic storms, HF communication blackouts
- **Format**: JSON
- **Update Frequency**: Real-time / every few minutes
- **Authentication**: Not required
- **Notes**: Alerts contain levels (e.g., G1 to G5) and timestamps. Useful for both real-time alerting and LLM training.

---

## 2. NASA DONKI – Database of Notifications for Kinematic Interactions

- **URL**: https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/getAllNotifications
- **Type**: Solar flares, CMEs (Coronal Mass Ejections), SEPs (Solar Energetic Particles)
- **Format**: JSON
- **Authentication**: Not required
- **Notes**: Historical and real-time event records. Requires filtering by event type. Excellent for contextual training.

---

## 3. USGS Earthquake Alerts

- **URL**: https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
- **Type**: Global earthquakes
- **Format**: GeoJSON
- **Update Frequency**: Real-time
- **Authentication**: Not required
- **Notes**: Magnitude, location, depth, and timestamp provided. High quality data source for natural hazard monitoring.

---

## 4. AEMET – Spanish State Meteorological Agency

- **URL**: https://opendata.aemet.es/
- **Type**: Adverse weather alerts (storms, heatwaves, snow, wind, etc.)
- **Format**: JSON / XML
- **Authentication**: ✅ Required (free API key registration)
- **Notes**: Data available by region (autonomous communities and provinces). Includes level of risk (green, yellow, orange, red).

---

## 5. IGN – Spanish Geographical Institute

- **URL**: https://www.ign.es/resources/sismologia/mov-sis/ultimos-terremotos.json
- **Type**: Earthquakes in Spanish territory
- **Format**: JSON
- **Authentication**: Not required
- **Notes**: Basic structured format. Includes magnitude, depth, and location.

---

## 6. Meteoalarm (EU, extended) — *Deprecated in current prototype*

- **URL**: https://feeds.meteoalarm.org/rss/spain *(legacy feed, not used)*
- **Type**: Weather + civil protection (e.g. flood, fire, health risk)
- **Format**: RSS/XML, OGC API (GeoJSON, MQTT)
- **Auth**: Not required for RSS; OGC API archive available via REST, real-time requires MQTT and token
- **Status**: **Currently disabled in pipeline**
- **Notes**:  
    - Meteoalarm covers all EU but the RSS feed and archive API are not suitable for real-time alerting in Spain (due to delay and redundancy with AEMET).  
    - The new OGC API only exposes real-time data via MQTT, which is not implemented in this project prototype.  
    - May be re-enabled in the future for cross-border, pan-European, or historic alert analysis if scope is expanded.

---

## 7. GDACS – Global Disaster Alert and Coordination System

- **URL**: https://www.gdacs.org/rss.aspx
- **Type**: Global civil disasters (earthquake, tsunami, tropical cyclones, floods)
- **Format**: RSS/XML
- **Auth**: Not required
- **Notes**: Good for training the LLM with international alert variety and standard event categorization.

---

## 8. FIRMS – Fire Information for Resource Management System (NASA)

- **URL**: https://firms.modaps.eosdis.nasa.gov/
- **Type**: Active fires detected by satellite (MODIS, VIIRS)
- **Format**: CSV (converted to JSON)
- **Authentication**: ✅ Required (API Key from NASA Earthdata)
- **Notes**: Global fire alerts with satellite-derived timestamps, location, and fire radiative power. Useful for real-time wildfire monitoring and LLM training.