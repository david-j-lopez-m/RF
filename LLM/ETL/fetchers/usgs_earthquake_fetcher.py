"""
USGS Earthquake Alerts Fetcher

This script defines a fetcher class to retrieve recent earthquake alerts from the USGS (United States Geological Survey)
GeoJSON feed. It performs the following tasks:
- Sends a GET request to the configured USGS endpoint.
- Parses and filters earthquake alerts based on timestamp to avoid duplicates.
- Extracts and structures key earthquake fields (datetime, location, magnitude, depth, etc.).
- Saves new structured alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""

import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class USGSEarthquakeFetcher:
    """Fetcher class to retrieve USGS earthquake alerts and store them locally."""

    def __init__(self):
        """Initialize USGSEarthquakeFetcher with configuration settings."""
        self.config = get_source_config("usgs_earthquakes")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("usgs_earthquakes")

    def fetch(self):
        """Fetch USGS earthquake alerts and save new structured entries to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            ts_path = Path(self.config.get("last_timestamp_path", "data/alertas/usgs_earthquakes_last_timestamp.txt"))
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

            # Filter and parse new alerts
            new_alerts = []
            for feature in data.get("features", []):
                try:
                    alert_ts = datetime.fromtimestamp(feature["properties"]["time"] / 1000.0, tz=timezone.utc)
                    if not last_ts or alert_ts > last_ts:
                        parsed = self.parse_feature(feature)
                        new_alerts.append(parsed)
                except Exception as e:
                    logging.warning(f"[USGS] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                save_json(new_alerts, self.output)
                logging.info(
                    f"[USGS] Fetched {len(new_alerts)} new structured alerts from {self.url} | Status: {status_code}"
                )

                # Save latest timestamp
                latest_ts = max(
                    datetime.fromtimestamp(feature["properties"]["time"] / 1000.0, tz=timezone.utc)
                    for feature in data.get("features", [])
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                # Cuando guardas el latest_ts
                ts_path.write_text(latest_ts.strftime("%Y-%m-%dT%H:%M:%SZ"))  # ISO 8601 UTC
                logging.info(f"[USGS] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[USGS] No new alerts to save from {self.url}")
        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[USGS] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )


    @staticmethod
    def parse_feature(feature):
        """
        Extract relevant fields from a USGS earthquake feature for structured storage.

        Args:
            feature (dict): A GeoJSON feature from the USGS API.

        Returns:
            dict: Parsed fields (event_datetime, place, magnitude, depth, latitude, longitude, url, etc.)
        """
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [None, None, None])

        event_datetime = None
        if props.get("time") is not None:
            event_datetime = datetime.fromtimestamp(props["time"] / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z")

        return {
            "event_datetime": event_datetime,
            "place": props.get("place"),
            "magnitude": props.get("mag"),
            "depth_km": coords[2] if len(coords) > 2 else None,
            "latitude": coords[1] if len(coords) > 1 else None,
            "longitude": coords[0] if len(coords) > 0 else None,
            "event_type": props.get("type"),
            "status": props.get("status"),
            "tsunami": props.get("tsunami"),
            "url": props.get("url"),
            "title": props.get("title"),
            "code": props.get("code"),
            "ids": props.get("ids"),
        }