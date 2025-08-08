"""
USGS Earthquake Alerts Fetcher

This script defines a fetcher class to retrieve recent earthquake alerts from the USGS (United States Geological Survey)
GeoJSON feed. It performs the following tasks:
- Sends a GET request to the configured USGS endpoint.
- Extracts and structures key earthquake fields (datetime, location, magnitude, depth, etc.).
- Saves structured alerts in JSON format to a local file.
- Logs all relevant events including success and failure.
- Intended to be called from a main controller script managing multiple data sources.
"""

import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from utils import save_json
from config import get_source_config, get_timestamp_format

class USGSEarthquakeFetcher:
    """Fetcher class to retrieve USGS earthquake alerts and store them locally."""

    def __init__(self, config=None, source_key="usgs_earthquakes"):
        """Initialize USGSEarthquakeFetcher with configuration.
            
            Args:
                config (dict): Optional global config dictionary. If None, it will use get_source_config.
                source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url = self.config["url"]
        self.base_path = self.config["base_data_path"] 
        self.output = self.config["output_filename"]
        self.timestamp_format = get_timestamp_format(source_key)
        self.unique_key = self.config.get("unique_key")

    def fetch(self, incremental: bool = True):
        """Fetch USGS earthquake alerts and save structured entries to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Parse all alerts
            alerts = []
            for feature in data.get("features", []):
                try:
                    parsed = self.parse_feature(feature)
                    alerts.append(parsed)
                except Exception as e:
                    logging.warning(f"[USGS] Skipping alert due to parsing error: {e}")

            if alerts:
                full_output_path = Path(self.base_path) / self.output
                save_json(alerts, full_output_path, unique_key=self.unique_key)
                logging.info(
                    f"[USGS] Fetched and saved {len(alerts)} structured alerts from {self.url} | Status: {status_code}"
                )

                if incremental:
                    # Also save (overwrite) an incremental JSON of new alerts
                    incremental_dir = Path(self.config.get("incremental_output_dir", self.base_path)) / "incremental"
                    incremental_dir.mkdir(parents=True, exist_ok=True)
                    # Use the same output filename to overwrite previous incremental file
                    inc_file = incremental_dir / self.output
                    save_json(alerts, inc_file)
                    logging.info(f"[USGS] Overwrote incremental alerts file {inc_file.name}")

            else:
                logging.info(f"[USGS] No alerts to save from {self.url}")
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