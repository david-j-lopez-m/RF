"""
USGS Earthquake Alerts Fetcher

This script defines a fetcher class to retrieve recent earthquake alerts from the USGS (United States Geological Survey)
GeoJSON feed. It performs the following tasks:
- Sends a GET request to the configured USGS endpoint.
- Parses and filters earthquake alerts based on timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""
import os
import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class USGSEarthquakeFetcher:
    """Fetcher class to retrieve USGS earthquake alerts and store them locally."""

    def __init__(self):
        """Initialize USGSEarthquakeFetcher with configuration settings.
        
        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("usgs_earthquakes")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("usgs_earthquakes")

    def fetch(self):
        """Fetch USGS earthquake alerts and save new entries to a JSON file.
        
        Handles HTTP request errors and logs status messages.
        """
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            ts_path = Path("data/alertas/usgs_earthquakes_last_timestamp.txt")
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)

            # Filter new alerts
            new_alerts = []
            for feature in data.get("features", []):
                try:
                    alert_ts = datetime.fromtimestamp(feature["properties"]["time"] / 1000.0, tz=timezone.utc)
                    if not last_ts or alert_ts > last_ts:
                        new_alerts.append(feature)
                except Exception as e:
                    logging.warning(f"[USGS] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                save_json(new_alerts, self.output)
                logging.info(
                    f"[USGS] Fetched {len(new_alerts)} new alerts from {self.url} | Status: {status_code}"
                )

                # Save latest timestamp
                latest_ts = max(
                    datetime.fromtimestamp(alert["properties"]["time"] / 1000.0, tz=timezone.utc)
                    for alert in new_alerts
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[USGS] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[USGS] No new alerts to save from {self.url}")
        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[USGS] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )