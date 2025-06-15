"""
noaa_fetcher.py

This script defines a fetcher class to retrieve space weather alerts from NOAA SWPC (Space Weather Prediction Center).
It performs the following tasks:
- Sends a GET request to the configured NOAA SWPC endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from config import get_source_config, get_source_timestamp_format
from utils import save_json
import requests

class NOAAFetcher:
    """Fetcher class to retrieve NOAA space weather alerts and save them locally."""

    def __init__(self):
        """Initialize NOAAFetcher with configuration settings.

        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("noaa_swpc")
        self.url = self.config.get("url")
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("noaa_swpc")

    def fetch(self):
        """Fetch NOAA alerts from the configured URL and save them to a JSON file.

        Handles HTTP request errors and logs status messages.
        """
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            ts_path = Path(self.config.get("last_timestamp_path", "data/alertas/noaa_last_timestamp.txt"))
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format + " UTC")

            # Filter new alerts
            new_alerts = []
            for alert in data:
                try:
                    alert_ts = datetime.strptime(alert["issue_datetime"], self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
                        new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[NOAA] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                save_json(new_alerts, self.output)
                logging.info(
                    f"[NOAA] Fetched {len(new_alerts)} new alerts from {self.url} | Status: {status_code}"
                )

                # Save latest timestamp
                latest_ts = max(
                    datetime.strptime(alert["issue_datetime"], self.timestamp_format)
                    for alert in new_alerts
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format + " UTC"))
                logging.info(f"[NOAA] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[NOAA] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[NOAA] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )