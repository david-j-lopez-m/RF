"""
nasa_donki_fetcher.py

This script defines a fetcher class to retrieve alerts from NASA's DONKI (Database Of Notifications, Knowledge, Information).
It performs the following tasks:
- Sends a GET request to the configured NASA DONKI endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""
import os
import requests
import logging
from pathlib import Path
from datetime import datetime
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class NASADONKIFetcher:
    """Fetcher class to retrieve NASA DONKI space weather alerts and store them locally."""

    def __init__(self):
        """Initialize NASADONKIFetcher with configuration settings.

        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("nasa_donki")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("nasa_donki")

    def fetch(self):
        """Fetch NASA DONKI alerts from the configured URL and save them to a JSON file.

        Handles HTTP request errors and logs status messages.
        """
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            ts_path = Path("data/alertas/nasa_donki_last_timestamp.txt")
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)

            # Filter new alerts
            new_alerts = []
            for alert in data:
                try:
                    alert_ts = datetime.strptime(alert["messageIssueTime"], self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
                        new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[DONKI] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                save_json(new_alerts, self.output)
                logging.info(
                    f"[DONKI] Fetched {len(new_alerts)} new alerts from {self.url} | Status: {status_code}"
                    )

                # Save latest timestamp
                latest_ts = max(
                    datetime.strptime(alert["messageIssueTime"], self.timestamp_format)
                    for alert in new_alerts
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[DONKI] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[DONKI] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[DONKI] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )