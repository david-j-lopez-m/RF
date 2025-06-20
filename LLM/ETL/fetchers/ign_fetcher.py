"""
ign_fetcher.py

This script defines a fetcher class to retrieve alerts from Spain's Instituto Geográfico Nacional (IGN).
It performs the following tasks:
- Sends a GET request to the configured IGN endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""
import os
import requests
import logging
import re
from pathlib import Path
from datetime import datetime
import xml.etree.ElementTree as ET
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class IGNFetcher:
    """Fetcher class to retrieve IGN seismic alerts and store them locally."""

    def __init__(self):
        """Initialize IGNFetcher with configuration settings.

        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("ign")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("ign")

    def fetch(self):
        """
        Fetch IGN alerts from the configured URL and save them to a JSON file.

        - Performs an HTTP request to download the XML RSS feed.
        - Parses alert elements, extracting relevant fields (title, description, event_datetime, magnitude, location).
        - Filters alerts based on their event_datetime to avoid duplicates.
        - Saves only new alerts in JSON format.
        - Logs all significant events and saves the most recent timestamp for next run.
        """

        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            channel = root.find("channel")
            items = channel.findall("item") if channel is not None else []

            new_alerts = []
            for item in items:
                try:
                    title = item.find("title").text.strip()
                    description = item.find("description").text.strip()

                    # Parse description for magnitude, location, datetime
                    mag_match = re.search(r"magnitud (\d+(?:\.\d+)?)", description)
                    loc_match = re.search(r"en (.*?) en la fecha", description)
                    fecha_match = re.search(r"fecha ([\d/ :]+) en la siguiente", description)
                    magnitude = float(mag_match.group(1)) if mag_match else None
                    location = loc_match.group(1) if loc_match else None
                    event_datetime = fecha_match.group(1) if fecha_match else None

                    alert = {
                        "title": title,
                        "description": description,
                        "event_datetime": event_datetime,
                        "magnitude": magnitude,
                        "location": location
                    }
                    new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[IGN] Skipping malformed alert element: {e}")

            # Load last processed timestamp
            ts_path = Path("data/alertas/ign_last_timestamp.txt")
            last_ts = None
            if ts_path.exists():
                try:
                    with ts_path.open("r") as f:
                        last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)
                except Exception as e:
                    logging.warning(f"[IGN] Failed to read last timestamp: {e}")

            # Filter new alerts (by event_datetime)
            filtered_alerts = []
            for alert in new_alerts:
                try:
                    if alert["event_datetime"]:
                        alert_ts = datetime.strptime(alert["event_datetime"], "%d/%m/%Y %H:%M:%S")
                        if not last_ts or alert_ts > last_ts:
                            filtered_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[IGN] Skipping alert with invalid timestamp: {e}")

            # Save filtered alerts
            if filtered_alerts:
                save_json(filtered_alerts, self.output)
                logging.info(f"[IGN] Fetched {len(filtered_alerts)} new alerts from {self.url} | Status: {response.status_code}")

                # Save latest timestamp
                latest_ts = max(datetime.strptime(alert["event_datetime"], "%d/%m/%Y %H:%M:%S") for alert in filtered_alerts)
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[IGN] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[IGN] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[IGN] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )