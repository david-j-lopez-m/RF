"""
gdacs_fetcher.py

This script defines a fetcher class to retrieve alerts from the Global Disaster Alert and Coordination System (GDACS).
It performs the following tasks:
- Sends a GET request to the configured GDACS endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""
import os
import requests
import logging
import xml.etree.ElementTree as ET
import re
from pathlib import Path
from datetime import datetime
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class GDACSFetcher:
    """Fetcher class to retrieve GDACS alerts and store them locally."""

    def __init__(self):
        """Initialize GDACSFetcher with configuration settings.

        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("gdacs")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("gdacs")

    def fetch(self):
        """
        Fetches GDACS alerts from the configured XML RSS feed, extracts relevant fields,
        filters new alerts by event datetime, and saves them as JSON.
        Keeps a record of the last processed timestamp to avoid duplicates.
        """


        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()

            # Parse XML response
            root = ET.fromstring(r.content)
            items = root.findall(".//item")
            new_alerts = []

            for item in items:
                try:
                    # Extract main fields
                    title = item.find("title").text.strip() if item.find("title") is not None else ""
                    description = item.find("description").text.strip() if item.find("description") is not None else ""

                    # Extract event datetime (prefer gdacs:fromdate, fallback to pubDate)
                    event_dt_elem = item.find("{http://www.gdacs.org/}fromdate")
                    event_datetime = event_dt_elem.text.strip() if event_dt_elem is not None else None
                    if not event_datetime:
                        pubdate_elem = item.find("pubDate")
                        event_datetime = pubdate_elem.text.strip() if pubdate_elem is not None else None

                    # Extract magnitude from <gdacs:severity> or from text if not present
                    mag = None
                    sev_elem = item.find("{http://www.gdacs.org/}severity")
                    if sev_elem is not None and sev_elem.text:
                        mag_match = re.search(r"Magnitude\s+([0-9.]+)", sev_elem.text)
                        if mag_match:
                            mag = float(mag_match.group(1))
                    if mag is None:
                        mag_match = re.search(r"Magnitude\s+([0-9.]+)", title + " " + description)
                        mag = float(mag_match.group(1)) if mag_match else None

                    # Extract country and alert level
                    country_elem = item.find("{http://www.gdacs.org/}country")
                    country = country_elem.text.strip() if country_elem is not None else None

                    alertlevel_elem = item.find("{http://www.gdacs.org/}alertlevel")
                    alertlevel = alertlevel_elem.text.strip() if alertlevel_elem is not None else None

                    alert = {
                        "title": title,
                        "description": description,
                        "event_datetime": event_datetime,
                        "magnitude": mag,
                        "country": country,
                        "alertlevel": alertlevel
                    }
                    new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[GDACS] Skipping malformed alert: {e}")

            # Load last processed timestamp to filter duplicates
            ts_path = Path("data/alertas/gdacs_last_timestamp.txt")
            last_ts = None
            if ts_path.exists():
                try:
                    with ts_path.open("r") as f:
                        last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)
                except Exception as e:
                    logging.warning(f"[GDACS] Failed to read last timestamp: {e}")

            # Filter only new alerts by event_datetime
            filtered_alerts = []
            for alert in new_alerts:
                try:
                    if alert["event_datetime"]:
                        alert_ts = datetime.strptime(alert["event_datetime"], self.timestamp_format)
                        if not last_ts or alert_ts > last_ts:
                            filtered_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[GDACS] Skipping alert with invalid timestamp: {e}")

            # Save filtered alerts and update latest timestamp
            if filtered_alerts:
                save_json(filtered_alerts, self.output)
                logging.info(f"[GDACS] Fetched {len(filtered_alerts)} new alerts from {self.url} | Status: {status_code}")
                latest_ts = max(datetime.strptime(a["event_datetime"], self.timestamp_format) for a in filtered_alerts)
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[GDACS] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[GDACS] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(f"[GDACS] Error fetching from {self.url} | Status: {status} | Exception: {e}")