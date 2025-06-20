"""
meteoalarm_fetcher.py

This script defines a fetcher class to retrieve alerts from Meteoalarm.
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
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class MeteoalarmFetcher:
    """Fetcher class to retrieve Meteoalarm alerts and store them locally."""

    def __init__(self):
        """Initialize MeteoalarmFetcher with configuration settings.

        Retrieves the URL and output filename from the source configuration.
        """
        self.config = get_source_config("meteoalarm")
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("meteoalarm")

    def fetch(self):
        """Fetch Meteoalarm alerts from the configured URL and save them to a JSON file.

        Handles HTTP request errors and logs status messages. Parses XML RSS feed
        and filters alerts based on their publication date to avoid duplicates.
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
                    title = item.find("title").text
                    description = item.find("description").text
                    pub_date = item.find("pubDate").text
                    link = item.find("link").text
                    guid = item.find("guid").text

                    alert = {
                        "title": title,
                        "description": description,
                        "pubDate": pub_date,
                        "link": link,
                        "guid": guid
                    }
                    new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[METEOALARM] Skipping malformed alert element: {e}")

            # Load last processed timestamp
            ts_path = Path("data/alertas/meteoalarm_last_timestamp.txt")
            last_ts = None
            if ts_path.exists():
                try:
                    with ts_path.open("r") as f:
                        last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)
                except Exception as e:
                    logging.warning(f"[METEOALARM] Failed to read last timestamp: {e}")

            # Filter new alerts
            filtered_alerts = []
            for alert in new_alerts:
                try:
                    alert_ts = datetime.strptime(alert["pubDate"], self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
                        filtered_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[METEOALARM] Skipping alert with invalid timestamp: {e}")

            # Save filtered alerts
            if filtered_alerts:
                save_json(filtered_alerts, self.output)
                logging.info(f"[METEOALARM] Fetched {len(filtered_alerts)} new alerts from {self.url} | Status: {response.status_code}")

                # Save latest timestamp
                latest_ts = max(
                    datetime.strptime(alert["pubDate"], self.timestamp_format)
                    for alert in filtered_alerts
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[METEOALARM] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[METEOALARM] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[METEOALARM] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )