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
from config import get_source_config, get_timestamp_format

class GDACSFetcher:
    """Fetcher class to retrieve GDACS alerts and store them locally."""

    def __init__(self, config=None, source_key="gdacs"):
        """Initialize GDACSFetcher with configuration.
            
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


    def fetch(self):
        """
        Fetches GDACS alerts from the configured XML RSS feed, extracts relevant fields,
        and saves them as JSON. Deduplication is handled by save_json via unique_key.
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

            # Save all alerts (deduplication by unique_key in save_json)
            if new_alerts:
                full_output_path = Path(self.base_path) / self.output
                save_json(new_alerts, full_output_path, unique_key=self.unique_key)
                logging.info(f"[GDACS] Fetched {len(new_alerts)} alerts from {self.url} | Status: {status_code}")
            else:
                logging.info(f"[GDACS] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(f"[GDACS] Error fetching from {self.url} | Status: {status} | Exception: {e}")