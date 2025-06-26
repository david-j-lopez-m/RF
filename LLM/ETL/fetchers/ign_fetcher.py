"""
ign_fetcher.py

This script defines a fetcher class to retrieve alerts from Spain's Instituto Geográfico Nacional (IGN).
It performs the following tasks:
- Sends a GET request to the configured IGN endpoint.
- Parses all alerts without filtering.
- Saves all parsed alerts in JSON format to a local file.
- Logs all relevant events including success and failure.

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
from config import get_source_config, get_timestamp_format

class IGNFetcher:
    """Fetcher class to retrieve IGN seismic alerts and store them locally."""

    def __init__(self, config=None, source_key="ign"):
        """Initialize IGNFetcher with configuration.
            
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
        Fetch IGN alerts from the configured URL and save them to a JSON file.

        - Performs an HTTP request to download the XML RSS feed.
        - Parses alert elements, extracting relevant fields (title, description, event_datetime, magnitude, location).
        - Saves all parsed alerts in JSON format without filtering.
        - Logs all significant events.
        """

        try:
            response = requests.get(self.url, timeout=10)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            channel = root.find("channel")
            items = channel.findall("item") if channel is not None else []

            alerts = []
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
                    alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[IGN] Skipping malformed alert element: {e}")

            # Save all parsed alerts
            if alerts:
                full_output_path = Path(self.base_path) / self.output
                save_json(alerts, full_output_path, unique_key=self.unique_key)
                logging.info(f"[IGN] Fetched {len(alerts)} alerts from {self.url} | Status: {response.status_code}")
            else:
                logging.info(f"[IGN] No alerts found to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[IGN] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )