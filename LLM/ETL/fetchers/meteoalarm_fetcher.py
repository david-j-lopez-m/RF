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

    def __init__(self, config=None, source_key="meteoalarm"):
        """Initialize MeteoalarmFetcher with configuration.
            
            Args:
                config (dict): Optional global config dictionary. If None, it will use get_source_config.
                source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format(source_key)
        self.unique_key = self.config.get("unique_key")

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

            alerts = []
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
                    alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[METEOALARM] Skipping malformed alert element: {e}")

            if alerts:
                save_json(alerts, self.output, source_key=self.source_key, unique_key=self.unique_key)
                logging.info(f"[METEOALARM] Fetched {len(alerts)} alerts from {self.url} | Status: {response.status_code}")
            else:
                logging.info(f"[METEOALARM] No alerts found at {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[METEOALARM] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )