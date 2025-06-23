"""
firms_fetcher.py

Fetcher class to retrieve wildfire alerts from NASA FIRMS (Fire Information for Resource Management System).
- Sends a GET request to the NASA FIRMS CSV endpoint.
- Parses the CSV and stores wildfire events as JSON, grouped by date.
"""

import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import io
import hashlib
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class FIRMSFetcher:
    """Fetcher class to retrieve and store NASA FIRMS wildfire alerts from CSV."""

    def __init__(self, config=None, source_key="firms"):
        """Initialize FIRMSFetcher with configuration.
            
            Args:
                config (dict): Optional global config dictionary. If None, it will use get_source_config.
                source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url_template = self.config["url_template"]
        self.map_key = self.config["MAP_KEY"]
        self.source = self.config["SOURCE"]
        self.day_range = self.config["DAY_RANGE"]
        self.output = self.config["output_filename"]
        self.base_data_path = self.config.get("base_data_path", "data/alertas")
        self.timestamp_format = get_source_timestamp_format("firms")
        self.unique_key = self.config.get("unique_key", "identifier")

    def fetch(self):
        """
        Fetch FIRMS wildfire alerts from CSV, parse, and save as JSON.
        All alerts from the current fetch are stored.
        """
        try:
            # Build the API URL using config and template
            url = self.url_template.format(
                MAP_KEY=self.map_key,
                SOURCE=self.source,
                DAY_RANGE=self.day_range
            )
            r = requests.get(url, timeout=20)
            r.raise_for_status()

            # Parse CSV to DataFrame
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty:
                logging.info("[FIRMS] No data found in CSV.")
                return

            alerts = []

            for _, row in df.iterrows():
                alert = self.parse_alert(row)
                alerts.append(alert)

            if alerts:
                save_json(alerts, self.output, source_key=self.source_key, unique_key=self.unique_key)
                logging.info(f"[FIRMS] Fetched and saved {len(alerts)} wildfire alerts from {url}")
            else:
                logging.info(f"[FIRMS] No wildfire alerts to save from {url}")

        except Exception as e:
            logging.error(f"[FIRMS] Error fetching from {url} | Exception: {e}")

    @staticmethod
    def parse_alert(row):
        """
        Extract relevant fields from a single FIRMS CSV row.

        Args:
            row (pd.Series): DataFrame row with alert fields.

        Returns:
            dict: Parsed alert fields.
        """
        acq_date = str(row.get('acq_date', ''))
        acq_time = str(row.get('acq_time', ''))
        event_datetime = f"{acq_date} {acq_time}"
        alert = {
            "event_datetime": event_datetime,
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "brightness": row.get("brightness"),
            "confidence": row.get("confidence"),
            "satellite": row.get("satellite"),
            "instrument": row.get("instrument"),
            "daynight": row.get("daynight"),
            "frp": row.get("frp"),
            "alert_type": "wildfire"
        }
        # Añade el campo único
        alert["firms_id"] = FIRMSFetcher.generate_firms_id(row)
        return alert
    
    @staticmethod
    def generate_firms_id(row):
        """Generate a unique ID for FIRMS alerts using key fields."""
        # Use latitude, longitude, acq_date, acq_time as unique key
        key_str = f"{row.get('latitude','')}_{row.get('longitude','')}_{row.get('acq_date','')}_{row.get('acq_time','')}"
        return hashlib.md5(key_str.encode()).hexdigest()