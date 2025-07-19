import json
import os
import logging
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path

class USGSEarthquakePreprocessor:
    """
    Preprocessor for USGS Earthquake alerts. Converts raw USGS earthquake JSONs into a standardized schema.
    """

    def __init__(self):
        self.cfg = get_source_config("usgs_earthquakes")
        self.input_path = get_source_input_path("usgs_earthquakes")
        self.output_path = get_source_output_path("usgs_earthquakes")
        self.unique_key = self.cfg.get("unique_key", "code")
        self.timestamp_format = self.cfg.get("timestamp_format", "%Y-%m-%d %H:%M:%S UTC")
        logging.info(f"Initialized USGSEarthquakePreprocessor with input: {self.input_path}, output: {self.output_path}")

    def load_alerts(self) -> List[Dict]:
        """Load raw USGS earthquake alerts from input JSON file."""
        try:
            with open(self.input_path, "r", encoding="utf-8") as f:
                alerts = json.load(f)
            logging.info(f"Loaded {len(alerts)} USGS alerts from {self.input_path}")
            return alerts
        except FileNotFoundError:
            logging.error(f"Input file not found: {self.input_path}")
            return []
        except Exception as e:
            logging.error(f"Failed to load input alerts: {e}")
            return []

    def load_preprocessed_keys(self) -> set:
        """Load unique_keys from previous output, if exists."""
        if not os.path.exists(self.output_path):
            return set()
        try:
            with open(self.output_path, "r", encoding="utf-8") as f:
                preprocessed = json.load(f)
            logging.info(f"Loaded {len(preprocessed)} preprocessed alerts from {self.output_path}")
            return set(alert.get(self.unique_key) for alert in preprocessed if self.unique_key in alert)
        except Exception as e:
            logging.warning(f"Could not read preprocessed file: {e}")
            return set()

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert USGS datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """Transform raw alerts into the standardized output format, skipping duplicates."""
        already_processed = self.load_preprocessed_keys()
        processed = []
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                continue

            event_datetime = self.standardize_datetime(alert.get("event_datetime", ""))
            # Place (location) is already a string
            location = alert.get("place", "Unknown")
            magnitude = alert.get("magnitude")
            depth_km = alert.get("depth_km")
            latitude = alert.get("latitude")
            longitude = alert.get("longitude")
            event_type = alert.get("event_type", "earthquake")
            url = alert.get("url")
            title = alert.get("title")
            status = alert.get("status")
            tsunami = alert.get("tsunami")
            ids = alert.get("ids")
            code = alert.get("code")

            processed.append({
                "source": "USGS",
                "alert_type": event_type,
                "title": title if title else f"USGS {event_type.capitalize()}",
                "description": f"Magnitude {magnitude} {event_type} at {location}. Depth: {depth_km} km. Status: {status}. Tsunami: {tsunami}.",
                "event_datetime": event_datetime,
                "location": location,
                "severity": None,  # Not present in USGS data, unless you want to map magnitude to severity
                "magnitude": magnitude,
                "link": url,
                self.unique_key: key,
                "impacts": None,
                "valid_from": event_datetime,
                "valid_to": None,
                "tags": [],
                "extra_data": {
                    "depth_km": depth_km,
                    "latitude": latitude,
                    "longitude": longitude,
                    "status": status,
                    "tsunami": tsunami,
                    "ids": ids,
                    "code": code
                }
            })
            #logging.info(f"Processed new alert with key: {key}")
        return processed

    def save_alerts(self, processed_alerts: List[Dict]):
        """Append new processed alerts to output JSON file (if any)."""
        if not processed_alerts:
            logging.info("No new alerts to preprocess.")
            return
        if os.path.exists(self.output_path):
            with open(self.output_path, "r", encoding="utf-8") as f:
                previous = json.load(f)
        else:
            previous = []
        all_alerts = previous + processed_alerts
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(all_alerts, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(processed_alerts)} new preprocessed alerts. Total: {len(all_alerts)}.")