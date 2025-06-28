import json
import logging
import os
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path

class FIRMSAlertPreprocessor:
    """
    Preprocessor for NASA FIRMS wildfire alerts to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("firms")
        self.input_path = get_source_input_path("firms")
        self.output_path = get_source_output_path("firms")
        self.unique_key = self.cfg.get("unique_key", "firms_id")
        self.timestamp_format = self.cfg.get("timestamp_format", "%Y-%m-%d %H%M")
        logging.info(f"Initialized FIRMSAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_location(self, latitude, longitude):
        """
        Return location string from latitude and longitude.
        For future improvement: use reverse geocoding here.
        """
        if latitude is not None and longitude is not None:
            return f"{latitude:.5f}, {longitude:.5f}"
        return "Unknown"

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert FIRMS datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def load_alerts(self) -> List[Dict]:
        """Load raw FIRMS alerts from input JSON file."""
        try:
            with open(self.input_path, "r", encoding="utf-8") as f:
                alerts = json.load(f)
            logging.info(f"Loaded {len(alerts)} alerts from {self.input_path}")
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

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """Transform raw alerts into the standardized output format, skipping duplicates."""
        already_processed = self.load_preprocessed_keys()
        processed = []
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                logging.debug(f"Skipping already processed alert: {key}")
                continue

            event_datetime = self.standardize_datetime(alert.get("event_datetime", ""))
            latitude = alert.get("latitude")
            longitude = alert.get("longitude")
            description = f"Wildfire detected by {alert.get('satellite', 'Unknown Satellite')} ({alert.get('instrument', '')}), brightness: {alert.get('brightness', '')}, confidence: {alert.get('confidence', '')}, FRP: {alert.get('frp', '')}."
            
            processed.append({
                "source": "FIRMS",
                "alert_type": "wildfire",
                "title": "Wildfire detection",
                "description": description,
                "event_datetime": event_datetime,
                "location": self.extract_location(latitude, longitude),
                "severity": None,
                "magnitude": alert.get("brightness"),  # Could be replaced with another field if needed
                "link": "https://firms.modaps.eosdis.nasa.gov/",
                self.unique_key: key,
                "latitude": latitude,
                "longitude": longitude,
                "confidence": alert.get("confidence"),
                "extra_data": {
                    "frp": alert.get("frp"),
                    "satellite": alert.get("satellite"),
                    "instrument": alert.get("instrument"),
                    "daynight": alert.get("daynight")
                }
            })
            logging.info(f"Processed new alert with key: {key}")
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