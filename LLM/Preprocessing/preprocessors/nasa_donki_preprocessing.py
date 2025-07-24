import json
import re
import logging
import os
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path, get_output_schema, get_serialization_rules

class NASADONKIAlertPreprocessor:
    """
    Preprocessor for NASA DONKI alerts. Converts raw DONKI alert JSONs into a standardized schema.
    """

    def __init__(self):
        self.cfg = get_source_config("nasa_donki")
        self.input_path = get_source_input_path("nasa_donki")
        self.output_path = get_source_output_path("nasa_donki")
        self.unique_key = self.cfg.get("unique_key", "message_id")
        self.timestamp_format = self.cfg.get("timestamp_format", "%Y-%m-%dT%H:%MZ")
        self.serialization_rules = get_serialization_rules()
        self.output_schema = get_output_schema()
        logging.info(f"Initialized NASADONKIAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def load_alerts(self) -> List[Dict]:
        """Load raw NASA DONKI alerts from input JSON file."""
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
                continue

            body = (alert.get("body") or "").replace("\r\n", " ").replace("\n", " ").strip()
            links = self.extract_links(body)
            magnitude = self.extract_magnitude(body)
            event_time = self.extract_event_time(body)
            location = self.extract_location(body)

            processed_alert = {
                "source": "NASA_DONKI",
                "alert_type": alert.get("alert_type", "space_weather"),
                "title": f"{alert.get('alert_type', 'DONKI Alert')}: {alert.get('message_id')}",
                "description": body,
                "event_datetime": event_time or alert.get("issue_datetime"),
                "location": location,
                "severity": None,
                "magnitude": magnitude,
                "link": alert.get("url"),
                self.unique_key: key,
                "impacts": None,
                "valid_from": event_time,
                "valid_to": None,
                "tags": [],
                "extra_data": {
                    "links": links,
                    "event_summary": alert.get("event_summary"),
                }
            }


            # Apply serialization rules efficiently
            for field in processed_alert.keys() & self.serialization_rules.keys():
                if self.serialization_rules[field] == "json_string":
                    processed_alert[field] = json.dumps(processed_alert[field], ensure_ascii=False)

            # Build final alert dict with all fields in output schema
            final_alert = {field: processed_alert.get(field, None) for field in self.output_schema}

            processed.append(self.sanitize_for_chroma(final_alert))
  
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

    @staticmethod
    def extract_links(body: str) -> List[str]:
        """Extract all URLs from the alert body."""
        return re.findall(r"https?://[^\s]+", body)

    @staticmethod
    def extract_magnitude(body: str) -> float:
        """Extract CME estimated speed in km/s from the body, if available."""
        m = re.search(r"Estimated speed:\s*~?(\d+)\s*km/s", body)
        if m:
            return float(m.group(1))
        return None

    @staticmethod
    def extract_event_time(body: str) -> str:
        """Extract start time of the event from the alert body."""
        m = re.search(r"Start time of the event:\s*([\d\-T:Z]+)", body)
        if m:
            return m.group(1)
        return None

    @staticmethod
    def extract_location(body: str) -> str:
        """Extract direction/location information, if available."""
        m = re.search(r"Direction \(lon\./lat\.\):\s*([^\n]+)", body)
        if m:
            return m.group(1).strip()
        return "Unknown"
    
    @staticmethod
    def sanitize_for_chroma(meta: dict) -> dict:
        """
        Replace None with '' (empty string) for string fields,
        or with 0 for numeric fields if you wish.
        If unsure, safest is to use empty string for all None.
        """
        return {k: ("" if v is None else v) for k, v in meta.items()}