import json
import os
import logging
import re
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path, get_serialization_rules, get_output_schema

class GDACSAlertPreprocessor:
    """
    Preprocessor for GDACS alerts to convert them into a standard format for LLM/RAG pipelines.
    Filters alerts to include only those relevant to Spain, specific alert types, and severity levels.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("gdacs")
        self.input_path = get_source_input_path("gdacs")
        self.output_path = get_source_output_path("gdacs")
        self.timestamp_format = self.cfg.get("timestamp_format", "%a, %d %b %Y %H:%M:%S %Z")
        self.unique_key = self.cfg.get("unique_key", "event_datetime")
        self.serialization_rules = get_serialization_rules()
        self.output_schema = get_output_schema()
        self.alert_type_keywords = {
            "tropical_cyclone": ["cyclone", "tropical cyclone", "hurricane", "typhoon"],
            "earthquake": ["earthquake", "seismic", "magnitude"],
            "flood": ["flood", "inundation", "overflow"],
            "forest_fire": ["forest fire", "wildfire", "bushfire"],
            "drought": ["drought"],
            # Add more types as needed
        }
        logging.info(f"Initialized GDACSAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_alert_type(self, title: str) -> str:
        """
        Detect alert_type from the title using predefined keywords.
        """
        title = (title or "").lower()
        for alert_type, keywords in self.alert_type_keywords.items():
            for kw in keywords:
                if kw in title:
                    return alert_type
        return "other"

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert GDACS datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def extract_location(self, alert: Dict) -> str:
        """
        Extract location info from alert dictionary.
        Prefer country field, else extract from description using regex.
        """
        country = alert.get("country")
        if country:
            return country
        desc = alert.get("description", "")
        # Try to extract country names (simplistic, but can tune with spaCy if needed)
        m = re.search(r"affects these countries: ([\w\s,()]+)[\.;]", desc)
        if m:
            return m.group(1).strip()
        return "Unknown"

    def extract_severity(self, alert: Dict) -> str:
        """
        Try to extract severity from alertlevel or description/title.
        """
        if alert.get("alertlevel"):
            return alert["alertlevel"].capitalize()
        # Fallback: green, orange, red in title
        title = alert.get("title", "").lower()
        for level in ["green", "orange", "red"]:
            if level in title:
                return level.capitalize()
        return None

    def extract_magnitude(self, alert: Dict) -> float:
        """
        Try to extract magnitude/maximum windspeed or similar metric from description.
        """
        desc = alert.get("description", "")
        m = re.search(r"maximum wind speed of (\d+) km/h", desc)
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                pass
        return None

    def load_alerts(self) -> List[Dict]:
        """Load raw GDACS alerts from input JSON file."""
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

    def is_alert_in_spain(self, alert: Dict) -> bool:
        """
        Check if 'Spain' appears in the location, title, or description fields (case-insensitive).
        """
        location = alert.get("location", "")
        title = alert.get("title", "")
        description = alert.get("description", "")
        combined_text = f"{location} {title} {description}".lower()
        return "spain" in combined_text

    def is_relevant_type(self, alert: Dict) -> bool:
        """
        Keep only alert types 'flood', 'earthquake', or 'forest_fire'.
        """
        alert_type = alert.get("alert_type", "").lower()
        return alert_type in {"flood", "earthquake", "forest_fire"}

    def is_severe(self, alert: Dict) -> bool:
        """
        Return True only if the severity field is 'Orange' or 'Red'.
        """
        severity = alert.get("severity")
        return severity in {"Orange", "Red"}

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """
        Transform raw alerts into the standardized output format, skipping duplicates.
        Filters alerts to include only those relevant to Spain, specific alert types (flood, earthquake, forest_fire),
        and severity levels (Orange or Red).
        """
        already_processed = self.load_preprocessed_keys()
        processed = []
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                continue

            standardized_dt = self.standardize_datetime(alert.get("event_datetime", ""))
            title = alert.get("title", "GDACS Alert")
            description = alert.get("description", "")
            alert_type = self.extract_alert_type(title)
            location = self.extract_location(alert)
            severity = self.extract_severity(alert)
            magnitude = self.extract_magnitude(alert)

            processed_alert = {
                "source": "GDACS",
                "alert_type": alert_type,
                "title": title,
                "description": description,
                "event_datetime": standardized_dt,
                "location": location,
                "severity": severity,
                "magnitude": magnitude,
                "link": "",  # Optionally add event-specific link if present in alert
                self.unique_key: key
            }

            if not self.is_alert_in_spain(processed_alert):
                continue
            if not self.is_relevant_type(processed_alert):
                continue
            if not self.is_severe(processed_alert):
                continue
            

            # Apply serialization rules efficiently
            for field in processed_alert.keys() & self.serialization_rules.keys():
                if self.serialization_rules[field] == "json_string":
                    processed_alert[field] = json.dumps(processed_alert[field], ensure_ascii=False)

            # Build final alert dict with all fields in output schema
            final_alert = {field: processed_alert.get(field, None) for field in self.output_schema}

            processed.append(self.sanitize_for_chroma(processed_alert))
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
    def sanitize_for_chroma(meta: dict) -> dict:
        """
        Replace None with '' (empty string) for string fields,
        or with 0 for numeric fields if you wish.
        If unsure, safest is to use empty string for all None.
        """
        return {k: ("" if v is None else v) for k, v in meta.items()}