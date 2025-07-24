import json
import re
import spacy
import os
import logging
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path, get_serialization_rules, get_output_schema

class NOAAAlertPreprocessor:
    """
    Preprocessor for NOAA space weather alerts to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("noaa_swpc")
        self.input_path = get_source_input_path("noaa_swpc")
        self.output_path = get_source_output_path("noaa_swpc")
        self.timestamp_format = self.cfg.get("timestamp_format", "%Y-%m-%d %H:%M:%S.%f")
        self.unique_key = self.cfg.get("unique_key", "issue_datetime")
        self.serialization_rules = get_serialization_rules()
        self.output_schema = get_output_schema()
        self.nlp = spacy.load("en_core_web_sm") 
        logging.info(f"Initialized NOAAAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_location(self, impacts: str, message: str = "") -> str:
        """
        Extract relevant location info from impacts/message.
        Uses spaCy NER for place/entity extraction, plus geomagnetic patterns.
        """
        fulltext = impacts + " " + message

        # 1. Geomagnetic Latitude
        lat_match = re.search(r"(\d+)\s*degrees? Geomagnetic Latitude", fulltext, re.IGNORECASE)
        if lat_match:
            return f"Poleward of {lat_match.group(1)} degrees Geomagnetic Latitude"

        # 2. Use spaCy to extract GPE (Geo-Political Entities: countries, cities, etc.)
        doc = self.nlp(fulltext)
        places = [ent.text for ent in doc.ents if ent.label_ in ("GPE", "LOC")]
        if places:
            return ", ".join(sorted(set(places)))

        # 3. Satellite/space/global
        if any(kw in fulltext.lower() for kw in ["satellite", "spacecraft", "satellite systems"]):
            return "Global (satellite/space systems)"
        if "global" in fulltext.lower():
            return "Global"

        # Fallback: unknown or general description
        return "Unknown"

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert NOAA datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def load_alerts(self) -> List[Dict]:
        """Load raw NOAA alerts from input JSON file."""
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

            message = (alert.get("message") or "").replace("\r\n", " ").replace("\n", " ").strip()
            impacts = (alert.get("impacts") or "").replace("\r\n", " ").replace("\n", " ").strip()
            event_datetime = self.standardize_datetime(alert.get("issue_datetime", ""))
            magnitude = alert.get("k_index")
            try:
                magnitude = float(magnitude)
            except (TypeError, ValueError):
                pass

            processed_alert = {
                "source": "NOAA",
                "alert_type": "space_weather",
                "title": message.split(".")[0] if message else "NOAA Alert",
                "description": message,
                "event_datetime": event_datetime,
                "location": self.extract_location(impacts, message),
                "severity": None,
                "magnitude": magnitude,
                "link": "https://www.swpc.noaa.gov/noaa-scales-explanation",
                self.unique_key: key
            }

            # Apply serialization rules efficiently
            for field in processed_alert.keys() & self.serialization_rules.keys():
                if self.serialization_rules[field] == "json_string":
                    processed_alert[field] = json.dumps(processed_alert[field], ensure_ascii=False)

            # Build final alert dict with all fields in output schema
            final_alert = {field: processed_alert.get(field, None) for field in self.output_schema}

            processed.append(self.sanitize_for_chroma(final_alert))
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