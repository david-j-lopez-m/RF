import json
import re
import spacy
import os
import logging
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path

class IGNAlertPreprocessor:
    """
    Preprocessor for IGN earthquake alerts (Spanish) to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("ign")
        self.input_path = get_source_input_path("ign")
        self.output_path = get_source_output_path("ign")
        self.timestamp_format = self.cfg.get("timestamp_format", "%d/%m/%Y %H:%M:%S")
        self.unique_key = self.cfg.get("unique_key", "event_datetime")
        self.nlp = spacy.load("es_core_news_sm")
        logging.info(f"Initialized IGNAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_location(self, description: str) -> str:
        """
        Extract relevant location info from Spanish description using spaCy NER.
        """
        # Try regex to extract between "en " and " en la fecha"
        m = re.search(r"en ([^\.]+) en la fecha", description)
        if m:
            return m.group(1).strip()

        # Fall back to spaCy for GPE (geopolitical entity) extraction
        doc = self.nlp(description)
        places = [ent.text for ent in doc.ents if ent.label_ in ("LOC", "GPE")]
        if places:
            return ", ".join(sorted(set(places)))
        return "Desconocido"

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert IGN datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def load_alerts(self) -> List[Dict]:
        """Load raw IGN alerts from input JSON file."""
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

    def is_relevant_magnitude(self, alert: Dict, threshold: float = 4.0) -> bool:
        """Return True if the alert's magnitude is not None and is >= threshold."""
        magnitude = alert.get("magnitude")
        try:
            magnitude = float(magnitude)
        except (TypeError, ValueError):
            return False
        return magnitude >= threshold

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """Transform raw alerts into the standardized output format, skipping duplicates.
        Only earthquakes with magnitude >= 4.0 are processed.
        """
        already_processed = self.load_preprocessed_keys()
        processed = []
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                continue

            title = alert.get("title", "Alerta IGN")
            description = (alert.get("description") or "").replace("\r\n", " ").replace("\n", " ").strip()
            event_datetime = self.standardize_datetime(alert.get("event_datetime", ""))
            magnitude = alert.get("magnitude")
            try:
                magnitude_float = float(magnitude)
            except (TypeError, ValueError):
                magnitude_float = None

            if not self.is_relevant_magnitude({"magnitude": magnitude_float}):
                continue

            location = self.extract_location(description)

            processed.append({
                "source": "IGN",
                "alert_type": "earthquake",
                "title": title,
                "description": description,
                "event_datetime": event_datetime,
                "location": location,
                "severity": None,
                "magnitude": magnitude_float,
                "link": "",  # If you want to add a link field later, extract it here.
                self.unique_key: key
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