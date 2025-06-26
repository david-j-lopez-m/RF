import json
import re
import spacy
import os
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path

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
        self.nlp = spacy.load("en_core_web_sm") 

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
        except Exception:
            return dt_string

    def load_alerts(self) -> List[Dict]:
        """Load raw NOAA alerts from input JSON file."""
        with open(self.input_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_preprocessed_keys(self) -> set:
        """Load unique_keys from previous output, if exists."""
        if not os.path.exists(self.output_path):
            return set()
        with open(self.output_path, "r", encoding="utf-8") as f:
            try:
                preprocessed = json.load(f)
                return set(alert.get(self.unique_key) for alert in preprocessed if self.unique_key in alert)
            except Exception:
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

            processed.append({
                "source": "NOAA",
                "alert_type": "space_weather",
                "title": message.split(".")[0] if message else "NOAA Alert",
                "description": message,
                "event_datetime": event_datetime,
                "location": self.extract_location(impacts, message),
                "severity": None,  # Set to None or calculate later if needed
                "magnitude": magnitude,
                "link": "https://www.swpc.noaa.gov/noaa-scales-explanation",
                self.unique_key: key  # Guarda el unique_key para futuras comprobaciones
            })
        return processed

    def save_alerts(self, processed_alerts: List[Dict]):
        """Append new processed alerts to output JSON file (if any)."""
        if not processed_alerts:
            print("[INFO] No new alerts to preprocess.")
            return
        # Carga los ya preprocesados (si existe) y concatena
        if os.path.exists(self.output_path):
            with open(self.output_path, "r", encoding="utf-8") as f:
                previous = json.load(f)
        else:
            previous = []
        all_alerts = previous + processed_alerts
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(all_alerts, f, ensure_ascii=False, indent=2)
        print(f"[INFO] Saved {len(processed_alerts)} new preprocessed alerts. Total: {len(all_alerts)}.")