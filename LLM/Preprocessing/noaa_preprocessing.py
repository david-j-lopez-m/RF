import json
import spacy
from typing import List, Dict
from LLM.Preprocessing.config import get_source_config, render_path  # Ajusta si el path es distinto

class NOAAAlertPreprocessor:
    """
    Preprocessor for NOAA space weather alerts to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self, date: str, datetime_str: str):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        Args:
            date (str): Date string (YYYY-MM-DD).
            datetime_str (str): Full timestamp string (e.g., 2025-06-22T15-22-01).
        """
        self.cfg = get_source_config("noaa_swpc")
        self.input_path = render_path(self.cfg["input_path_template"], date, datetime_str)
        self.output_path = render_path(self.cfg["output_path_template"], date, datetime_str)
        self.timestamp_format = self.cfg.get("timestamp_format", "%Y-%m-%d %H:%M:%S.%f")
        self.nlp = spacy.load("en_core_web_sm")

    def extract_locations(self, text: str) -> str:
        """Extract geographical locations from text using spaCy NER."""
        doc = self.nlp(text)
        locations = [ent.text for ent in doc.ents if ent.label_ == "GPE"]
        return ", ".join(locations) if locations else None

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

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """Transform raw alerts into the standardized output format."""
        processed = []
        for alert in alerts:
            location = self.extract_locations(alert.get("impacts", "") + " " + alert.get("message", ""))
            processed.append({
                "source": "NOAA",
                "alert_type": "space_weather",
                "title": alert.get("message", "").split("\n")[0] if alert.get("message") else "NOAA Alert",
                "description": alert.get("message", ""),
                "event_datetime": self.standardize_datetime(alert.get("issue_datetime", "")),
                "location": location,
                "severity": "moderate",  # Placeholder, adjust if you want
                "magnitude": alert.get("k_index", ""),
                "link": "www.swpc.noaa.gov/noaa-scales-explanation"
            })
        return processed

    def save_alerts(self, processed_alerts: List[Dict]):
        """Save processed alerts to output JSON file."""
        # Ensure output directory exists
        import os
        os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(processed_alerts, f, ensure_ascii=False, indent=2)