import json
import spacy
import os
import logging
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path, get_serialization_rules, get_output_schema


class AEMETAlertPreprocessor:
    """
    Preprocessor for AEMET weather alerts to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("aemet")
        self.input_path = get_source_input_path("aemet")
        self.output_path = get_source_output_path("aemet")
        self.unique_key = self.cfg.get("unique_key", "identifier")
        self.serialization_rules = get_serialization_rules()
        self.output_schema = get_output_schema()
        self.nlp = spacy.load("es_core_news_sm")
        logging.info(f"Initialized NOAAAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_tags(self, event: str, headline: str, description: str) -> List[str]:
        """
        Extract keywords or tags from event/headline/description using simple heuristics or NER.
        """
        tags = set()
        for txt in [event, headline, description]:
            if not txt:
                continue
            doc = self.nlp(txt)
            tags.update(ent.text.lower() for ent in doc.ents if ent.label_ in ("LOC", "MISC", "PER", "ORG"))
            # También incluye palabras clave de fenómenos meteorológicos comunes
            for w in ["costeros", "viento", "lluvia", "tormenta", "nieve", "temperatura"]:
                if w in txt.lower():
                    tags.add(w)
        return list(sorted(tags))

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert datetime string to ISO 8601 UTC format."""
        from datetime import datetime
        if not dt_string:
            return ""
        # Manejar posible zona horaria
        try:
            # Si el string ya es ISO 8601 se puede convertir directamente a UTC
            dt = datetime.fromisoformat(dt_string.replace("Z", "+00:00"))
            return dt.astimezone(tz=None).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def load_alerts(self) -> List[Dict]:
        """Load raw AEMET alerts from input JSON file."""
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
        """Transform raw alerts into the standardized output format, skipping duplicates and filtering by relevance."""
        already_processed = self.load_preprocessed_keys()
        processed = []
        # Define relevant severities and levels
        relevant_severities = {"Severe", "Extreme"}
        relevant_levels = {"naranja", "rojo"} 
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                continue

            # Filtering by severity and level
            severity = (alert.get("severity") or "").capitalize()
            level = (alert.get("level") or "").lower()
            if severity not in relevant_severities:
                continue
            if level not in relevant_levels:
                continue

            title = alert.get("headline") or alert.get("event") or "AEMET Alert"
            description = (alert.get("description") or "").replace("\r\n", " ").replace("\n", " ").strip()
            event_datetime = self.standardize_datetime(alert.get("sent") or alert.get("onset"))
            valid_from = self.standardize_datetime(alert.get("onset"))
            valid_to = self.standardize_datetime(alert.get("expires"))
            location = alert.get("area") or ""
            magnitude = None
            try:
                magnitude = float(alert.get("probability", "") or 0)
            except Exception:
                pass
            link = ""
            impacts = ""
            tags = self.extract_tags(alert.get("event", ""), alert.get("headline", ""), alert.get("description", ""))
            extra_data = {
                "urgency": alert.get("urgency"),
                "certainty": alert.get("certainty"),
                "level": alert.get("level"),
                "status": alert.get("status"),
                "parameter": alert.get("parameter")
            }

            processed_alert = {
                "source": "AEMET",
                "alert_type": "weather",
                "title": title,
                "description": description,
                "event_datetime": event_datetime,
                "location": location,
                "severity": severity,
                "magnitude": magnitude,
                "link": link,
                "unique_key": key,
                "impacts": impacts,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "tags": tags,
                "extra_data": extra_data,
            }

            # Apply serialization rules efficiently
            for field in processed_alert.keys() & self.serialization_rules.keys():
                if self.serialization_rules[field] == "json_string":
                    processed_alert[field] = json.dumps(processed_alert[field], ensure_ascii=False)

            # Build final alert dict with all fields in output schema
            final_alert = {field: processed_alert.get(field, None) for field in self.output_schema}
            
            processed.append(final_alert)
            #logging.info(f"Processed new relevant alert with key: {key}")

        return processed

    def save_alerts(self, processed_alerts: List[Dict]):
        """Append new processed alerts to output JSON file (if any)."""
        if not processed_alerts:
            logging.info("No new alerts to preprocess.")
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
        logging.info(f"Saved {len(processed_alerts)} new preprocessed alerts. Total: {len(all_alerts)}.")