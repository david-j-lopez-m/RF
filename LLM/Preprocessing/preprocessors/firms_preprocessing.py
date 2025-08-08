import json
import logging
import os
from pathlib import Path
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path,get_serialization_rules, get_output_schema
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

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
        self.serialization_rules = get_serialization_rules()
        self.output_schema = get_output_schema()
        self.geolocator = Nominatim(user_agent="rf_preprocessor")
        self.reverse = RateLimiter(self.geolocator.reverse, min_delay_seconds=1)
        self.location_cache = {}  # Cache for already resolved coordinates
        logging.info(f"Initialized FIRMSAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def extract_location(self, latitude, longitude):
        """
        Resolve location name (province/region) from latitude and longitude using reverse geocoding.
        Fallback to "lat, lon" string if resolution fails.
        """
        if latitude is None or longitude is None:
            return "Unknown"

        coord_key = (round(latitude, 3), round(longitude, 3))  # Round to avoid too many unique keys
        if coord_key in self.location_cache:
            return self.location_cache[coord_key]

        try:
            location = self.reverse((latitude, longitude), language='en', exactly_one=True, timeout=10)
            if location and location.raw and "address" in location.raw:
                address = location.raw["address"]
                province = address.get("state") or address.get("county") or address.get("region")
                if province:
                    self.location_cache[coord_key] = province
                    return province
            logging.warning(f"No province found for coordinates: {latitude}, {longitude}")
        except Exception as e:
            logging.error(f"Reverse geocoding failed for ({latitude}, {longitude}): {e}")

        fallback = f"{latitude:.5f}, {longitude:.5f}"
        self.location_cache[coord_key] = fallback
        return fallback

    def standardize_datetime(self, dt_string: str) -> str:
        """Convert FIRMS datetime string to ISO 8601 format (UTC)."""
        from datetime import datetime
        try:
            dt = datetime.strptime(dt_string, self.timestamp_format)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            logging.warning(f"Failed to standardize datetime: {dt_string} | {e}")
            return dt_string

    def load_alerts(self, incremental: bool = True) -> List[Dict]:
        """Load raw FIRMS alerts from input JSON file."""
        # If incremental is requested, attempt to load only the latest incremental file if available
        if incremental:
            incremental_dir = Path(self.input_path).parent / "incremental"
            try:
                if incremental_dir.exists():
                    json_files = sorted(incremental_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
                    if json_files:
                        latest = json_files[0]
                        with open(latest, "r", encoding="utf-8") as f:
                            alerts = json.load(f)
                        logging.info(f"Loaded {len(alerts)} incremental alerts from {latest}")
                        return alerts
                logging.info("No incremental file found; skipping load because incremental=True.")
                return []
            except Exception as e:
                logging.warning(f"Failed to load incremental alerts: {e}")
                return []
        else:
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

    def is_relevant_fire(self, brightness, confidence, frp):
        """
        Determine if a fire alert is relevant based on brightness, confidence, and FRP thresholds.
        Precision-oriented: exclude small, low-confidence fires.
        """
        if brightness is None or confidence is None or frp is None:
            return False
        # Confidence >= 30 (nominal/high), FRP >= 20 MW, brightness optional
        return confidence >= 30 and frp >= 20.0
    
    def is_in_spain(self, lat, lon) -> bool:
        """
        Check if coordinates are within Spain and Southern Europe bounding box.
        Spain approx: lat 35.9 to 43.8, lon -9.3 to 3.3
        Southern Europe buffer: lat 35 to 47, lon -10 to 20
        """
        if lat is None or lon is None:
            return False
        return 36.0 <= lat <= 44.0 and 5 <= lon <= 20.0

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """
        Process raw FIRMS alerts: filter for relevance, standardize, and prepare for storage.
        """
        already_processed = self.load_preprocessed_keys()
        processed = []

        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                continue

            latitude = alert.get("latitude")
            longitude = alert.get("longitude")
            # Filter by geographic relevance (Spain & Southern Europe)
            if not self.is_in_spain(latitude, longitude):
                continue

            brightness = alert.get("brightness")
            confidence = alert.get("confidence")
            frp = alert.get("frp")
            # Filter by fire relevance (confidence, FRP thresholds)
            if not self.is_relevant_fire(brightness, confidence, frp):
                continue

            event_datetime = self.standardize_datetime(alert.get("event_datetime", ""))
            description = f"Wildfire detected by {alert.get('satellite', 'Unknown Satellite')} ({alert.get('instrument', '')}), brightness: {brightness}, confidence: {confidence}, FRP: {frp}."

            processed_alert = {
                "source": "FIRMS",
                "alert_type": "wildfire",
                "title": "Wildfire detection",
                "description": description,
                "event_datetime": event_datetime,
                "location": self.extract_location(latitude, longitude),
                "severity": None,
                "magnitude": brightness,
                "link": "https://firms.modaps.eosdis.nasa.gov/",
                self.unique_key: key,
                "latitude": latitude,
                "longitude": longitude,
                "confidence": confidence,
                "extra_data": {
                    "frp": frp,
                    "satellite": alert.get("satellite"),
                    "instrument": alert.get("instrument"),
                    "daynight": alert.get("daynight")
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
    def sanitize_for_chroma(meta: dict) -> dict:
        """
        Replace None with '' (empty string) for string fields,
        or with 0 for numeric fields if you wish.
        If unsure, safest is to use empty string for all None.
        """
        return {k: ("" if v is None else v) for k, v in meta.items()}