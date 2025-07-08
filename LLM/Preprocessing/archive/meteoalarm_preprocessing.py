# DEPRECATED: This script is currently not in use.
# Kept for potential future reference or reactivation.

import json
import re
import logging
import os
from bs4 import BeautifulSoup
from typing import List, Dict
from config import get_source_config, get_source_input_path, get_source_output_path

class MeteoalarmAlertPreprocessor:
    """
    Preprocessor for Meteoalarm alerts to convert them into a standard format for LLM/RAG pipelines.
    """

    def __init__(self):
        """
        Initialize the preprocessor by resolving input/output paths from config.
        """
        self.cfg = get_source_config("meteoalarm")
        self.input_path = get_source_input_path("meteoalarm")
        self.output_path = get_source_output_path("meteoalarm")
        self.timestamp_format = self.cfg.get("timestamp_format", "%a, %d %b %y %H:%M:%S %z")
        self.unique_key = self.cfg.get("unique_key", "guid")
        logging.info(f"Initialized MeteoalarmAlertPreprocessor with input: {self.input_path}, output: {self.output_path}")

    def load_alerts(self) -> List[Dict]:
        """Load raw Meteoalarm alerts from input JSON file."""
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
        if not self.output_path or not os.path.exists(self.output_path):
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
        Determine if the alert is for Spain.
        Checks title for keywords and also checks region code 'ES' in link or region.
        """
        title = alert.get("title", "").lower()
        if ("spain" in title) or ("españa" in title):
            return True
        # Also check region code ES in link if present
        link = alert.get("link", "")
        m = re.search(r"region=([A-Za-z]+)", link)
        if m and m.group(1).upper() == "ES":
            return True
        # Additionally, check if region code ES is in the title after removing 'MeteoAlarm'
        region = None
        if "MeteoAlarm" in alert.get("title", ""):
            region = alert.get("title", "").replace("MeteoAlarm", "").strip()
            if region.upper() == "ES":
                return True
        return False

    def is_severe(self, warning: Dict, min_level: int = 2) -> bool:
        # Warning dict has 'level' as string "2", "3", "4"
        try:
            return int(warning.get("level", "0")) >= min_level
        except Exception:
            return False

    def process_alerts(self, alerts: List[Dict]) -> List[Dict]:
        """
        Transform raw alerts into the standardized output format, skipping duplicates.
        Only warnings for Spain with awareness level >= 2 are included.
        """
        already_processed = self.load_preprocessed_keys()
        processed = []
        for alert in alerts:
            key = alert.get(self.unique_key)
            if key in already_processed:
                logging.debug(f"Skipping already processed alert: {key}")
                continue

            region = None
            title = alert.get("title", "")
            if "MeteoAlarm" in title:
                region = title.replace("MeteoAlarm", "").strip()
            if not region and alert.get("link"):
                m = re.search(r"region=(\w+)", alert["link"])
                if m:
                    region = m.group(1)

            pubdate = alert.get("pubDate")
            description = alert.get("description", "")
            warnings = self.parse_description(description)

            # Check if alert is for Spain (title or region code ES)
            if not self.is_alert_in_spain(alert):
                logging.debug(f"Skipping alert not for Spain: {key} with region '{region}' and title '{title}'")
                continue

            for w in warnings:
                # Only process warnings with level >= 2
                if not self.is_severe(w):
                    logging.debug(f"Skipping warning with low severity level {w.get('level')} for alert {key}")
                    continue
                processed.append({
                    "source": "MeteoAlarm",
                    "alert_type": f"type_{w['type']}",
                    "title": title,
                    "description": f"Level {w['level']} warning, type {w['type']}.",
                    "event_datetime": w["from"],
                    "location": region,
                    "severity": w["level"],
                    "magnitude": None,
                    "link": alert.get("link"),
                    self.unique_key: key,
                    "impacts": "",
                    "valid_from": w["from"],
                    "valid_to": w["until"],
                    "tags": [],
                    "extra_data": {
                        "warning_type": w["type"]
                    }
                })
                logging.info(f"Processed new Meteoalarm alert with key: {key} [{w['from']} - {w['until']}]")
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
    def parse_description(description: str):
        """
        Parse the HTML table in 'description' and extract warnings.
        Returns a list of dicts with level, type, from, until.
        """
        soup = BeautifulSoup(description, "html.parser")
        warnings = []
        # Each <tr> with data-awareness info contains a warning
        for tr in soup.find_all("tr"):
            td = tr.find("td", attrs={"data-awareness-level": True, "data-awareness-type": True})
            if td:
                level = td.get("data-awareness-level")
                warning_type = td.get("data-awareness-type")
                next_td = td.find_next_sibling("td")
                # Extract 'From' and 'Until' with regex
                if next_td:
                    text = next_td.get_text()
                    m = re.search(r"From:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2})\s*Until:\s*(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{2}:\d{2})", text)
                    if m:
                        from_dt, until_dt = m.group(1), m.group(2)
                        warnings.append({
                            "level": level,
                            "type": warning_type,
                            "from": from_dt,
                            "until": until_dt
                        })
        return warnings
