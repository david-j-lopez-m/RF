"""
nasa_donki_fetcher.py

Fetcher class to retrieve alerts from NASA's DONKI (Database Of Notifications, Knowledge, Information).
Features:
- Sends a GET request to the NASA DONKI endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended for use in a modular ETL pipeline.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from config import get_source_config, get_source_timestamp_format
from utils import save_json
import requests

class NASADONKIFetcher:
    """Fetcher class to retrieve NASA DONKI space weather alerts and save them locally."""

    def __init__(self, config=None, source_key="nasa_donki"):
        """Initialize NASADONKIFetcher with configuration.
            
            Args:
                config (dict): Optional global config dictionary. If None, it will use get_source_config.
                source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format(source_key)
        self.unique_key = self.config.get("unique_key")

    def fetch(self):
        """Fetch NOAA alerts from the configured URL, parse message fields, and save to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Parse all alerts
            parsed_alerts = []
            for alert in data:
                parsed = self.parse_message(alert.get("messageBody", ""))
                enriched_alert = {
                    "message_id": alert.get("messageID", ""),
                    "issue_datetime": alert.get("messageIssueTime", ""),
                    "body": alert.get("messageBody", ""),
                    "url": alert.get("messageURL", ""),
                    # Enriched fields:
                    "alert_type": parsed["alert_type"],
                    "event_summary": parsed["event_summary"],
                }
                parsed_alerts.append(enriched_alert)

            if parsed_alerts:
                save_json(parsed_alerts, self.output, source_key=self.source_key, unique_key=self.unique_key)
                logging.info(
                    f"[DONKI] Fetched {len(parsed_alerts)} alerts from {self.url} | Status: {status_code}"
                )
            else:
                logging.info(f"[DONKI] No alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[DONKI] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )

    @staticmethod
    def parse_message(message):
        """
        Extract structured fields from the DONKI alert message string.

        Args:
            message (str): The raw alert message.

        Returns:
            dict: Parsed fields (alert_type, event_summary, etc.)
        """
        # Example parsing (you can expand this as needed):
        alert_type = None
        event_summary = None

        # Find type in the message body (very basic, adjust as needed)
        if "Radiation Belt Enhancement" in message:
            alert_type = "Radiation Belt Enhancement"
        elif "CME" in message:
            alert_type = "Coronal Mass Ejection"
        elif "Solar Flare" in message:
            alert_type = "Solar Flare"

        # For summary, grab first few lines or a summary section
        summary_lines = message.strip().split("\n")
        event_summary = ""
        for line in summary_lines:
            if "Summary" in line or "Significantly elevated" in line:
                event_summary = line.strip()
                break

        return {
            "alert_type": alert_type,
            "event_summary": event_summary,
        }