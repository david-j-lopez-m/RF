"""
noaa_fetcher.py

This script defines a fetcher class to retrieve space weather alerts from NOAA SWPC (Space Weather Prediction Center).
It performs the following tasks:
- Sends a GET request to the configured NOAA SWPC endpoint.
- Parses and filters alerts based on a timestamp to avoid duplicates.
- Extracts structured information from the alert message body.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""

import logging
import re
from datetime import datetime
from pathlib import Path
from config import get_source_config, get_timestamp_format
from utils import save_json
import requests

class NOAAFetcher:
    """Fetcher class to retrieve NOAA space weather alerts and save them locally."""

    def __init__(self, config=None, source_key="noaa_swpc"):
        """Initialize NOAAFetcher with configuration.
            
            Args:
                config (dict): Optional global config dictionary. If None, it will use get_source_config.
                source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url = self.config["url"]
        self.base_path = self.config["base_data_path"] 
        self.output = self.config["output_filename"]
        self.timestamp_format = get_timestamp_format(source_key)
        self.unique_key = self.config.get("unique_key")


    def fetch(self):
        """Fetch NOAA alerts from the configured URL, parse message fields, and save to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Parse and enrich alerts
            new_alerts = []
            for alert in data:
                try:
                    # Parse the message field and enrich the alert
                    parsed = self.parse_message(alert["message"])
                    enriched_alert = {
                        "product_id": alert.get("product_id", ""),
                        "issue_datetime": alert.get("issue_datetime", ""),
                        "message": alert.get("message", ""),
                        # Enriched fields:
                        "alert_type": parsed["alert_type"],
                        "k_index": parsed["k_index"],
                        "valid_from": parsed["valid_from"],
                        "valid_to": parsed["valid_to"],
                        "impacts": parsed["impacts"],
                    }
                    new_alerts.append(enriched_alert)
                except Exception as e:
                    logging.warning(f"[NOAA] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                full_output_path = Path(self.base_path) / self.output
                save_json(new_alerts, full_output_path, unique_key=self.unique_key)
                logging.info(
                    f"[NOAA] Fetched {len(new_alerts)} alerts from {self.url} | Status: {status_code}"
                )
            else:
                logging.info(f"[NOAA] No alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[NOAA] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )

    @staticmethod
    def parse_message(message):
        """
        Extract structured fields from the NOAA alert message string.

        Args:
            message (str): The raw alert message.

        Returns:
            dict: Parsed fields (alert_type, valid_from, valid_to, k_index, impacts, etc.)
        """
        alert_type = None
        valid_from = None
        valid_to = None
        k_index = None
        impacts = None

        # Type of alert (ALERT, WARNING, etc.)
        if "ALERT" in message:
            alert_type = "ALERT"
        elif "WARNING" in message:
            alert_type = "WARNING"

        # Geomagnetic K-index
        k_match = re.search(r"K-index of (\d+)", message)
        if k_match:
            k_index = k_match.group(1)

        # Validity period
        from_match = re.search(r"Valid From:\s*([^\r\n]+)", message)
        if from_match:
            valid_from = from_match.group(1).strip()
        to_match = re.search(r"Valid To:\s*([^\r\n]+)", message)
        if to_match:
            valid_to = to_match.group(1).strip()

        # Potential Impacts
        impacts_match = re.search(r"Potential Impacts:(.+)", message, re.DOTALL)
        if impacts_match:
            impacts = impacts_match.group(1).strip().replace("\r\n", " ").replace("\n", " ")

        return {
            "alert_type": alert_type,
            "k_index": k_index,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "impacts": impacts,
        }
