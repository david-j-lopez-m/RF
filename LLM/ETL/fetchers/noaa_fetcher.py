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
from config import get_source_config, get_source_timestamp_format
from utils import save_json
import requests

class NOAAFetcher:
    """Fetcher class to retrieve NOAA space weather alerts and save them locally."""

    def __init__(self):
        """Initialize NOAAFetcher with configuration settings."""
        self.config = get_source_config("noaa_swpc")
        self.url = self.config.get("url")
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("noaa_swpc")

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

    def fetch(self):
        """Fetch NOAA alerts from the configured URL, parse message fields, and save to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            ts_path = Path(self.config.get("last_timestamp_path", "data/alertas/noaa_last_timestamp.txt"))
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format + " UTC")

            # Filter and parse new alerts
            new_alerts = []
            for alert in data:
                try:
                    alert_ts = datetime.strptime(alert["issue_datetime"], self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
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
                save_json(new_alerts, self.output)
                logging.info(
                    f"[NOAA] Fetched {len(new_alerts)} new alerts from {self.url} | Status: {status_code}"
                )

                # Save latest timestamp
                latest_ts = max(
                    datetime.strptime(alert["issue_datetime"], self.timestamp_format)
                    for alert in new_alerts
                )
                ts_path.parent.mkdir(parents=True, exist_ok=True)
                ts_path.write_text(latest_ts.strftime(self.timestamp_format + " UTC"))
                logging.info(f"[NOAA] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[NOAA] No new alerts to save from {self.url}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[NOAA] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )