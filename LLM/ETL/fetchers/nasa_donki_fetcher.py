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

    def __init__(self):
        """Initialize NASADONKIFetcher with configuration settings."""
        self.config = get_source_config("nasa_donki")
        self.url = self.config.get("url")
        self.output = self.config["output_filename"]
        self.timestamp_format = get_source_timestamp_format("nasa_donki")
        self.ts_path = Path(self.config.get("last_timestamp_path", "data/alertas/nasa_donki_last_timestamp.txt"))

    def fetch(self):
        """Fetch NOAA alerts from the configured URL, parse message fields, and save to a JSON file."""
        try:
            r = requests.get(self.url, timeout=10)
            status_code = r.status_code
            r.raise_for_status()
            data = r.json()

            # Load last processed timestamp
            last_ts = None
            if self.ts_path.exists():
                with self.ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)

            # Filter and parse new alerts
            new_alerts = []
            for alert in data:
                try:
                    alert_ts = datetime.strptime(alert["messageIssueTime"], self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
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
                        new_alerts.append(enriched_alert)
                except Exception as e:
                    logging.warning(f"[DONKI] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                save_json(new_alerts, self.output)
                logging.info(
                    f"[DONKI] Fetched {len(new_alerts)} new alerts from {self.url} | Status: {status_code}"
                )

                # Save latest timestamp
                latest_ts = max(
                    datetime.strptime(alert["issue_datetime"], self.timestamp_format)
                    for alert in new_alerts
                )
                self.ts_path.parent.mkdir(parents=True, exist_ok=True)
                self.ts_path.write_text(latest_ts.strftime(self.timestamp_format))
                logging.info(f"[DONKI] Saved latest timestamp: {latest_ts}")
            else:
                logging.info(f"[DONKI] No new alerts to save from {self.url}")

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