"""
firms_fetcher.py

Fetcher class to retrieve wildfire alerts from NASA FIRMS (Fire Information for Resource Management System).
- Sends a GET request to the NASA FIRMS CSV endpoint.
- Parses the CSV and stores new wildfire events as JSON, grouped by date.
- Saves the latest timestamp for incremental fetches.
"""

import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import io
from utils import save_json
from config import get_source_config, get_source_timestamp_format

class FIRMSFetcher:
    """Fetcher class to retrieve and store NASA FIRMS wildfire alerts from CSV."""

    def __init__(self):
        """Initialize FIRMSFetcher with configuration."""
        self.config = get_source_config("firms")
        self.url_template = self.config["url_template"]
        self.map_key = self.config["MAP_KEY"]
        self.source = self.config["SOURCE"]
        self.day_range = self.config["DAY_RANGE"]
        self.output = self.config["output_filename"]
        self.base_data_path = self.config.get("base_data_path", "data/alertas")
        self.timestamp_format = get_source_timestamp_format("firms")

    def fetch(self):
        """
        Fetch FIRMS wildfire alerts from CSV, parse, and save as JSON.
        Only new alerts (after last timestamp) are stored.
        """
        try:
            # Build the API URL using config and template
            url = self.url_template.format(
                MAP_KEY=self.map_key,
                SOURCE=self.source,
                DAY_RANGE=self.day_range
            )
            r = requests.get(url, timeout=20)
            r.raise_for_status()

            # Parse CSV to DataFrame
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty:
                logging.info("[FIRMS] No data found in CSV.")
                return

            # Load last processed timestamp for incremental updates
            ts_path = Path(self.base_data_path) / "firms_last_timestamp.txt"
            last_ts = None
            if ts_path.exists():
                with ts_path.open("r") as f:
                    last_ts = datetime.strptime(f.read().strip(), self.timestamp_format)

            new_alerts = []
            latest_ts = last_ts

            for _, row in df.iterrows():
                alert = self.parse_alert(row)
                event_datetime = alert.get("event_datetime")
                try:
                    alert_ts = datetime.strptime(event_datetime, self.timestamp_format)
                    if not last_ts or alert_ts > last_ts:
                        new_alerts.append(alert)
                        if not latest_ts or alert_ts > latest_ts:
                            latest_ts = alert_ts
                except Exception as e:
                    logging.warning(f"[FIRMS] Skipping alert with invalid timestamp: {e}")

            if new_alerts:
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                out_dir = Path(self.base_data_path) / now_str
                out_dir.mkdir(parents=True, exist_ok=True)
                #out_path = out_dir / self.output
                save_json(new_alerts, self.output)
                logging.info(f"[FIRMS] Fetched {len(new_alerts)} new wildfire alerts from {url}")
                # Save latest timestamp if new alerts fetched
                if latest_ts:
                    ts_path.parent.mkdir(parents=True, exist_ok=True)
                    ts_path.write_text(latest_ts.strftime(self.timestamp_format))
            else:
                logging.info(f"[FIRMS] No new wildfire alerts to save from {url}")

        except Exception as e:
            logging.error(f"[FIRMS] Error fetching from {url} | Exception: {e}")

    @staticmethod
    def parse_alert(row):
        """
        Extract relevant fields from a single FIRMS CSV row.

        Args:
            row (pd.Series): DataFrame row with alert fields.

        Returns:
            dict: Parsed alert fields.
        """
        # event_datetime is a combination of acq_date and acq_time (format: %Y-%m-%d %H%M)
        acq_date = str(row.get('acq_date', ''))
        acq_time = str(row.get('acq_time', ''))
        event_datetime = f"{acq_date} {acq_time}"
        return {
            "event_datetime": event_datetime,
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "brightness": row.get("brightness"),
            "confidence": row.get("confidence"),
            "satellite": row.get("satellite"),
            "instrument": row.get("instrument"),
            "daynight": row.get("daynight"),
            "frp": row.get("frp"),
            "alert_type": "wildfire"
        }