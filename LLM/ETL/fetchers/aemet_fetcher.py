"""
aemet_fetcher.py

This script defines a fetcher class to retrieve meteorological alerts from AEMET (Agencia Estatal de Meteorología).
It performs the following tasks:
- Sends an authenticated GET request to the AEMET endpoint.
- Downloads the final data file via a second URL provided in the response.
- Parses and filters alerts based on timestamp to avoid duplicates.
- Saves new alerts in JSON format to a local file.
- Logs all relevant events including success, failure, and filtering decisions.
- Stores the most recent timestamp in a text file for future runs.

Intended to be called from a main controller script managing multiple data sources.
"""

import os
import requests
import logging
from pathlib import Path
from datetime import datetime, timezone
from utils import save_json
from config import get_source_config, get_source_timestamp_format
import tarfile
import tempfile
import xml.etree.ElementTree as ET
import shutil


class AEMETFetcher:
    """Fetcher class to retrieve AEMET weather alerts and store them locally."""

    def __init__(self, config=None, source_key="aemet"):
        """
        Initialize AEMETFetcher with configuration settings.
        
        Args:
            config (dict): Optional global config dictionary. If None, it will use get_source_config.
            source_key (str): Key identifying the source in config.
        """
        # Allow passing a preloaded config or fallback to get_source_config
        self.source_key = source_key
        self.config = config[source_key] if config and source_key in config else get_source_config(source_key)
        self.url = self.config["url"]
        self.output = self.config["output_filename"]
        self.token = self.config["token"]
        self.timestamp_format = get_source_timestamp_format(source_key)
        self.unique_key = self.config.get("unique_key", "identifier")
        
    def fetch(self):
        """Fetch AEMET alerts using the provided API token and store them locally."""
        try:
            # First request to get the actual data URL
            headers = {"accept": "application/json", "api_key": self.token}
            initial_response = requests.get(self.url, headers=headers, timeout=10)
            initial_response.raise_for_status()
            response_json = initial_response.json()

            data_url = response_json.get("datos")
            if not data_url:
                logging.error("[AEMET] 'datos' field not found in response.")
                return

            # Second request to get actual alert data
            data_response = requests.get(data_url, timeout=10)
            data_response.raise_for_status()
            #data = data_response.json()
            # Guardar temporalmente
            with tempfile.NamedTemporaryFile(delete=False, suffix=".tar.gz") as tmp_tar:
                tmp_tar.write(data_response.content)
                tar_path = tmp_tar.name

            # Extraer
            extract_path = Path("data/aemet_extracted")
            extract_path.mkdir(parents=True, exist_ok=True)
            with tarfile.open(tar_path, "r:") as tar:
                tar.extractall(path=extract_path)

            new_alerts = []
            for xml_file in extract_path.glob("*.xml"):
                try:
                    alert = self.parse_alert(xml_file)
                    new_alerts.append(alert)
                except Exception as e:
                    logging.warning(f"[AEMET] Skipping file {xml_file.name} due to parsing error: {e}")

            if new_alerts:
                save_json(new_alerts, self.output, source_key=self.source_key, unique_key=self.unique_key)
                logging.info(f"[AEMET] Saved {len(new_alerts)} alerts to {self.output}")
            else:
                logging.info("[AEMET] No new alerts to save.")

            # Clean up extracted XML directory and downloaded tar file
            try:
                shutil.rmtree(extract_path)
                os.remove(tar_path)
            except Exception as cleanup_err:
                logging.warning(f"[AEMET] Cleanup failed: {cleanup_err}")

        except Exception as e:
            status = getattr(e.response, 'status_code', 'N/A') if hasattr(e, 'response') else 'N/A'
            logging.error(
                f"[AEMET] Error fetching from {self.url} | Status: {status} | Exception: {e}"
            )

    @staticmethod
    def parse_alert(xml_path):
                ns = {"cap": "urn:oasis:names:tc:emergency:cap:1.2"}
                tree = ET.parse(xml_path)
                root = tree.getroot()
                info = root.findall("cap:info", ns)[0]  # solo español

                return {
                    "identifier": root.findtext("cap:identifier", default="", namespaces=ns),
                    "sender": root.findtext("cap:sender", default="", namespaces=ns),
                    "sent": root.findtext("cap:sent", default="", namespaces=ns),
                    "status": root.findtext("cap:status", default="", namespaces=ns),
                    "event": info.findtext("cap:event", default="", namespaces=ns),
                    "urgency": info.findtext("cap:urgency", default="", namespaces=ns),
                    "severity": info.findtext("cap:severity", default="", namespaces=ns),
                    "certainty": info.findtext("cap:certainty", default="", namespaces=ns),
                    "onset": info.findtext("cap:onset", default="", namespaces=ns),
                    "expires": info.findtext("cap:expires", default="", namespaces=ns),
                    "headline": info.findtext("cap:headline", default="", namespaces=ns),
                    "description": info.findtext("cap:description", default="", namespaces=ns),
                    "instruction": info.findtext("cap:instruction", default="", namespaces=ns),
                    "area": info.find("cap:area/cap:areaDesc", ns).text if info.find("cap:area/cap:areaDesc", ns) is not None else "",
                    "level": next((p.findtext("cap:value", default="", namespaces=ns)
                                   for p in info.findall("cap:parameter", ns)
                                   if p.findtext("cap:valueName", default="", namespaces=ns) == "AEMET-Meteoalerta nivel"), ""),
                    "parameter": next((p.findtext("cap:value", default="", namespaces=ns)
                                       for p in info.findall("cap:parameter", ns)
                                       if p.findtext("cap:valueName", default="", namespaces=ns) == "AEMET-Meteoalerta parametro"), ""),
                    "probability": next((p.findtext("cap:value", default="", namespaces=ns)
                                         for p in info.findall("cap:parameter", ns)
                                         if p.findtext("cap:valueName", default="", namespaces=ns) == "AEMET-Meteoalerta probabilidad"), "")
                }