# utils.py – Common utilities for alert processing

import os
import json
from datetime import datetime
import logging

# def create_daily_directory(base_path=None):
#     """
#     Create a subfolder named with today's date inside the base data directory.

#     Args:
#         base_path (str, optional): Override base path from config.

#     Returns:
#         str: Full path to today's directory.
#     """
#     if base_path is None:
#         base_path = get_base_path()
#     today = datetime.now().strftime("%Y-%m-%d")
#     dir_path = os.path.join(base_path, today)
#     os.makedirs(dir_path, exist_ok=True)
#     return dir_path

def save_json(data, file_path, unique_key=None):
    """
    Save or append JSON data to a file in the base data directory for a specific source.
    If the file exists, appends new unique entries (by unique_key).
    If unique_key is None, just appends all.

    Args:
        data (list): List of alert dicts to save.
        filename (str): File name (e.g. 'noaa_alerts.json').
        source_key (str, optional): Key to get base path from config for each source.
        unique_key (str, optional): Key to use for deduplication.

    Returns:
        str: Full path to the saved file.
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    existing = []
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except Exception:
                existing = []

    if unique_key:
        all_alerts = {a[unique_key]: a for a in existing if unique_key in a}
        for alert in data:
            if unique_key in alert:
                all_alerts[alert[unique_key]] = alert
            else:
                all_alerts[str(alert)] = alert
        final = list(all_alerts.values())
    else:
        final = existing + data

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)
    logging.info(f"[utils] Saved to: {file_path}")
    return file_path