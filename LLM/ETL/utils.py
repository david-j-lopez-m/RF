# utils.py – Common utilities for alert processing

import os
import json
from datetime import datetime
from config import get_base_path
import logging

def create_daily_directory(base_path=None):
    """
    Create a subfolder named with today's date inside the base data directory.

    Args:
        base_path (str, optional): Override base path from config.

    Returns:
        str: Full path to today's directory.
    """
    if base_path is None:
        base_path = get_base_path()
    today = datetime.now().strftime("%Y-%m-%d")
    dir_path = os.path.join(base_path, today)
    os.makedirs(dir_path, exist_ok=True)
    return dir_path

def save_json(data, filename, base_path=None):
    """
    Save a JSON-serializable object to a file in today's date folder.

    Args:
        data (dict or list): JSON data to be saved.
        filename (str): File name to save (e.g. 'noaa_alerts.json').
        base_path (str, optional): Override base path from config.

    Returns:
        str: Full path to the saved file.
    """
    dir_path = create_daily_directory(base_path)
    file_path = os.path.join(dir_path, filename)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"[utils] Saved file to: {file_path}")
    return file_path