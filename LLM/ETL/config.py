import json
import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]

CONFIG_PATH = pathlib.Path(__file__).parent / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_absolute_path(rel_path):
    """
    Convert relative path (from config.json) to absolute from PROJECT_ROOT.
    """
    return (PROJECT_ROOT / rel_path).resolve()

def get_source_config(source_key):
    """
    Get the config dict for a given source (e.g., 'noaa_swpc').
    """
    return CONFIG[source_key]

def get_input_path(source_key):
    """
    Get the absolute path to the input file for a source.
    """
    rel_path = CONFIG[source_key].get("base_data_path", "data/alerts")
    filename = CONFIG[source_key]["output_filename"]
    return get_absolute_path(rel_path) / filename

def get_output_path(source_key, output_name=None):
    """
    Get the absolute output path for a source (or specific name).
    """
    rel_path = CONFIG[source_key].get("base_data_path", "data/alerts")
    filename = output_name if output_name else CONFIG[source_key]["output_filename"]
    return get_absolute_path(rel_path) / filename

def get_unique_key(source_key):
    """
    Get the unique key field name for a given source.
    """
    return CONFIG[source_key].get("unique_key")

def get_timestamp_format(source_key):
    """
    Get the timestamp format string for a given source.
    """
    return CONFIG[source_key].get("timestamp_format")

def get_incremental_flag(source_key):
    """
    Retrieve the 'incremental' boolean flag from config.json.
    Returns True if not present.
    """
    return CONFIG[source_key].get("incremental", True)