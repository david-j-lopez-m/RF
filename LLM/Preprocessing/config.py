import json
import pathlib

# Define the root of the project (assume config.py is at RF/LLM/Preprocessing/)
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]  # This points to RF/

CONFIG_PATH = pathlib.Path(__file__).parent / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_absolute_path(rel_path):
    """
    Given a relative path (from config.json), return its absolute path from project root.
    """
    return PROJECT_ROOT / rel_path

def get_base_path():
    # Base input path for all alerts (raw source data)
    rel_path = CONFIG.get("base_data_path", "data/alerts")
    return get_absolute_path(rel_path)

def get_preprocessed_path():
    # Output path for preprocessed data
    rel_path = CONFIG.get("preprocessed_output_path", "data/preprocessed")
    return get_absolute_path(rel_path)

def get_source_config(source_key):
    # Get config for a specific source
    return CONFIG["sources"].get(source_key, {})

def get_source_input_path(source_key):
    # Get absolute input path for a given source
    rel_path = CONFIG["sources"][source_key]["input_path_template"]
    return get_absolute_path(rel_path)

def get_source_output_path(source_key):
    rel_path = CONFIG["sources"][source_key]["output_path_template"]
    return get_absolute_path(rel_path)

def get_serialization_rules():
    # Return the serialization rules
    return CONFIG.get("serialization_rules", {})

def get_output_schema():
    # Return the output schema for the target unified JSON
    return CONFIG.get("output_schema", {})

def get_field_format(field):
    # Get formatting string for a specific field, if defined
    return CONFIG.get("field_formats", {}).get(field, None)

def get_incremental_flag(source_key):
    """
    Retrieve the 'incremental' boolean flag from config.json.
    Returns False if not present.
    """
    return CONFIG["sources"].get(source_key, {}).get("incremental", True)