import json
import pathlib

CONFIG_PATH = pathlib.Path(__file__).parent / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_base_path():
    raw_path = CONFIG.get("base_data_path", "./data/alerts")
    return pathlib.Path(__file__).parent.parent.parent / raw_path

def get_preprocessed_path():
    raw_path = CONFIG.get("preprocessed_output_path", "./data/preprocessed")
    return pathlib.Path(__file__).parent.parent.parent / raw_path

def get_source_config(source_key):
    return CONFIG["sources"].get(source_key, {})

def render_path(template: str, date: str, datetime_str: str) -> str:
    """
    Replace placeholders in the path template with provided values.
    """
    return template.format(date=date, datetime=datetime_str)

def get_output_schema():
    return CONFIG.get("output_schema", {})

def get_field_format(field):
    return CONFIG.get("field_formats", {}).get(field, None)