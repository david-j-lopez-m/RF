import json
import pathlib

CONFIG_PATH = pathlib.Path(__file__).parent / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_base_path():
    raw_path = CONFIG.get("base_data_path", "./data/alertas")
    return pathlib.Path(__file__).parent.parent.parent / raw_path

def get_source_config(source_key):
    return CONFIG.get(source_key, {})

def get_timestamp_format():
    return CONFIG.get("timestamp_format", "%Y-%m-%d %H:%M:%S.%f")

def get_source_timestamp_format(source_key):
    return get_source_config(source_key).get("timestamp_format", get_timestamp_format())