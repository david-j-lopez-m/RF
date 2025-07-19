import json
import pathlib

# Define the root of the project (assume config.py is at RF/LLM/Vectorization/)
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

def get_preprocessed_input_path():
    # Path to preprocessed alert JSONs (for vectorization)
    rel_path = CONFIG.get("input_path", "data/preprocessed/")
    return get_absolute_path(rel_path)

def get_vector_db_path():
    # Where to store ChromaDB or other vector DB
    rel_path = CONFIG.get("output_db_path", "data/vectors/alerts_chroma_db")
    return get_absolute_path(rel_path)

def get_model_name():
    # Embedding model
    return CONFIG.get("model_name", "paraphrase-multilingual-MiniLM-L12-v2")

def get_fields_to_embed():
    # List of fields to concatenate for embedding
    return CONFIG.get("fields_to_embed", ["title", "description"])

def get_batch_size():
    return CONFIG.get("batch_size", 32)

def get_vector_dim():
    return CONFIG.get("vector_dim", 384)