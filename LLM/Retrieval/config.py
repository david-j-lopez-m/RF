import json
import pathlib

# Define the root of the project (assume config.py is at RF/LLM/Retrieval/)
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]  # This points to RF/

CONFIG_PATH = PROJECT_ROOT / "LLM" / "Preprocessing" / "config.json"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

CONFIG = load_config()

def get_retrieval_config():
    # Return the retrieval config block
    return CONFIG.get("retrieval", {})

def get_vector_db_path():
    # Path to the vector database (should match vectorization config)
    retrieval_conf = get_retrieval_config()
    rel_path = retrieval_conf.get("vector_db_path", "data/vectors")
    return PROJECT_ROOT / rel_path

def get_collection_name():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("collection_name", "alerts")

def get_top_k():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("top_k", 5)

def get_search_distance_metric():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("search_distance_metric", "cosine")

def get_min_score_threshold():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("min_score_threshold", 0.4)

def get_default_query_language():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("default_query_language", "en")

def get_fields_to_return():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("fields_to_return", ["title", "description", "event_datetime", "severity", "location"])

def get_embedder_model():
    retrieval_conf = get_retrieval_config()
    return retrieval_conf.get("embedder_model", "paraphrase-multilingual-MiniLM-L12-v2")