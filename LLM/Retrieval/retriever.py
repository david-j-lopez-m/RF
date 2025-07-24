from config import get_vector_db_path, get_collection_name, get_top_k, get_embedder_model, get_fields_to_return
import chromadb
from sentence_transformers import SentenceTransformer

class AlertRetriever:
    def __init__(self):
        self.client = chromadb.PersistentClient(path=str(get_vector_db_path()))
        self.collection = self.client.get_or_create_collection(name=get_collection_name())
        self.model = SentenceTransformer(get_embedder_model())
        self.fields_to_return = get_fields_to_return()

    def search(self, query, top_k=None):
        embedding = self.model.encode([query])
        top_k = top_k or get_top_k()
        results = self.collection.query(
            query_embeddings=embedding,
            n_results=top_k
        )
        # Format output: return only selected fields
        alerts = []
        for meta in results['metadatas'][0]:
            alerts.append({f: meta.get(f, None) for f in self.fields_to_return})
        # Add scores if you wish
        return alerts