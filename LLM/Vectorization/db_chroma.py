import chromadb
import os
from chromadb.config import Settings
from config import get_vector_db_path

class ChromaDBHandler:
    def __init__(self):
        # Settings: persist_directory controls where your DB files are stored
        persist_dir = str(get_vector_db_path())
        os.makedirs(persist_dir, exist_ok=True)
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.collection = None

    def create_or_get_collection(self, name="alerts"):
        self.collection = self.client.get_or_create_collection(name=name)
        # Print the persist directory directly
        print("ChromaDB persist_directory used:", str(get_vector_db_path()))
        print("ChromaDB collections:", self.client.list_collections())
        return self.collection

    def add_alerts(self, embeddings, ids, metadatas):
        """
        embeddings: List[List[float]] (N x vector_dim)
        ids: List[str] (unique string id per embedding)
        metadatas: List[dict] (parallel metadata for each alert)
        """
        if self.collection is None:
            raise RuntimeError("Collection not created! Call create_or_get_collection first.")
        self.collection.add(
            embeddings=embeddings,
            ids=ids,
            metadatas=metadatas
        )
        print(f"Added {len(ids)} alerts to vector DB.")