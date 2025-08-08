import chromadb
import os
from chromadb.config import Settings
from config import get_vector_db_path, get_batch_size

class ChromaDBHandler:
    def __init__(self):
        # Settings: persist_directory controls where your DB files are stored
        persist_dir = str(get_vector_db_path())
        os.makedirs(persist_dir, exist_ok=True)
        self.client = chromadb.PersistentClient(path=persist_dir)
        self.collection = None
        self.batch_size = get_batch_size()

    def create_or_get_collection(self, name="alerts"):
        self.collection = self.client.get_or_create_collection(name=name)
        # Print the persist directory directly
        print("ChromaDB persist_directory used:", str(get_vector_db_path()))
        print("ChromaDB collections:", self.client.list_collections())
        return self.collection

    def add_alerts(self, embeddings, ids, metadatas, incremental=True):
        """
        embeddings: List[List[float]] (N x vector_dim)
        ids: List[str] (unique string id per embedding)
        metadatas: List[dict] (parallel metadata for each alert)
        incremental: bool (if True, only add new alerts not already in the collection)
        """
        def batch(iterable, n=self.batch_size):
            for i in range(0, len(iterable), n):
                yield iterable[i:i + n]

        if incremental:
            # Retrieve existing IDs from the collection
            existing_data = self.collection.get()
            existing_ids = set(existing_data["ids"])

            # Filter out alerts whose IDs are already in the collection
            new_embeddings = []
            new_ids = []
            new_metadatas = []
            skipped_count = 0
            for emb, id_, meta in zip(embeddings, ids, metadatas):
                if id_ in existing_ids:
                    skipped_count += 1
                else:
                    new_embeddings.append(emb)
                    new_ids.append(id_)
                    new_metadatas.append(meta)

            if new_ids:
                # Add in batches
                for emb_batch, id_batch, meta_batch in zip(batch(new_embeddings), batch(new_ids), batch(new_metadatas)):
                    self.collection.add(
                        embeddings=emb_batch,
                        ids=id_batch,
                        metadatas=meta_batch
                    )
                print(f"Skipped {skipped_count} alerts already in vector DB.")
                print(f"Added {len(new_ids)} new alerts to vector DB.")
            else:
                print(f"Skipped {skipped_count} alerts; no new alerts to add.")
        else:
            # Add all in batches
            for emb_batch, id_batch, meta_batch in zip(batch(embeddings), batch(ids), batch(metadatas)):
                self.collection.add(
                    embeddings=emb_batch,
                    ids=id_batch,
                    metadatas=meta_batch
                )
            print(f"Added {len(ids)} alerts to vector DB.")