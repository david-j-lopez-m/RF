import logging
import os
import time
from load_data import AlertLoader
from embedder import Embedder
from db_chroma import ChromaDBHandler

    
def run_all_vectorization():
    # Configure logging
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'etl.log')

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logging.info(f"Loading Preprocessed data")
    alert_loader = AlertLoader()
    alerts = alert_loader.load_alerts()
    logging.info(f"Number of alerts loaded: {len(alerts)}")

    logging.info(f"Starting Embedding")
    embedder = Embedder()
    start = time.time()
    alerts_embedded = embedder.encode(alerts)
    logging.info(f"Embedding completed in {time.time() - start:.2f} seconds")

    logging.info(f"Creating Chroma db")
    db = ChromaDBHandler()
    collection = db.create_or_get_collection(name="alerts")

    # Generate unique IDs for each alert (can be just an integer or from alert data)
    ids = [f"alert_{i:05d}" for i in range(len(alerts))]
    # Use the original alerts as metadata (or you can extract specific fields if you want)
    metadatas = alerts  
    
    for i, alert in enumerate(alerts):
        for k, v in alert.items():
            if isinstance(v, (list, dict)):
                print(f"Alert {i} ('{alert.get('title', 'no-title')}') - field '{k}' has type {type(v).__name__}: {v}")
    db.add_alerts(alerts_embedded, ids, metadatas)
    logging.info(f"Inserted {len(ids)} vectors into ChromaDB.")



    # After creating the collection
    db.collection.delete()  # CAREFUL: This deletes all entries in the collection!
    


if __name__ == "__main__":
    run_all_vectorization()

    
    # TODO: Add embedding step here
    
    # TODO: Insert embeddings into the database here
    
    # TODO: Further processing steps here
