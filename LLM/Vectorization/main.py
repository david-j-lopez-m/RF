import logging
import os
import time
from load_data import AlertLoader
from embedder import Embedder
    
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
    


if __name__ == "__main__":
    run_all_vectorization()

    
    # TODO: Add embedding step here
    
    # TODO: Insert embeddings into the database here
    
    # TODO: Further processing steps here
