import logging
import torch
from sentence_transformers import SentenceTransformer

class Embedder:
    def __init__(self, model_name="paraphrase-multilingual-MiniLM-L12-v2", batch_size=32, device=None, logger=None):
        """
        Args:
            model_name (str): Name of the sentence-transformers model.
            batch_size (int): Number of texts to embed at once.
            device (str or None): Force device ("cpu" or "mps" for Apple Silicon). If None, auto-detect.
            logger (logging.Logger or None): Optional logger for status messages.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.batch_size = batch_size

        # Set device: for Apple Silicon, use "mps" if available, otherwise "cpu"
        if device:
            self.device = device
        else:
            try:

                if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
                    self.device = "mps"
                else:
                    self.device = "cpu"
            except ImportError:
                self.device = "cpu"

        self.logger.info(f"Loading embedding model '{model_name}' on device '{self.device}'")
        self.model = SentenceTransformer(model_name, device=self.device)

    def encode(self, texts):
        """
        Embed a list of texts (batchable).
        Args:
            texts (List[str]): List of texts to embed.
        Returns:
            List[List[float]]: List of embedding vectors.
        """
        self.logger.info(f"Embedding {len(texts)} texts (batch size={self.batch_size})")
        # sentence-transformers handles batching internally via encode(batch, batch_size=...)
        return self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=True,
            normalize_embeddings=True
        )