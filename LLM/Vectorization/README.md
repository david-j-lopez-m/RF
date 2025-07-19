# Vectorization Module

This module transforms preprocessed alert documents into vector embeddings and stores them in a vector database for fast semantic search and RAG applications.

## Overview

- **Inputs:** Preprocessed JSON alert files from the `/data/preprocessed/` directory.
- **Embedding:** Uses a Sentence Transformers model (default: `paraphrase-multilingual-MiniLM-L12-v2`) to convert alert text into dense vector representations.
- **Storage:** Stores vectors and alert metadata in a vector database (default: ChromaDB).
- **Config:** Pipeline parameters and paths are managed in `config.json` and loaded via `config.py`.

## Workflow

1. **Load Configuration**
   - Reads settings from `config.json` (input path, model, DB location, etc.).
2. **Load Preprocessed Alerts**
   - Reads structured alert data from JSON files.
3. **Embed Documents**
   - Concatenates selected fields (e.g., title + description) and encodes to vectors using the configured model.
4. **Store Embeddings**
   - Inserts vectors and metadata into the vector DB.
5. **Ready for Retrieval**
   - Alerts can now be semantically searched or retrieved for RAG pipelines (e.g., with LangChain).

## Configuration

- Edit `config.json` to set:
  - `input_path`: Folder containing preprocessed JSONs.
  - `output_db_path`: Where to store the vector database.
  - `model_name`: Sentence Transformers model to use.
  - `fields_to_embed`: List of fields to concatenate for embedding.
  - `batch_size`: Batch size for embedding (trade-off: speed vs. memory).
  - `vector_dim`: Output dimension of the embedding model.

## Embedding Model: `paraphrase-multilingual-MiniLM-L12-v2`

We use the [paraphrase-multilingual-MiniLM-L12-v2](https://www.sbert.net/docs/pretrained_models.html#multilingual-models) model from the Sentence Transformers library for generating alert embeddings.

**Why this model?**
- **Multilingual:** Trained on more than 50 languages, including both Spanish and English—matching the languages in our alert sources.
- **Compact and fast:** Delivers strong semantic performance while remaining efficient on CPU and Apple Silicon (M1/M2) hardware—no GPU required.
- **Semantic quality:** Accurately maps similar alerts (even across languages) to nearby vectors, ideal for cross-lingual semantic search and RAG.
- **Widely adopted:** Used in both industry and research for scalable, production-grade vector search.

**Rationale:**  
Given our alert sources are in both Spanish and English, and our development hardware is Mac M1 without discrete GPU, this model offers the best balance between speed, memory usage, and semantic quality. It enables robust cross-lingual retrieval without requiring cloud APIs or GPU infrastructure.