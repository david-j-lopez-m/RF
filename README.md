# RF: Natural Hazard Alert Intelligence

This repository powers the collection and preparation of natural hazard alerts for fine-tuning a domain-specific LLM using RAG (Retrieval-Augmented Generation).

---

## Project Structure
RF/
├── LLM/
│   ├── ETL/
│   │   ├── config.example.json   # Example ETL config (copy as config.json)
│   │   ├── ...                   # ETL scripts, fetchers, utils
│   ├── Preprocessing/
│   │   ├── config.example.json   # Example Preprocessing config (copy as config.json)
│   │   ├── ...                   # Preprocessing scripts, normalizers, utils
│   └── ...                       # (Other submodules: Training, Evaluation, etc.)
├── data/                         # Raw and processed alert data by date/source
├── requirements.txt              # Python dependencies (global or ETL-specific)
├── source.md                     # Documentation of sources and design notes
└── README.md                     # Project overview and usage
---

## 🚦 Phase 1: Fine-tuning Data Preparation

**Goal:** Prepare a clean, high-quality dataset of real-world alerts, focusing on significant and relevant events for scientific use cases.

### 1. Setup

- For each submodule (e.g., ETL, Preprocessing), copy `config.example.json` to `config.json`
- Add your own API keys and credentials to the relevant config (do **not** commit real keys)
- Each submodule is designed to be run and configured independently

### 2. Data Extraction (ETL)

- Run fetchers in `LLM/ETL/` to gather alerts from:
  - NASA DONKI
  - USGS
  - AEMET
  - GDACS
  - IGN
  - FIRMS
  - Meteoalarm
- Each fetcher:
  - Saves alerts as `.json` in `data/alertas/{YYYY-MM-DD}/`
  - Tracks latest timestamps to avoid duplication

### 3. Preprocessing

- Scripts in `LLM/Preprocessing/`:
  - Normalize and unify fields across sources
  - Filter irrelevant or low-impact alerts (if needed)
  - Export a final dataset ready for vectorization/fine-tuning

### 4. Next Steps

- Build a vector database (e.g., FAISS, Qdrant)
- Design prompts for retrieval-augmented tasks
- Fine-tune your LLM on the preprocessed dataset

---

## 🎯 Objectives

- Enable robust and accurate detection of alerts relevant to scientific operations
- Prepare the ground for a RAG pipeline with strong recall and reasoning
- Support flexible, transparent, and reproducible data workflows

---

## 🔗 Sources

See [`source.md`](./source.md) for full details of each data provider, inclusion rationale, and field documentation.

---

## 🛠️ Requirements

Install Python packages with:

```bash
pip install -r requirements.txt