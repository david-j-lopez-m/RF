# RF: Natural Hazard Alert Intelligence

This repository powers a pipeline for collecting, processing, and preparing natural hazard alerts, focused on Spain, for domain-specific LLM training and retrieval-augmented generation (RAG) applications.

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
├── environment.yml              # Python dependencies (global or ETL-specific)
├── SOURCES.md                    # Documentation of sources and design notes
└── README.md                     # Project overview and usage

---

## 🚦 Workflow

### 1. Setup

- In ETL, copy `config.example.json` to `config.json` and add your own API keys (do **not** commit real keys).
- Other submodules (Preprocessing, etc.) do not require config files with secrets.- Add your own API keys and credentials to the relevant config (do **not** commit real keys)
- Each submodule is modular and can be run/configured independently

### 2. Data Extraction (ETL)

- Run fetchers in `LLM/ETL/` to gather alerts from:
  - NASA DONKI
  - USGS
  - AEMET 
  - GDACS
  - IGN
  - NOAA
  - FIRMS
  - Meteoalarm *(currently disabled, see SOURCES.md)*
- Each fetcher:
  - Saves alerts as `.json` in `data/alerts/`
  - Tracks latest timestamps and IDs to avoid duplication

### 3. Preprocessing

- Scripts in `LLM/Preprocessing/`:
  - Normalize and unify fields across sources
  - Filter irrelevant or low-impact alerts (e.g., earthquakes < mag 4.0)
  - Export structured, clean `.json` files for vectorization/fine-tuning

### 4. Next Steps

- Build a vector database (e.g., FAISS, Qdrant)
- Design prompts and retrieval strategies for RAG tasks
- Fine-tune your LLM using the prepared dataset

---

## 🎯 Objectives

- Enable robust and accurate detection of alerts relevant to scientific operations
- Prepare the ground for a RAG pipeline with strong recall and reasoning
- Support flexible, transparent, and reproducible data workflows

---

## 🔗 Sources

See [`SOURCES.md`](./SOURCES.md) for full details of each data provider, rationale, and current activation status.

---

## 🛠️ Requirements

Dependencies are managed using `environment.yml`. Create the conda environment with:

```bash
conda env create -f environment.yml
```

Alternatively, using mamba:

```bash
mamba env create -f environment.yml
```