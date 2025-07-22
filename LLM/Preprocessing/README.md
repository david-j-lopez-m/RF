# Preprocessing Module

This module transforms raw JSON alert files into structured documents suitable for vector storage and RAG-based applications.

## Goals
- Normalize and clean alert data from multiple sources.
- Filter for scientific or operational relevance (e.g., magnitude, region, severity).
- Convert data to a unified document format (Python dict/JSON).
- Save structured documents as JSON, ready for downstream indexing and retrieval.

## Workflow

1. **Raw Data Ingestion:**  
   Raw alert files are downloaded by ETL fetchers and stored as `.json` files in the `/data/alerts/` directory, organized by source.  
   All sources—regardless of original format (XML, CSV, API, etc.)—are converted and archived as JSON for consistency and easier downstream processing.

2. **Source-specific Preprocessors:**  
   Each source (AEMET, IGN, GDACS, etc.) has its own script in this folder, responsible for:
   - Parsing source format and handling missing fields
   - Extracting and mapping metadata (type, severity, region, etc.)
   - Applying source-specific filters (e.g., minimum earthquake magnitude, Spanish territory, alert severity)
   - Outputting standardized dicts/JSON, always including a unique key

3. **Filtering Logic:**  
   - Only alerts that match configured criteria (e.g., magnitude ≥ 4 for IGN earthquakes, Spain location for GDACS, etc.) are included.
   - Filters can be adjusted per source as needed.

4. **Structured Output:**  
   Processed alerts are saved as JSON files in `/data/preprocessed/`, one per source and batch.  
   These files are ready for vectorization and use in RAG (retrieval-augmented generation) pipelines.

## Adding a New Source

To add support for a new alert provider:
- Create a new `*_preprocessing.py` script following the pattern of existing ones.
- Implement extraction, cleaning, and filtering as needed.
- Document any special fields or mapping logic in the script and in this README.

## Current Preprocessors

| Source    | Script                         | Notes / Criteria            |
|-----------|-------------------------------|-----------------------------|
| AEMET     | `aemet_preprocessing.py`      | Real-time Spanish alerts    |
| IGN       | `ign_alerts_preprocessing.py` | Earthquakes, mag ≥ 4.0      |
| GDACS     | `gdacs_preprocessing.py`      | Spain + relevant types      |
| (Others)  | ...                           | ...                         |

*For full source list and current status, see [../SOURCES.md](../SOURCES.md).*

## Vectorization Compatibility

To ensure preprocessed alert data can be directly ingested by vector databases (such as ChromaDB), all preprocessors enforce:

- **Scalar Metadata:**  
  All top-level fields in each processed alert are limited to scalar types (`str`, `int`, `float`, `bool`, or `None`). Fields that are naturally lists or dicts (e.g., `tags`, `extra_data`) are automatically serialized to JSON strings according to the `serialization_rules` in `config.json`.

- **Schema Enforcement:**  
  Every output alert strictly follows the unified `output_schema` from `config.json`. Missing fields are filled with `None`, guaranteeing every alert shares the same structure.

- **Centralized & Automated:**  
  Both serialization and schema logic are defined in configuration, so changes propagate across all sources. Adding a field to `serialization_rules` or the output schema ensures all preprocessors handle it correctly.

These guarantees make all preprocessed data suitable for downstream vectorization and RAG pipelines, and compatible with vector DB metadata requirements.

## Design Notes

- All preprocessors should be idempotent: running multiple times on the same data does not create duplicates.
- All output alerts should have a unique identifier (e.g., GUID, hash, or composite key).
- Filters and mappings should be easy to adjust for scientific evolution.