# Preprocessing Module

This module transforms raw JSON alert files into structured documents suitable for vector storage and RAG-based applications.

## Goals
- Normalize and clean alert data.
- Filter for scientific relevance (magnitude, region).
- Convert to unified document format (dict).
- Save structured documents as JSON ready for indexing.

Each alert source will have its own preprocessor script.