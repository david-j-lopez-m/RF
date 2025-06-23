# Preprocessed Document Schema

This document defines the standardized format that all alerts should follow after preprocessing. This format is optimized for indexing into a vector store and for use in Retrieval-Augmented Generation (RAG) applications.

## Unified Alert Schema

Each alert will be converted into a dictionary with the following fields:

- `source`: **(str)** – Identifier of the original data source (e.g., "USGS", "GDACS", "AEMET").
- `alert_type`: **(str)** – General category (e.g., "earthquake", "fire", "storm", "temperature", "space_weather").
- `title`: **(str)** – Title or summary of the alert.
- `description`: **(str)** – Full description of the event.
- `event_datetime`: **(datetime str)** – Timestamp when the event occurred or started (ISO 8601).
- `location`: **(str)** – Geographic region (textual).
- `severity`: **(optional, str)** – Severity level if available (e.g., "moderate", "high", "green", "red").
- `magnitude`: **(optional, float)** – Magnitude if applicable (e.g., earthquakes, solar flares).
- `link`: **(optional, str)** – URL to full alert or external details. This field is preserved for traceability but will not be used during model training.

## Notes

- All timestamps should be in **UTC** and ISO format (`%Y-%m-%dT%H:%M:%SZ`).
- Missing fields should be set as `null` or omitted if not applicable.
- Alerts that do not meet the minimum relevance criteria can be skipped or tagged accordingly.
- The `link` field is excluded from the model's input; it is retained for debugging and traceability only.

## Example

```json
{
  "source": "USGS",
  "alert_type": "earthquake",
  "title": "M 5.1 - Central California",
  "description": "An earthquake of magnitude 5.1 occurred...",
  "event_datetime": "2025-06-19T18:22:00Z",
  "location": "Central California",
  "severity": "moderate",
  "magnitude": 5.1,
  "link": "https://earthquake.usgs.gov/earthquakes/eventpage/ci39912355"
}
```