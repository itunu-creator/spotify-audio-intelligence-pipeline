# Spotify Audio Intelligence & Listening Habit Pipeline

## Project Overview
An automated data pipeline designed to parse, validate, and store music listening logs and track audio features (BPM, danceability, energy). This project models how personal consumption metrics can be extracted from streaming APIs, filtered for anomalies, and stored in relational schemas for trend analytics.

## Architecture
1. **Extraction:** Ingests nested streaming event logs containing track metadata and audio vectors.
2. **Data Cleaning & Schema Enforcement:**
   * Validates positive tempos ($>40$ BPM).
   * Enforces normalization constraints on acoustic scores ($0.0 \le \text{score} \le 1.0$).
   * Quarantines malformed tracking events into a dedicated `ingestion_rejections` audit table.
3. **Database & Storage:** Persists clean events into an analytical SQLite relational store.
4. **Analytics:** Generates aggregation summaries profiling listening energy and tempo across genres.

## Quickstart
Run the pipeline directly using standard Python libraries:
```bash
python pipeline.py
