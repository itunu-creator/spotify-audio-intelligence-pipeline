"""
Personal Listening Habits & Audio Feature ETL Pipeline
Simulates fetching real-time listening data from Spotify/streaming APIs,
extracting audio features, validating data integrity, and staging it for analytical SQL queries.
"""

import json
import sqlite3
from datetime import datetime

# 1. EXTRACT: Simulating a JSON payload returned from a streaming Web API endpoint
MOCK_API_PAYLOAD = """
[
    {
        "track_id": "trk_01",
        "title": "Essence",
        "artist": "Wizkid",
        "genre": "Afrobeats",
        "played_at": "2026-10-22 14:10:00",
        "audio_features": {"tempo_bpm": 104, "energy_score": 0.65, "danceability": 0.82}
    },
    {
        "track_id": "trk_02",
        "title": "Last Last",
        "artist": "Burna Boy",
        "genre": "Afrobeats",
        "played_at": "2026-10-22 14:14:00",
        "audio_features": {"tempo_bpm": 128, "energy_score": 0.78, "danceability": 0.74}
    },
    {
        "track_id": "trk_03",
        "title": "Higher",
        "artist": "Tems",
        "genre": "R&B / Soul",
        "played_at": "2026-10-22 18:30:00",
        "audio_features": {"tempo_bpm": 95, "energy_score": 0.42, "danceability": 0.61}
    },
    {
        "track_id": "trk_04",
        "title": "Corrupted_Audio_Log",
        "artist": "Unknown",
        "genre": "Ambient",
        "played_at": "2026-10-22 19:00:00",
        "audio_features": {"tempo_bpm": -10, "energy_score": 1.5, "danceability": 0.0}
    }
]
"""

DATABASE_NAME = "spotify_streams.db"


def init_database():
    """Initializes normalized SQLite tables for tracks, listening sessions, and validation failures."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listening_history (
        playback_id INTEGER PRIMARY KEY AUTOINCREMENT,
        track_id TEXT NOT NULL,
        title TEXT NOT NULL,
        artist TEXT NOT NULL,
        genre TEXT NOT NULL,
        tempo_bpm REAL NOT NULL,
        energy_score REAL NOT NULL,
        danceability REAL NOT NULL,
        played_at TIMESTAMP NOT NULL,
        ingested_at TIMESTAMP NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ingestion_rejections (
        rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_payload TEXT,
        reason TEXT,
        rejected_at TIMESTAMP
    );
    """)

    conn.commit()
    conn.close()


def transform_and_load():
    """Validates audio feature constraints and loads clean logs into the database."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    raw_data = json.loads(MOCK_API_PAYLOAD)
    clean_count = 0
    rejected_count = 0

    for item in raw_data:
        features = item.get("audio_features", {})
        tempo = features.get("tempo_bpm", 0)
        energy = features.get("energy_score", 0.0)
        danceability = features.get("danceability", 0.0)

        # Quality Rule 1: Tempo must be realistic (> 40 BPM)
        # Quality Rule 2: Audio scores must be normalized between 0.0 and 1.0
        if tempo <= 40 or not (0.0 <= energy <= 1.0) or not (0.0 <= danceability <= 1.0):
            cursor.execute("""
            INSERT INTO ingestion_rejections (raw_payload, reason, rejected_at)
            VALUES (?, ?, ?)
            """, (json.dumps(item), "Invalid audio metric ranges (BPM or 0-1 scores violated)", datetime.utcnow()))
            rejected_count += 1
            continue

        # Load valid records
        cursor.execute("""
        INSERT INTO listening_history 
        (track_id, title, artist, genre, tempo_bpm, energy_score, danceability, played_at, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            item["track_id"],
            item["title"],
            item["artist"],
            item["genre"],
            tempo,
            energy,
            danceability,
            item["played_at"],
            datetime.utcnow()
        ))
        clean_count += 1

    conn.commit()
    conn.close()

    print(f"[INGESTION COMPLETE] {clean_count} records validated and inserted.")
    print(f"[AUDIT LOG] {rejected_count} corrupted payloads quarantined.")


def run_analytics_summary():
    """Executes SQL aggregations to calculate average listening mood and tempo by genre."""
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()

    print("\n--- LISTENING MOOD & AUDIO ANALYTICS BY GENRE ---")
    query = """
    SELECT 
        genre,
        COUNT(track_id) AS tracks_streamed,
        ROUND(AVG(tempo_bpm), 1) AS avg_bpm,
        ROUND(AVG(energy_score), 2) AS avg_energy,
        ROUND(AVG(danceability), 2) AS avg_danceability
    FROM listening_history
    GROUP BY genre
    ORDER BY tracks_streamed DESC;
    """
    cursor.execute(query)
    for row in cursor.fetchall():
        print(f"Genre: {row[0]} | Streams: {row[1]} | Avg BPM: {row[2]} | Energy: {row[3]} | Danceability: {row[4]}")

    conn.close()


if __name__ == "__main__":
    init_database()
    transform_and_load()
    run_analytics_summary()
