import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from db import init_db, list_library


class LibraryLoadingTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "library.sqlite3"
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        init_db(self.conn)
        self._seed_library()

    def tearDown(self):
        self.conn.close()
        self.temp_dir.cleanup()

    def _seed_library(self):
        timestamp = "2026-06-12T12:00:00+08:00"
        self.conn.execute(
            """
            INSERT INTO creators(name, sec_user_id, profile_url, category, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("creator", "sec-user", "https://example.com/creator", "finance", timestamp, timestamp),
        )
        creator_id = self.conn.execute("SELECT id FROM creators").fetchone()["id"]
        for index in range(40):
            cursor = self.conn.execute(
                """
                INSERT INTO videos(
                    platform, source_url, source_id, creator_id, title, description,
                    like_count, comment_count, repost_count, favorite_count,
                    metadata_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "douyin",
                    f"https://example.com/video/{index}",
                    str(index),
                    creator_id,
                    f"title-{index}",
                    f"description-{index}",
                    index,
                    12,
                    3,
                    4,
                    json.dumps({"raw": "x" * 2000}),
                    "transcribed",
                    timestamp,
                    timestamp,
                ),
            )
            video_id = cursor.lastrowid
            self.conn.execute(
                """
                INSERT INTO transcripts(video_id, transcript_text, transcript_path, engine, model, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (video_id, "transcript " * 1000, None, "test", "test", timestamp),
            )
            self.conn.execute(
                """
                INSERT INTO analyses(video_id, provider, model, analysis_text, analysis_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (video_id, "test", "test", "analysis " * 1000, "{}", timestamp),
            )
            for comment_index in range(12):
                self.conn.execute(
                    """
                    INSERT INTO comments(video_id, content, like_count, published_at, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (video_id, f"comment-{comment_index}", comment_index, timestamp, timestamp),
                )
        self.conn.commit()

    def test_summary_is_lightweight_and_uses_one_select(self):
        select_count = 0

        def trace(statement):
            nonlocal select_count
            if statement.lstrip().upper().startswith(("SELECT", "WITH")):
                select_count += 1

        self.conn.set_trace_callback(trace)
        summary = list_library(self.conn, include_details=False)
        self.conn.set_trace_callback(None)
        details = list_library(self.conn, include_details=True)

        self.assertEqual(40, len(summary))
        self.assertEqual(1, select_count)
        self.assertNotIn("transcript_text", summary[0])
        self.assertNotIn("analysis_text", summary[0])
        self.assertNotIn("top_comments", summary[0])
        self.assertTrue(summary[0]["has_transcript"])
        self.assertTrue(summary[0]["has_analysis"])
        self.assertFalse(summary[0]["details_loaded"])

        summary_size = len(json.dumps(summary).encode("utf-8"))
        details_size = len(json.dumps(details).encode("utf-8"))
        self.assertLess(summary_size * 20, details_size)

    def test_detail_query_batches_comments(self):
        select_count = 0

        def trace(statement):
            nonlocal select_count
            if statement.lstrip().upper().startswith(("SELECT", "WITH")):
                select_count += 1

        ids = [row["id"] for row in self.conn.execute("SELECT id FROM videos ORDER BY id LIMIT 5")]
        self.conn.set_trace_callback(trace)
        details = list_library(self.conn, video_ids=ids, include_details=True)
        self.conn.set_trace_callback(None)

        self.assertEqual(5, len(details))
        self.assertEqual(2, select_count)
        self.assertTrue(all(row["details_loaded"] for row in details))
        self.assertTrue(all(len(row["top_comments"]) == 10 for row in details))
        self.assertTrue(all(row["top_comments"][0]["like_count"] == 11 for row in details))


if __name__ == "__main__":
    unittest.main()
