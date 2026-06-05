import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "library.sqlite3"
ROOT = Path(__file__).resolve().parent


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS creators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sec_user_id TEXT,
            profile_url TEXT,
            category TEXT,
            tags TEXT DEFAULT '[]',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(name, category)
        );

        CREATE TABLE IF NOT EXISTS videos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL DEFAULT 'douyin',
            source_url TEXT NOT NULL UNIQUE,
            source_id TEXT,
            creator_id INTEGER,
            title TEXT,
            description TEXT,
            duration REAL,
            published_at TEXT,
            view_count INTEGER,
            like_count INTEGER,
            comment_count INTEGER,
            repost_count INTEGER,
            favorite_count INTEGER,
            media_path TEXT,
            audio_path TEXT,
            metadata_json TEXT DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'created',
            error TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(creator_id) REFERENCES creators(id)
        );

        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id INTEGER NOT NULL,
            content TEXT NOT NULL,
            like_count INTEGER DEFAULT 0,
            published_at TEXT,
            author_hash TEXT,
            metadata_json TEXT DEFAULT '{}',
            created_at TEXT NOT NULL,
            FOREIGN KEY(video_id) REFERENCES videos(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS transcripts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id INTEGER NOT NULL UNIQUE,
            transcript_text TEXT NOT NULL,
            transcript_path TEXT,
            engine TEXT,
            model TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(video_id) REFERENCES videos(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id INTEGER NOT NULL UNIQUE,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            analysis_text TEXT NOT NULL,
            analysis_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(video_id) REFERENCES videos(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS failed_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_type TEXT NOT NULL,
            video_id INTEGER,
            payload_json TEXT DEFAULT '{}',
            error TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'failed',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(video_id) REFERENCES videos(id) ON DELETE CASCADE
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
            title,
            description,
            transcript,
            analysis,
            content=''
        );
        """
    )
    ensure_column(conn, "creators", "sec_user_id", "TEXT")
    conn.commit()


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def upsert_creator(conn: sqlite3.Connection, name: str, category: str = "未分类", profile_url: str | None = None, sec_user_id: str | None = None) -> int:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO creators(name, sec_user_id, profile_url, category, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, category) DO UPDATE SET
            sec_user_id = COALESCE(excluded.sec_user_id, creators.sec_user_id),
            profile_url = COALESCE(excluded.profile_url, creators.profile_url),
            updated_at = excluded.updated_at
        """,
        (name, sec_user_id, profile_url, category, ts, ts),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM creators WHERE name = ? AND category = ?", (name, category)).fetchone()
    return int(row["id"])


def upsert_video(conn: sqlite3.Connection, data: dict) -> int:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO videos(
            platform, source_url, source_id, creator_id, title, description, duration,
            published_at, view_count, like_count, comment_count, repost_count, favorite_count,
            media_path, audio_path, metadata_json, status, error, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
            source_id = excluded.source_id,
            creator_id = excluded.creator_id,
            title = excluded.title,
            description = excluded.description,
            duration = excluded.duration,
            published_at = excluded.published_at,
            view_count = excluded.view_count,
            like_count = excluded.like_count,
            comment_count = excluded.comment_count,
            repost_count = excluded.repost_count,
            favorite_count = excluded.favorite_count,
            media_path = COALESCE(excluded.media_path, videos.media_path),
            audio_path = COALESCE(excluded.audio_path, videos.audio_path),
            metadata_json = excluded.metadata_json,
            status = CASE
                WHEN videos.status IN ('transcribed', 'analyzed', 'no_speech') THEN videos.status
                ELSE excluded.status
            END,
            error = excluded.error,
            updated_at = excluded.updated_at
        """,
        (
            data.get("platform", "douyin"), data["source_url"], data.get("source_id"), data.get("creator_id"),
            data.get("title"), data.get("description"), data.get("duration"), data.get("published_at"),
            data.get("view_count"), data.get("like_count"), data.get("comment_count"), data.get("repost_count"),
            data.get("favorite_count"), data.get("media_path"), data.get("audio_path"),
            json.dumps(data.get("metadata", {}), ensure_ascii=False), data.get("status", "created"),
            data.get("error"), ts, ts,
        ),
    )
    conn.commit()
    row = conn.execute("SELECT id FROM videos WHERE source_url = ?", (data["source_url"],)).fetchone()
    return int(row["id"])


def save_video_comments(conn: sqlite3.Connection, video_id: int, comments: list[dict]) -> None:
    conn.execute("DELETE FROM comments WHERE video_id = ?", (video_id,))
    ts = now_iso()
    for item in comments:
        content = item.get("text") or item.get("content")
        if not content:
            continue
        conn.execute(
            """
            INSERT INTO comments(video_id, content, like_count, published_at, author_hash, metadata_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                video_id,
                content,
                item.get("digg_count") or item.get("like_count") or 0,
                timestamp_to_iso(item.get("create_time")),
                item.get("cid"),
                json.dumps(item.get("raw") or item, ensure_ascii=False),
                ts,
            ),
        )
    conn.commit()


def timestamp_to_iso(value: object) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), timezone.utc).astimezone().isoformat(timespec="seconds")
    except Exception:
        return None


def douyin_video_url(aweme_id: str) -> str:
    return f"https://www.douyin.com/video/{aweme_id}"


def wechat_channels_video_url(feed: dict) -> str:
    oid = str(feed.get("id") or "")
    nid = str(feed.get("objectNonceId") or "")
    if oid and nid:
        return f"https://channels.weixin.qq.com/web/pages/feed?oid={oid}&nid={nid}"
    return f"wechat_channels://feed/{oid or nid}"


def save_harvest_result(conn: sqlite3.Connection, result: dict, category: str = "未分类", profile_url: str | None = None) -> dict:
    init_db(conn)
    creator = result.get("creator") or {}
    creator_name = result.get("creator_nickname") or creator.get("nickname") or result.get("sec_user_id") or "未命名博主"
    sec_user_id = result.get("creator_sec_user_id") or creator.get("sec_user_id") or creator.get("sec_uid") or result.get("sec_user_id")
    creator_id = upsert_creator(conn, name=creator_name, category=category, profile_url=profile_url or result.get("target"), sec_user_id=sec_user_id)
    saved_video_ids = []
    for item in result.get("videos") or []:
        aweme_id = str(item.get("aweme_id") or "")
        if not aweme_id:
            continue
        video_id = upsert_video(
            conn,
            {
                "platform": "douyin",
                "source_url": douyin_video_url(aweme_id),
                "source_id": aweme_id,
                "creator_id": creator_id,
                "title": item.get("desc"),
                "description": item.get("desc"),
                "published_at": timestamp_to_iso(item.get("create_time")),
                "like_count": item.get("digg_count"),
                "comment_count": item.get("comment_count"),
                "repost_count": item.get("share_count"),
                "favorite_count": item.get("collect_count"),
                "media_path": item.get("media_path"),
                "metadata": {
                    "download_urls": item.get("download_urls") or [],
                    "author_nickname": item.get("author_nickname"),
                    "author_sec_user_id": item.get("author_sec_user_id"),
                    "download_error": item.get("download_error"),
                    "comment_error": item.get("comment_error"),
                    "raw": item.get("raw"),
                },
                "status": "harvested",
                "error": item.get("download_error") or item.get("comment_error"),
            },
        )
        save_video_comments(conn, video_id, item.get("top_comments") or [])
        saved_video_ids.append(video_id)
    return {
        "creator_id": creator_id,
        "creator_name": creator_name,
        "saved_video_count": len(saved_video_ids),
        "saved_video_ids": saved_video_ids,
    }


def save_wechat_channels_result(conn: sqlite3.Connection, result: dict, category: str = "未分类", profile_url: str | None = None) -> dict:
    init_db(conn)
    creator = result.get("creator") or {}
    username = result.get("creator_username") or result.get("username") or creator.get("username")
    creator_name = result.get("creator_nickname") or creator.get("nickname") or username or "未命名视频号"
    creator_id = upsert_creator(
        conn,
        name=creator_name,
        category=category,
        profile_url=profile_url or f"wechat_channels://{username or creator_name}",
        sec_user_id=username,
    )
    saved_video_ids = []
    for item in result.get("videos") or []:
        feed_id = str(item.get("id") or "")
        if not feed_id:
            continue
        media_items = ((item.get("objectDesc") or {}).get("media")) or []
        media = media_items[0] if media_items else {}
        count_info = item.get("countInfo") or {}
        description = (item.get("objectDesc") or {}).get("description")
        video_id = upsert_video(
            conn,
            {
                "platform": "wechat_channels",
                "source_url": wechat_channels_video_url(item),
                "source_id": feed_id,
                "creator_id": creator_id,
                "title": description,
                "description": description,
                "duration": media.get("videoPlayLen"),
                "published_at": timestamp_to_iso(item.get("createtime")),
                "like_count": count_info.get("likeCount"),
                "comment_count": count_info.get("commentCount"),
                "repost_count": count_info.get("forwardCount"),
                "favorite_count": count_info.get("favCount"),
                "media_path": item.get("media_path"),
                "metadata": {
                    "platform": "wechat_channels",
                    "creator_username": username,
                    "author_sec_user_id": username,
                    "object_nonce_id": item.get("objectNonceId"),
                    "download_error": item.get("download_error"),
                    "comment_error": item.get("comment_error"),
                    "media": media,
                    "download_task_ids": result.get("download_task_ids") or [],
                    "raw": item,
                },
                "status": "downloaded" if item.get("media_path") else "harvested",
                "error": item.get("download_error") or item.get("comment_error"),
            },
        )
        save_video_comments(conn, video_id, item.get("top_comments") or [])
        saved_video_ids.append(video_id)
    return {
        "creator_id": creator_id,
        "creator_name": creator_name,
        "saved_video_count": len(saved_video_ids),
        "saved_video_ids": saved_video_ids,
    }


def update_video_paths(conn: sqlite3.Connection, video_id: int, media_path: str | None = None, audio_path: str | None = None, status: str | None = None, error: str | None = None) -> None:
    row = conn.execute("SELECT media_path, audio_path, status FROM videos WHERE id = ?", (video_id,)).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    conn.execute(
        """
        UPDATE videos SET media_path = ?, audio_path = ?, status = ?, error = ?, updated_at = ? WHERE id = ?
        """,
        (
            media_path or row["media_path"],
            audio_path or row["audio_path"],
            status or row["status"],
            error,
            now_iso(),
            video_id,
        ),
    )
    conn.commit()


def update_video_status(conn: sqlite3.Connection, video_id: int, status: str, error: str | None = None) -> None:
    row = conn.execute("SELECT id FROM videos WHERE id = ?", (video_id,)).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    conn.execute(
        """
        UPDATE videos SET status = ?, error = ?, updated_at = ? WHERE id = ?
        """,
        (status, error, now_iso(), video_id),
    )
    conn.commit()


def save_transcript(conn: sqlite3.Connection, video_id: int, text: str, path: str, engine: str, model: str) -> None:
    conn.execute(
        """
        INSERT INTO transcripts(video_id, transcript_text, transcript_path, engine, model, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            transcript_text = excluded.transcript_text,
            transcript_path = excluded.transcript_path,
            engine = excluded.engine,
            model = excluded.model,
            created_at = excluded.created_at
        """,
        (video_id, text, path, engine, model, now_iso()),
    )
    conn.commit()


def save_analysis(conn: sqlite3.Connection, video_id: int, provider: str, model: str, text: str, parsed: dict | None = None) -> None:
    conn.execute(
        """
        INSERT INTO analyses(video_id, provider, model, analysis_text, analysis_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(video_id) DO UPDATE SET
            provider = excluded.provider,
            model = excluded.model,
            analysis_text = excluded.analysis_text,
            analysis_json = excluded.analysis_json,
            created_at = excluded.created_at
        """,
        (video_id, provider, model, text, json.dumps(parsed, ensure_ascii=False) if parsed else None, now_iso()),
    )
    conn.commit()


def record_failed_task(conn: sqlite3.Connection, task_type: str, error: str, video_id: int | None = None, payload: dict | None = None) -> int:
    init_db(conn)
    ts = now_iso()
    if video_id is not None:
        row = conn.execute("SELECT id FROM videos WHERE id = ?", (video_id,)).fetchone()
        if not row:
            video_id = None
    if video_id is None:
        conn.execute(
            """
            DELETE FROM failed_tasks
            WHERE task_type = ? AND video_id IS NULL AND status = 'failed'
            """,
            (task_type,),
        )
    else:
        conn.execute(
            """
            DELETE FROM failed_tasks
            WHERE task_type = ? AND video_id = ? AND status = 'failed'
            """,
            (task_type, video_id),
        )
    conn.execute(
        """
        INSERT INTO failed_tasks(task_type, video_id, payload_json, error, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, 'failed', ?, ?)
        """,
        (task_type, video_id, json.dumps(payload or {}, ensure_ascii=False), error, ts, ts),
    )
    conn.commit()
    row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
    return int(row["id"])


def clear_failed_task(conn: sqlite3.Connection, task_type: str, video_id: int | None = None) -> None:
    init_db(conn)
    if video_id is None:
        conn.execute(
            """
            DELETE FROM failed_tasks
            WHERE task_type = ? AND video_id IS NULL AND status = 'failed'
            """,
            (task_type,),
        )
    else:
        conn.execute(
            """
            DELETE FROM failed_tasks
            WHERE task_type = ? AND video_id = ? AND status = 'failed'
            """,
            (task_type, video_id),
        )
    conn.commit()


def ignore_failed_task(conn: sqlite3.Connection, task_id: int) -> None:
    init_db(conn)
    conn.execute(
        """
        UPDATE failed_tasks
        SET status = 'ignored', updated_at = ?
        WHERE id = ?
        """,
        (now_iso(), task_id),
    )
    conn.commit()


def list_failed_tasks(conn: sqlite3.Connection) -> list[dict]:
    init_db(conn)
    rows = conn.execute(
        """
        SELECT
            f.id, f.task_type, f.video_id, f.payload_json, f.error, f.status, f.created_at, f.updated_at,
            v.title, v.like_count, v.comment_count, v.media_path, v.status AS video_status,
            c.name AS creator_name, c.category
        FROM failed_tasks f
        LEFT JOIN videos v ON v.id = f.video_id
        LEFT JOIN creators c ON c.id = v.creator_id
        WHERE f.status = 'failed'
        ORDER BY f.updated_at DESC, f.id DESC
        """
    ).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(item.pop("payload_json") or "{}")
        except json.JSONDecodeError:
            item["payload"] = {}
        result.append(item)
    return result


def safe_unlink(path_value: str | None) -> bool:
    if not path_value:
        return False
    path = Path(path_value).expanduser()
    try:
        resolved = path.resolve()
        if ROOT.resolve() not in resolved.parents:
            return False
        if resolved.exists() and resolved.is_file():
            resolved.unlink()
            return True
    except Exception:
        return False
    return False


def delete_video(conn: sqlite3.Connection, video_id: int, delete_files: bool = False) -> dict:
    init_db(conn)
    row = conn.execute(
        """
        SELECT v.id, v.media_path, v.audio_path, t.transcript_path
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.id
        WHERE v.id = ?
        """,
        (video_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    deleted_files = []
    if delete_files:
        for key in ("media_path", "audio_path", "transcript_path"):
            path_value = row[key]
            if safe_unlink(path_value):
                deleted_files.append(path_value)
    conn.execute("DELETE FROM videos WHERE id = ?", (video_id,))
    conn.commit()
    return {"video_id": video_id, "delete_files": delete_files, "deleted_files": deleted_files}


def export_library(conn: sqlite3.Connection, output_path: Path) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            v.id, v.platform, v.source_url, v.title, v.description, v.duration, v.published_at,
            v.view_count, v.like_count, v.comment_count, v.repost_count, v.favorite_count,
            v.media_path, v.audio_path, v.metadata_json, v.status, v.error, v.created_at, v.updated_at,
            c.name AS creator_name, c.category, c.sec_user_id AS creator_sec_user_id, c.profile_url AS creator_profile_url,
            t.transcript_text,
            a.analysis_text, a.analysis_json
        FROM videos v
        LEFT JOIN creators c ON c.id = v.creator_id
        LEFT JOIN transcripts t ON t.video_id = v.id
        LEFT JOIN analyses a ON a.video_id = v.id
        ORDER BY COALESCE(v.like_count, 0) DESC, v.updated_at DESC
        """
    ).fetchall()
    data = []
    for r in rows:
        item = dict(r)
        try:
            metadata = json.loads(item.get("metadata_json") or "{}")
        except Exception:
            metadata = {}
        if not item.get("creator_sec_user_id"):
            item["creator_sec_user_id"] = metadata.get("author_sec_user_id")
        item.pop("metadata_json", None)
        comment_rows = conn.execute(
            """
            SELECT content, like_count, published_at
            FROM comments
            WHERE video_id = ?
            ORDER BY COALESCE(like_count, 0) DESC, id ASC
            LIMIT 10
            """,
            (item["id"],),
        ).fetchall()
        item["top_comments"] = [dict(c) for c in comment_rows]
        item["score"] = score_video(item)
        data.append(item)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data


def score_video(item: dict) -> float:
    likes = item.get("like_count") or 0
    comments = item.get("comment_count") or 0
    favorites = item.get("favorite_count") or 0
    reposts = item.get("repost_count") or 0
    transcript_bonus = 20 if item.get("transcript_text") else 0
    analysis_bonus = 30 if item.get("analysis_text") else 0
    return round(likes * 1.0 + comments * 3.0 + favorites * 2.0 + reposts * 2.5 + transcript_bonus + analysis_bonus, 2)
