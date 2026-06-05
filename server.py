import argparse
import asyncio
import json
import logging
import mimetypes
import os
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from app_logging import log_event, setup_logging, tail_logs
from db import (
    clear_failed_task,
    connect,
    delete_video,
    export_library,
    ignore_failed_task,
    init_db,
    list_failed_tasks,
    now_iso,
    record_failed_task,
    save_analysis,
    save_harvest_result,
    save_transcript,
    update_video_paths,
    update_video_status,
)
from doubao_asr import extract_audio_for_asr, extract_transcript_text, recognize_audio_file, save_asr_result
from douyin_creator_harvest import harvest_target
from volcengine import analyze_with_doubao, load_local_env, parse_json_object

ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
LOGGER = logging.getLogger("douyin_live_research.server")
load_local_env()
PROGRESS_LOCK = threading.Lock()
PROGRESS_STATE: dict[str, dict] = {}
EXPORT_LOCK = threading.Lock()
ACTIVE_HARVEST_LOCK = threading.Lock()
ACTIVE_HARVEST_KEYS: set[str] = set()
ACTIVE_VIDEO_TASK_LOCK = threading.Lock()
ACTIVE_VIDEO_TASKS: set[tuple[str, int]] = set()
ACTIVE_BATCH_LOCK = threading.Lock()
ACTIVE_BATCH_TYPES: set[str] = set()
JOB_LOCK = threading.Lock()
JOBS: dict[str, dict] = {}


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except Exception:
        value = default
    return max(minimum, min(maximum, value))


BATCH_LIMITS = {
    "transcribe": env_int("BATCH_TRANSCRIBE_CONCURRENCY", 2, 1, 4),
    "analyze": env_int("BATCH_ANALYZE_CONCURRENCY", 2, 1, 4),
}


def export_library_safely(conn) -> list[dict]:
    with EXPORT_LOCK:
        return export_library(conn, WEB_ROOT / "library.json")


def set_task_progress(task_id: str | None, **updates: object) -> None:
    if not task_id:
        return
    now = time.time()
    with PROGRESS_LOCK:
        state = PROGRESS_STATE.get(task_id, {"task_id": task_id, "created_at": now})
        state.update(updates)
        state["updated_at"] = now
        PROGRESS_STATE[task_id] = state


def get_task_progress(task_id: str | None) -> dict:
    if not task_id:
        return {"ok": False, "error": "缺少 task_id"}
    with PROGRESS_LOCK:
        state = dict(PROGRESS_STATE.get(task_id) or {})
    if not state:
        return {"ok": False, "error": "未找到任务进度"}
    return {"ok": True, "progress": state}


def set_job_state(job_id: str, **updates: object) -> None:
    now = time.time()
    with JOB_LOCK:
        state = JOBS.get(job_id, {"job_id": job_id, "created_at": now})
        state.update(updates)
        state["updated_at"] = now
        JOBS[job_id] = state


def list_jobs() -> list[dict]:
    with JOB_LOCK:
        jobs = [dict(item) for item in JOBS.values()]
    jobs.sort(key=lambda item: item.get("updated_at", 0), reverse=True)
    return jobs[:50]


def task_label(task_type: str) -> str:
    return {"transcribe": "转录", "analyze": "分析"}.get(task_type, task_type)


def acquire_video_task(task_type: str, video_id: int) -> bool:
    key = (task_type, video_id)
    with ACTIVE_VIDEO_TASK_LOCK:
        if key in ACTIVE_VIDEO_TASKS:
            return False
        ACTIVE_VIDEO_TASKS.add(key)
        return True


def release_video_task(task_type: str, video_id: int) -> None:
    with ACTIVE_VIDEO_TASK_LOCK:
        ACTIVE_VIDEO_TASKS.discard((task_type, video_id))


def filter_batch_video_ids(task_type: str, video_ids: list[int]) -> list[int]:
    if not video_ids:
        return []
    placeholders = ",".join("?" for _ in video_ids)
    conn = connect()
    init_db(conn)
    if task_type == "transcribe":
        rows = conn.execute(
            f"""
            SELECT v.id
            FROM videos v
            LEFT JOIN transcripts t ON t.video_id = v.id
            WHERE v.id IN ({placeholders})
              AND v.media_path IS NOT NULL
              AND v.media_path != ''
              AND t.id IS NULL
              AND COALESCE(v.status, '') NOT IN ('no_speech', 'transcribe_running')
            """,
            video_ids,
        ).fetchall()
    elif task_type == "analyze":
        rows = conn.execute(
            f"""
            SELECT v.id
            FROM videos v
            LEFT JOIN transcripts t ON t.video_id = v.id
            LEFT JOIN analyses a ON a.video_id = v.id
            WHERE v.id IN ({placeholders})
              AND t.id IS NOT NULL
              AND a.id IS NULL
              AND COALESCE(v.status, '') != 'analyze_running'
            """,
            video_ids,
        ).fetchall()
    else:
        return []
    allowed = {int(row["id"]) for row in rows}
    return [video_id for video_id in video_ids if video_id in allowed]


def execute_video_task(task_type: str, video_id: int) -> dict:
    if not acquire_video_task(task_type, video_id):
        raise RuntimeError(f"视频 #{video_id} 已有{task_label(task_type)}任务正在处理中")
    try:
        conn = connect()
        init_db(conn)
        try:
            update_video_paths(conn, video_id, status=f"{task_type}_running", error=None)
        except Exception:
            pass
        if task_type == "transcribe":
            return transcribe_video(video_id)
        if task_type == "analyze":
            return analyze_video(video_id)
        raise ValueError(f"不支持的任务类型：{task_type}")
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        log_event(LOGGER, f"{task_type}.failure", video_id=video_id or None, error=error)
        conn = connect()
        init_db(conn)
        record_failed_task(conn, task_type, error, video_id=video_id or None, payload={"video_id": video_id})
        try:
            update_video_paths(conn, video_id, status=f"{task_type}_failed", error=error)
        except Exception:
            pass
        export_library_safely(conn)
        raise
    finally:
        release_video_task(task_type, video_id)


def run_batch_job(job_id: str, task_type: str, video_ids: list[int]) -> None:
    label = task_label(task_type)
    try:
        set_job_state(job_id, status="running", started_at=time.time())
        set_task_progress(job_id, stage="running", label=f"批量{label}", done=0, total=len(video_ids), success=0, fail=0, status="running")
        success = 0
        fail = 0
        limit = max(1, min(4, BATCH_LIMITS.get(task_type, 1)))
        with ThreadPoolExecutor(max_workers=limit, thread_name_prefix=f"{task_type}-worker") as executor:
            future_map = {executor.submit(execute_video_task, task_type, video_id): video_id for video_id in video_ids}
            for future in as_completed(future_map):
                video_id = future_map[future]
                try:
                    future.result()
                    success += 1
                except Exception:
                    fail += 1
                done = success + fail
                set_job_state(job_id, done=done, success=success, fail=fail, current_video_id=video_id)
                set_task_progress(job_id, stage="running", label=f"{label} {done}/{len(video_ids)}", done=done, total=len(video_ids), success=success, fail=fail, current_video_id=video_id, status="running")
        conn = connect()
        init_db(conn)
        export_library_safely(conn)
        set_job_state(job_id, status="done", done=len(video_ids), success=success, fail=fail, finished_at=time.time())
        set_task_progress(job_id, stage="done", label=f"批量{label}完成", done=len(video_ids), total=len(video_ids), success=success, fail=fail, status="done")
        log_event(LOGGER, "batch.success", job_id=job_id, task_type=task_type, total=len(video_ids), success=success, fail=fail)
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
        set_job_state(job_id, status="failed", error=error, finished_at=time.time())
        set_task_progress(job_id, stage="failed", label=f"批量{label}失败", done=0, total=len(video_ids), success=0, fail=len(video_ids), status="failed", error=error)
        log_event(LOGGER, "batch.failure", job_id=job_id, task_type=task_type, error=error)
    finally:
        with ACTIVE_BATCH_LOCK:
            ACTIVE_BATCH_TYPES.discard(task_type)


def enqueue_batch_job(task_type: str, video_ids: list[int]) -> dict:
    if task_type not in {"transcribe", "analyze"}:
        raise ValueError(f"不支持的批量任务类型：{task_type}")
    clean_ids = []
    seen = set()
    for value in video_ids:
        try:
            video_id = int(value)
        except Exception:
            continue
        if video_id > 0 and video_id not in seen:
            seen.add(video_id)
            clean_ids.append(video_id)
    clean_ids = filter_batch_video_ids(task_type, clean_ids)
    if not clean_ids:
        raise ValueError(f"没有需要{task_label(task_type)}的视频")
    with ACTIVE_BATCH_LOCK:
        if task_type in ACTIVE_BATCH_TYPES:
            raise RuntimeError(f"已有批量{task_label(task_type)}任务正在运行，请等待完成后再提交")
        ACTIVE_BATCH_TYPES.add(task_type)
    job_id = f"{task_type}_{uuid.uuid4()}"
    job = {
        "job_id": job_id,
        "task_type": task_type,
        "status": "queued",
        "total": len(clean_ids),
        "done": 0,
        "success": 0,
        "fail": 0,
        "video_ids": clean_ids,
        "created_at": time.time(),
    }
    with JOB_LOCK:
        JOBS[job_id] = dict(job)
    set_task_progress(job_id, stage="queued", label=f"批量{task_label(task_type)}排队中", done=0, total=len(clean_ids), success=0, fail=0, status="queued")
    thread = threading.Thread(target=run_batch_job, args=(job_id, task_type, clean_ids), daemon=True)
    thread.start()
    return dict(JOBS[job_id])


def json_response(handler: SimpleHTTPRequestHandler, status: int, payload: dict | list) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_json(handler: SimpleHTTPRequestHandler) -> dict:
    length = int(handler.headers.get("Content-Length") or 0)
    raw = handler.rfile.read(length) if length else b"{}"
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError("请求体不是有效 JSON") from e


def bounded_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = default
    return max(minimum, min(maximum, parsed))


def export_current_library() -> list[dict]:
    conn = connect()
    init_db(conn)
    return export_library_safely(conn)


def analyze_video(video_id: int) -> dict:
    log_event(LOGGER, "analyze.start", video_id=video_id)
    conn = connect()
    init_db(conn)
    row = conn.execute(
        """
        SELECT v.id, v.title, v.description, t.transcript_text
        FROM videos v
        LEFT JOIN transcripts t ON t.video_id = v.id
        WHERE v.id = ?
        """,
        (video_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    comments = conn.execute(
        """
        SELECT content
        FROM comments
        WHERE video_id = ?
        ORDER BY COALESCE(like_count, 0) DESC, id ASC
        LIMIT 20
        """,
        (video_id,),
    ).fetchall()
    analysis_text = analyze_with_doubao(
        title=row["title"] or "",
        description=row["description"] or "",
        transcript=row["transcript_text"] or "",
        comments=[c["content"] for c in comments],
    )
    parsed = parse_json_object(analysis_text)
    save_analysis(
        conn,
        video_id,
        "volcengine-ark",
        os.environ.get("VOLCENGINE_ENDPOINT_ID", ""),
        analysis_text,
        parsed,
    )
    update_video_paths(conn, video_id, status="analyzed")
    clear_failed_task(conn, "analyze", video_id)
    export_library_safely(conn)
    log_event(LOGGER, "analyze.success", video_id=video_id, parsed=bool(parsed), analysis_length=len(analysis_text))
    return {
        "video_id": video_id,
        "analysis_preview": analysis_text[:800],
        "parsed": bool(parsed),
    }


def transcribe_video(video_id: int) -> dict:
    log_event(LOGGER, "transcribe.start", video_id=video_id)
    conn = connect()
    init_db(conn)
    row = conn.execute(
        """
        SELECT id, media_path, creator_id, metadata_json
        FROM videos
        WHERE id = ?
        """,
        (video_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    if not row["media_path"]:
        raise ValueError(f"视频 ID {video_id} 没有本地视频文件")
    try:
        metadata = json.loads(row["metadata_json"] or "{}")
    except Exception:
        metadata = {}
    author_sec_user_id = metadata.get("author_sec_user_id")
    if author_sec_user_id and row["creator_id"]:
        conn.execute(
            "UPDATE creators SET sec_user_id = COALESCE(sec_user_id, ?), updated_at = ? WHERE id = ?",
            (author_sec_user_id, now_iso(), row["creator_id"]),
        )
        conn.commit()
    audio_path = extract_audio_for_asr(row["media_path"], video_id)
    update_video_paths(conn, video_id, audio_path=str(audio_path), status="audio_ready")
    asr_result = recognize_audio_file(audio_path)
    transcript_text = extract_transcript_text(asr_result)
    if not transcript_text:
        raise RuntimeError(f"豆包语音 ASR 未返回可用文本：{json.dumps(asr_result, ensure_ascii=False)[:1000]}")
    result_path = save_asr_result(video_id, asr_result)
    save_transcript(conn, video_id, transcript_text, str(result_path), "doubao-asr", os.environ.get("DOUBAO_ASR_RESOURCE_ID", ""))
    update_video_paths(conn, video_id, status="transcribed")
    clear_failed_task(conn, "transcribe", video_id)
    export_library_safely(conn)
    log_event(LOGGER, "transcribe.success", video_id=video_id, transcript_length=len(transcript_text), audio_path=str(audio_path), result_path=str(result_path))
    return {
        "video_id": video_id,
        "audio_path": str(audio_path),
        "transcript_path": str(result_path),
        "transcript_length": len(transcript_text),
        "transcript_preview": transcript_text[:500],
    }


class AppHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        print(format % args)

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/library":
            json_response(self, 200, export_current_library())
            return
        if path == "/api/failed-tasks":
            conn = connect()
            init_db(conn)
            json_response(self, 200, list_failed_tasks(conn))
            return
        if path == "/api/logs":
            limit = bounded_int(dict([part.split("=", 1) for part in urlparse(self.path).query.split("&") if "=" in part]).get("limit"), 200, 1, 1000)
            json_response(self, 200, {"ok": True, "logs": tail_logs(limit)})
            return
        if path == "/api/jobs":
            json_response(self, 200, {"ok": True, "jobs": list_jobs()})
            return
        if path == "/api/progress":
            params = dict([part.split("=", 1) for part in urlparse(self.path).query.split("&") if "=" in part])
            payload = get_task_progress(params.get("task_id"))
            json_response(self, 200 if payload.get("ok") else 404, payload)
            return
        if path in ("", "/"):
            self.serve_file(WEB_ROOT / "index.html")
            return
        requested = (WEB_ROOT / path.lstrip("/")).resolve()
        if WEB_ROOT.resolve() in requested.parents and requested.exists() and requested.is_file():
            self.serve_file(requested)
            return
        json_response(self, 404, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        if path == "/api/transcribe":
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                result = execute_video_task("transcribe", video_id)
                json_response(self, 200, {"ok": True, "result": result})
            except Exception as e:
                json_response(self, 500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        if path == "/api/analyze":
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                result = execute_video_task("analyze", video_id)
                json_response(self, 200, {"ok": True, "result": result})
            except Exception as e:
                json_response(self, 500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        if path == "/api/batch-task":
            try:
                payload = read_json(self)
                task_type = str(payload.get("task_type") or "").strip()
                video_ids = payload.get("video_ids") or []
                if not isinstance(video_ids, list):
                    raise ValueError("video_ids 必须是数组")
                job = enqueue_batch_job(task_type, video_ids)
                json_response(self, 200, {"ok": True, "job": job, "task_id": job["job_id"]})
            except Exception as e:
                json_response(self, 500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        if path == "/api/failed-tasks/ignore":
            try:
                payload = read_json(self)
                task_id = bounded_int(payload.get("task_id"), 0, 1, 10_000_000)
                conn = connect()
                init_db(conn)
                ignore_failed_task(conn, task_id)
                log_event(LOGGER, "failed_task.ignore", task_id=task_id)
                json_response(self, 200, {"ok": True, "failed_tasks": list_failed_tasks(conn)})
            except Exception as e:
                log_event(LOGGER, "failed_task.ignore_failure", error=f"{type(e).__name__}: {e}")
                json_response(self, 500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        if path == "/api/video/status":
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                status = str(payload.get("status") or "").strip()
                allowed = {"harvested", "downloaded", "transcribed", "analyzed", "no_speech", "ignored"}
                if status not in allowed:
                    raise ValueError(f"不支持的视频状态：{status}")
                conn = connect()
                init_db(conn)
                update_video_status(conn, video_id, status=status, error=None)
                if status == "no_speech":
                    clear_failed_task(conn, "transcribe", video_id)
                library = export_library_safely(conn)
                log_event(LOGGER, "video.status_update", video_id=video_id, status=status)
                json_response(self, 200, {"ok": True, "video_id": video_id, "status": status, "library_count": len(library)})
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                log_event(LOGGER, "video.status_update_failure", error=error)
                json_response(self, 500, {"ok": False, "error": error})
            return
        if path == "/api/video/delete":
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                delete_files = bool(payload.get("delete_files", False))
                conn = connect()
                init_db(conn)
                result = delete_video(conn, video_id, delete_files=delete_files)
                library = export_library_safely(conn)
                log_event(LOGGER, "video.delete", video_id=video_id, delete_files=delete_files, deleted_files=result.get("deleted_files"))
                json_response(self, 200, {"ok": True, "result": result, "library_count": len(library)})
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                log_event(LOGGER, "video.delete_failure", error=error)
                json_response(self, 500, {"ok": False, "error": error})
            return
        if path != "/api/harvest":
            json_response(self, 404, {"ok": False, "error": "Not found"})
            return
        try:
            payload = read_json(self)
            target = str(payload.get("target") or "").strip()
            if not target:
                raise ValueError("请填写抖音主页链接或 sec_user_id")
            category = str(payload.get("category") or "未分类").strip() or "未分类"
            page_size = bounded_int(payload.get("page_size"), 10, 1, 50)
            max_pages_raw = bounded_int(payload.get("max_pages"), 1, 0, 200)
            max_pages = max_pages_raw or None
            top_comments = bounded_int(payload.get("top_comments"), 10, 0, 50)
            min_likes = bounded_int(payload.get("min_likes"), 0, 0, 1_000_000_000)
            top_videos_raw = bounded_int(payload.get("top_videos"), 0, 0, 10_000)
            top_videos = top_videos_raw or None
            download = bool(payload.get("download", True))
            task_id = str(payload.get("task_id") or uuid.uuid4())
            harvest_key = json.dumps({
                "target": target,
                "page_size": page_size,
                "max_pages": max_pages,
                "top_comments": top_comments,
                "min_likes": min_likes,
                "top_videos": top_videos,
                "download": download,
            }, ensure_ascii=False, sort_keys=True)
            with ACTIVE_HARVEST_LOCK:
                if harvest_key in ACTIVE_HARVEST_KEYS:
                    raise RuntimeError("相同账号采集任务正在运行，请等待完成后再提交")
                ACTIVE_HARVEST_KEYS.add(harvest_key)
            set_task_progress(task_id, stage="start", label="账号采集", done=0, total=0, success=0, fail=0, status="running")
            log_event(
                LOGGER,
                "harvest.start",
                task_id=task_id,
                target=target,
                category=category,
                page_size=page_size,
                max_pages=max_pages,
                top_comments=top_comments,
                min_likes=min_likes,
                top_videos=top_videos,
                download=download,
            )

            result = asyncio.run(
                harvest_target(
                    target=target,
                    page_size=page_size,
                    max_pages=max_pages,
                    top_comments=top_comments,
                    download=download,
                    min_likes=min_likes,
                    top_videos=top_videos,
                    progress_callback=lambda **updates: set_task_progress(task_id, **updates),
                )
            )
            conn = connect()
            saved = save_harvest_result(conn, result, category=category, profile_url=target)
            clear_failed_task(conn, "harvest")
            library = export_library_safely(conn)
            log_event(
                LOGGER,
                "harvest.success",
                task_id=task_id,
                sec_user_id=result.get("sec_user_id"),
                creator_nickname=result.get("creator_nickname"),
                fetched_video_count=result.get("fetched_video_count"),
                video_count=result.get("video_count"),
                saved_video_count=saved.get("saved_video_count"),
            )
            set_task_progress(task_id, stage="saved", label="账号采集完成", done=result.get("video_count") or 0, total=result.get("video_count") or 0, success=saved.get("saved_video_count") or 0, fail=0, status="done")
            if "harvest_key" in locals():
                with ACTIVE_HARVEST_LOCK:
                    ACTIVE_HARVEST_KEYS.discard(harvest_key)
            json_response(
                self,
                200,
                {
                    "ok": True,
                    "task_id": task_id,
                    "result": {
                        "sec_user_id": result.get("sec_user_id"),
                        "creator_nickname": result.get("creator_nickname"),
                        "creator_unique_id": result.get("creator_unique_id"),
                        "video_count": result.get("video_count"),
                        "fetched_video_count": result.get("fetched_video_count"),
                        "filters": result.get("filters"),
                        "raw_output": result.get("output"),
                    },
                    "saved": saved,
                    "library_count": len(library),
                },
            )
        except Exception as e:
            error = f"{type(e).__name__}: {e}"
            if "task_id" in locals():
                set_task_progress(task_id, stage="failed", label="账号采集失败", done=1, total=1, success=0, fail=1, status="failed", error=error)
            if "harvest_key" in locals():
                with ACTIVE_HARVEST_LOCK:
                    ACTIVE_HARVEST_KEYS.discard(harvest_key)
            log_event(LOGGER, "harvest.failure", error=error, payload=payload if "payload" in locals() else {})
            conn = connect()
            init_db(conn)
            record_failed_task(conn, "harvest", error, payload=payload if "payload" in locals() else {})
            json_response(self, 500, {"ok": False, "error": error})

    def serve_file(self, path: Path) -> None:
        content = path.read_bytes()
        ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        if path.suffix == ".html":
            ctype = "text/html; charset=utf-8"
        elif path.suffix == ".json":
            ctype = "application/json; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def main() -> None:
    setup_logging()
    parser = argparse.ArgumentParser(description="直播素材研究台本地服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    args = parser.parse_args()
    export_current_library()
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    log_event(LOGGER, "server.start", host=args.host, port=args.port)
    print(f"服务已启动：http://{args.host}:{args.port}/")
    server.serve_forever()


if __name__ == "__main__":
    main()
