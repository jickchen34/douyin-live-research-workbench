import argparse
import asyncio
import json
import logging
import mimetypes
import os
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
    record_failed_task,
    save_analysis,
    save_harvest_result,
    save_transcript,
    update_video_paths,
    update_video_status,
)
from doubao_asr import extract_audio_for_asr, extract_transcript_text, recognize_audio_file, save_asr_result
from douyin_creator_harvest import harvest_target
from volcengine import analyze_with_doubao, parse_json_object

ROOT = Path(__file__).resolve().parent
WEB_ROOT = ROOT / "web"
LOGGER = logging.getLogger("douyin_live_research.server")


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
    return export_library(conn, WEB_ROOT / "library.json")


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
    export_library(conn, WEB_ROOT / "library.json")
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
        SELECT id, media_path
        FROM videos
        WHERE id = ?
        """,
        (video_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"未找到视频 ID：{video_id}")
    if not row["media_path"]:
        raise ValueError(f"视频 ID {video_id} 没有本地视频文件")
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
    export_library(conn, WEB_ROOT / "library.json")
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
            video_id = 0
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                result = transcribe_video(video_id)
                json_response(self, 200, {"ok": True, "result": result})
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                log_event(LOGGER, "transcribe.failure", video_id=video_id or None, error=error)
                conn = connect()
                init_db(conn)
                record_failed_task(conn, "transcribe", error, video_id=video_id or None, payload={"video_id": video_id})
                if video_id:
                    try:
                        update_video_paths(conn, video_id, status="transcribe_failed", error=error)
                    except Exception:
                        pass
                export_library(conn, WEB_ROOT / "library.json")
                json_response(self, 500, {"ok": False, "error": f"{type(e).__name__}: {e}"})
            return
        if path == "/api/analyze":
            video_id = 0
            try:
                payload = read_json(self)
                video_id = bounded_int(payload.get("video_id"), 0, 1, 10_000_000)
                result = analyze_video(video_id)
                json_response(self, 200, {"ok": True, "result": result})
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                log_event(LOGGER, "analyze.failure", video_id=video_id or None, error=error)
                conn = connect()
                init_db(conn)
                record_failed_task(conn, "analyze", error, video_id=video_id or None, payload={"video_id": video_id})
                if video_id:
                    try:
                        update_video_paths(conn, video_id, status="analyze_failed", error=error)
                    except Exception:
                        pass
                export_library(conn, WEB_ROOT / "library.json")
                json_response(self, 500, {"ok": False, "error": error})
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
                library = export_library(conn, WEB_ROOT / "library.json")
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
                library = export_library(conn, WEB_ROOT / "library.json")
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
            log_event(
                LOGGER,
                "harvest.start",
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
                )
            )
            conn = connect()
            saved = save_harvest_result(conn, result, category=category, profile_url=target)
            clear_failed_task(conn, "harvest")
            library = export_library(conn, WEB_ROOT / "library.json")
            log_event(
                LOGGER,
                "harvest.success",
                sec_user_id=result.get("sec_user_id"),
                creator_nickname=result.get("creator_nickname"),
                fetched_video_count=result.get("fetched_video_count"),
                video_count=result.get("video_count"),
                saved_video_count=saved.get("saved_video_count"),
            )
            json_response(
                self,
                200,
                {
                    "ok": True,
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
