import json
import os
import subprocess
from pathlib import Path
from typing import Any

from db import connect, init_db, save_analysis, save_transcript, update_video_paths, upsert_creator, upsert_video
from volcengine import analyze_with_doubao, parse_json_object

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
MEDIA = DATA / "media"
AUDIO = DATA / "audio"
TRANSCRIPTS = DATA / "transcripts"
ANALYSIS = DATA / "analysis"


def run_command(args: list[str], cwd: Path = ROOT, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, cwd=str(cwd), text=True, capture_output=True, timeout=timeout, check=True)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        raise RuntimeError(f"命令执行失败：{' '.join(args)}\nSTDERR: {stderr[-2000:]}\nSTDOUT: {stdout[-1000:]}") from e


def probe_metadata(url: str) -> dict[str, Any]:
    result = run_command(["yt-dlp", "--dump-single-json", "--no-playlist", url], timeout=180)
    return json.loads(result.stdout)


def download_video(url: str) -> tuple[Path, dict[str, Any]]:
    MEDIA.mkdir(parents=True, exist_ok=True)
    metadata = probe_metadata(url)
    output_template = str(MEDIA / "%(extractor)s_%(id)s.%(ext)s")
    result = run_command(
        [
            "yt-dlp",
            "--no-playlist",
            "--restrict-filenames",
            "-f",
            "bv*+ba/b",
            "-o",
            output_template,
            "--print",
            "after_move:filepath",
            url,
        ],
        timeout=900,
    )
    paths = [Path(line.strip()) for line in result.stdout.splitlines() if line.strip()]
    media_path = paths[-1] if paths else find_downloaded_file(metadata)
    if not media_path.exists():
        raise RuntimeError(f"下载完成但未找到媒体文件：{media_path}")
    return media_path, metadata


def find_downloaded_file(metadata: dict[str, Any]) -> Path:
    source_id = str(metadata.get("id") or "")
    for p in MEDIA.glob(f"*{source_id}*"):
        if p.is_file() and p.suffix.lower() not in {".json", ".part"}:
            return p
    raise RuntimeError("未找到下载后的视频文件")


def metadata_to_video(url: str, metadata: dict[str, Any], creator_id: int, media_path: Path | None = None) -> dict[str, Any]:
    timestamp = metadata.get("timestamp") or metadata.get("release_timestamp")
    published = None
    if timestamp:
        try:
            from datetime import datetime, timezone

            published = datetime.fromtimestamp(int(timestamp), timezone.utc).astimezone().isoformat(timespec="seconds")
        except Exception:
            published = None
    return {
        "platform": metadata.get("extractor_key") or metadata.get("extractor") or "video",
        "source_url": url,
        "source_id": metadata.get("id"),
        "creator_id": creator_id,
        "title": metadata.get("title") or metadata.get("fulltitle"),
        "description": metadata.get("description"),
        "duration": metadata.get("duration"),
        "published_at": published or metadata.get("upload_date"),
        "view_count": metadata.get("view_count"),
        "like_count": metadata.get("like_count"),
        "comment_count": metadata.get("comment_count"),
        "repost_count": metadata.get("repost_count") or metadata.get("share_count"),
        "favorite_count": metadata.get("favorite_count"),
        "media_path": str(media_path) if media_path else None,
        "metadata": metadata,
        "status": "downloaded" if media_path else "metadata_only",
    }


def extract_audio(video_path: Path, video_id: int, max_seconds: int | None = None) -> Path:
    AUDIO.mkdir(parents=True, exist_ok=True)
    out = AUDIO / f"video_{video_id}.wav"
    args = ["ffmpeg", "-y", "-i", str(video_path), "-vn", "-ac", "1", "-ar", "16000"]
    if max_seconds:
        args.extend(["-t", str(max_seconds)])
    args.append(str(out))
    run_command(args, timeout=600)
    return out


def transcribe_audio(audio_path: Path, video_id: int, model: str = "tiny", language: str = "Chinese") -> tuple[str, Path, str]:
    TRANSCRIPTS.mkdir(parents=True, exist_ok=True)
    out_dir = TRANSCRIPTS / f"video_{video_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    result = run_command(
        [
            "whisper",
            str(audio_path),
            "--language",
            language,
            "--model",
            model,
            "--output_format",
            "txt",
            "--output_dir",
            str(out_dir),
        ],
        timeout=1800,
    )
    txt_files = sorted(out_dir.glob("*.txt"), key=lambda p: p.stat().st_mtime, reverse=True)
    if txt_files:
        text = txt_files[0].read_text(encoding="utf-8", errors="replace").strip()
        return text, txt_files[0], result.stdout + result.stderr
    raise RuntimeError("whisper 未生成 txt 转写文件")


def run_pipeline(url: str, creator: str, category: str, comment_limit: int = 20, max_seconds: int | None = 90, whisper_model: str = "tiny", whisper_language: str = "Chinese") -> dict[str, Any]:
    del comment_limit
    DATA.mkdir(parents=True, exist_ok=True)
    conn = connect()
    init_db(conn)
    creator_id = upsert_creator(conn, name=creator, category=category)

    media_path, metadata = download_video(url)
    video_id = upsert_video(conn, metadata_to_video(url, metadata, creator_id, media_path))

    audio_path = extract_audio(media_path, video_id, max_seconds=max_seconds)
    update_video_paths(conn, video_id, audio_path=str(audio_path), status="audio_ready")

    transcript_text, transcript_path, _ = transcribe_audio(audio_path, video_id, model=whisper_model, language=whisper_language)
    save_transcript(conn, video_id, transcript_text, str(transcript_path), "whisper", whisper_model)
    update_video_paths(conn, video_id, status="transcribed")

    analysis_text = analyze_with_doubao(
        title=metadata.get("title") or "",
        description=metadata.get("description") or "",
        transcript=transcript_text,
        comments=[],
    )
    parsed = parse_json_object(analysis_text)
    save_analysis(conn, video_id, "volcengine-ark", os.environ.get("VOLCENGINE_ENDPOINT_ID", ""), analysis_text, parsed)
    ANALYSIS.mkdir(parents=True, exist_ok=True)
    analysis_path = ANALYSIS / f"video_{video_id}_analysis.json"
    analysis_path.write_text(analysis_text, encoding="utf-8")
    update_video_paths(conn, video_id, status="analyzed")

    return {
        "video_id": video_id,
        "media_path": str(media_path),
        "audio_path": str(audio_path),
        "transcript_path": str(transcript_path),
        "analysis_path": str(analysis_path),
        "title": metadata.get("title"),
        "transcript_preview": transcript_text[:500],
        "analysis_preview": analysis_text[:800],
    }


def analyze_existing(video_id: int) -> dict[str, Any]:
    conn = connect()
    init_db(conn)
    row = conn.execute(
        """
        SELECT v.id, v.title, v.description, t.transcript_text
        FROM videos v
        JOIN transcripts t ON t.video_id = v.id
        WHERE v.id = ?
        """,
        (video_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"未找到已转写的视频：{video_id}")
    analysis_text = analyze_with_doubao(
        title=row["title"] or "",
        description=row["description"] or "",
        transcript=row["transcript_text"] or "",
        comments=[],
    )
    parsed = parse_json_object(analysis_text)
    save_analysis(conn, video_id, "volcengine-ark", os.environ.get("VOLCENGINE_ENDPOINT_ID", ""), analysis_text, parsed)
    ANALYSIS.mkdir(parents=True, exist_ok=True)
    analysis_path = ANALYSIS / f"video_{video_id}_analysis.json"
    analysis_path.write_text(analysis_text, encoding="utf-8")
    update_video_paths(conn, video_id, status="analyzed")
    return {
        "video_id": video_id,
        "analysis_path": str(analysis_path),
        "analysis_preview": analysis_text[:800],
    }
