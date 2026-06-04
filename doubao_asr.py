import json
import os
import ssl
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any
import urllib.error
import urllib.request

from volcengine import load_local_env

ROOT = Path(__file__).resolve().parent
ASR_AUDIO_DIR = ROOT / "data" / "asr_audio"
ASR_RESULT_DIR = ROOT / "data" / "asr_results"
DEFAULT_ASR_ENDPOINT = "https://openspeech.bytedance.com/api/v3/auc/bigmodel/recognize/flash"
DEFAULT_ASR_RESOURCE_ID = "volc.seedasr.auc"
DEFAULT_ASR_SUBMIT_ENDPOINT = "https://openspeech.bytedance.com/api/v3/auc/bigmodel/submit"
DEFAULT_ASR_QUERY_ENDPOINT = "https://openspeech.bytedance.com/api/v3/auc/bigmodel/query"


def run_command(args: list[str], timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(args, cwd=str(ROOT), text=True, capture_output=True, timeout=timeout, check=True)
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or "").strip()
        stdout = (e.stdout or "").strip()
        raise RuntimeError(f"命令执行失败：{' '.join(args)}\nSTDERR: {stderr[-2000:]}\nSTDOUT: {stdout[-1000:]}") from e


def extract_audio_for_asr(media_path: str | Path, video_id: int) -> Path:
    media = Path(media_path)
    if not media.exists():
        raise FileNotFoundError(f"视频文件不存在：{media}")
    ASR_AUDIO_DIR.mkdir(parents=True, exist_ok=True)
    out = ASR_AUDIO_DIR / f"video_{video_id}.mp3"
    run_command(
        [
            "ffmpeg",
            "-y",
            "-i",
            str(media),
            "-vn",
            "-ac",
            "1",
            "-ar",
            "16000",
            "-b:a",
            "64k",
            str(out),
        ],
        timeout=600,
    )
    return out


def require_asr_config() -> dict:
    load_local_env()
    auth_mode = os.environ.get("DOUBAO_ASR_AUTH_MODE", "").strip().lower()
    api_mode = os.environ.get("DOUBAO_ASR_API_MODE", "standard").strip().lower()
    api_key = os.environ.get("DOUBAO_ASR_API_KEY", "").strip()
    app_key = os.environ.get("DOUBAO_ASR_APP_KEY", "").strip()
    access_key = os.environ.get("DOUBAO_ASR_ACCESS_KEY", "").strip()
    endpoint = os.environ.get("DOUBAO_ASR_ENDPOINT", DEFAULT_ASR_ENDPOINT).strip()
    submit_endpoint = os.environ.get("DOUBAO_ASR_SUBMIT_ENDPOINT", DEFAULT_ASR_SUBMIT_ENDPOINT).strip()
    query_endpoint = os.environ.get("DOUBAO_ASR_QUERY_ENDPOINT", DEFAULT_ASR_QUERY_ENDPOINT).strip()
    resource_id = os.environ.get("DOUBAO_ASR_RESOURCE_ID", DEFAULT_ASR_RESOURCE_ID).strip()
    if auth_mode == "legacy":
        if not app_key or not access_key:
            raise RuntimeError("旧版豆包语音 ASR 认证缺少 DOUBAO_ASR_APP_KEY 或 DOUBAO_ASR_ACCESS_KEY")
        auth_headers = {
            "X-Api-App-Key": app_key,
            "X-Api-Access-Key": access_key,
        }
        uid = app_key
    else:
        if not api_key:
            raise RuntimeError("缺少 DOUBAO_ASR_API_KEY")
        auth_headers = {"X-Api-Key": api_key}
        uid = api_key
    return {
        "api_mode": api_mode,
        "endpoint": endpoint,
        "submit_endpoint": submit_endpoint,
        "query_endpoint": query_endpoint,
        "resource_id": resource_id,
        "uid": uid,
        "auth_headers": auth_headers,
    }


def ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def request_json(url: str, payload: dict | None, headers: dict[str, str], timeout: int = 180) -> tuple[dict, dict[str, str]]:
    body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ssl_context()) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            response_headers = {k.lower(): v for k, v in resp.headers.items()}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"豆包语音 ASR 失败：HTTP {e.code} {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"豆包语音 ASR 网络失败：{e}") from e
    try:
        data = json.loads(text) if text else {}
    except json.JSONDecodeError:
        data = {"raw_text": text}
    return data, response_headers


def audio_url_for_standard(audio_path: Path) -> str:
    direct = os.environ.get("DOUBAO_ASR_AUDIO_URL", "").strip()
    if direct:
        return direct
    provider = os.environ.get("DOUBAO_ASR_UPLOAD_PROVIDER", "mp3tourl").strip().lower()
    if provider == "mp3tourl":
        return upload_audio_mp3tourl(audio_path)
    if provider == "bashupload":
        return upload_audio_bashupload(audio_path)
    template = os.environ.get("DOUBAO_ASR_AUDIO_URL_TEMPLATE", "").strip()
    if template:
        return template.format(filename=audio_path.name, path=audio_path)
    public_base = os.environ.get("DOUBAO_ASR_PUBLIC_BASE_URL", "").strip().rstrip("/")
    if public_base:
        return f"{public_base}/{audio_path.name}"
    raise RuntimeError(
        "标准版豆包 ASR 需要公网可访问的音频 URL。请配置 DOUBAO_ASR_AUDIO_URL、"
        "DOUBAO_ASR_AUDIO_URL_TEMPLATE、DOUBAO_ASR_PUBLIC_BASE_URL，或使用 DOUBAO_ASR_UPLOAD_PROVIDER=mp3tourl。"
    )


def upload_audio_mp3tourl(audio_path: Path) -> str:
    result = run_command(
        [
            "curl",
            "-sS",
            "-F",
            f"file=@{audio_path}",
            "https://www.mp3tourl.com/api/upload-audio",
        ],
        timeout=180,
    )
    text = (result.stdout or "").strip()
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"mp3tourl 未返回 JSON：{text[:1000]}") from e
    if not data.get("success") or not data.get("url"):
        raise RuntimeError(f"mp3tourl 上传失败：{json.dumps(data, ensure_ascii=False)[:1000]}")
    return str(data["url"])


def upload_audio_bashupload(audio_path: Path) -> str:
    result = run_command(
        [
            "curl",
            "-sS",
            "-k",
            "-T",
            str(audio_path),
            f"https://bashupload.app/{audio_path.name}",
        ],
        timeout=180,
    )
    text = (result.stdout or "").strip()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("http://") or line.startswith("https://"):
            return line.replace("http://", "https://", 1)
    raise RuntimeError(f"bashupload 未返回可用链接：{text[:1000]}")


def recognize_audio_file(audio_path: str | Path) -> dict:
    config = require_asr_config()
    audio = Path(audio_path)
    if not audio.exists():
        raise FileNotFoundError(f"音频文件不存在：{audio}")
    suffix = audio.suffix.lower().lstrip(".") or "mp3"
    if suffix == "m4a":
        suffix = "mp4"
    if config["api_mode"] == "flash":
        return recognize_audio_file_flash(audio, suffix, config)
    return recognize_audio_file_standard(audio, suffix, config)


def recognize_audio_file_flash(audio: Path, suffix: str, config: dict) -> dict:
    import base64

    audio_b64 = base64.b64encode(audio.read_bytes()).decode("ascii")
    request_id = str(uuid.uuid4())
    payload = {
        "user": {"uid": config["uid"]},
        "audio": {
            "data": audio_b64,
            "format": suffix,
        },
        "request": {
            "model_name": "bigmodel",
            "enable_itn": True,
            "enable_punc": True,
            "enable_ddc": True,
        },
    }
    data, headers = request_json(
        config["endpoint"],
        payload,
        {
            "Content-Type": "application/json",
            "X-Api-Resource-Id": config["resource_id"],
            "X-Api-Request-Id": request_id,
            "X-Api-Sequence": "-1",
            **config["auth_headers"],
        },
    )
    status_code = headers.get("x-api-status-code")
    status_message = headers.get("x-api-message")
    if status_code and status_code not in {"20000000", "0"}:
        raise RuntimeError(f"豆包语音 ASR 状态异常：{status_code} {status_message or ''} {json.dumps(data, ensure_ascii=False)[:1000]}")
    data["_headers"] = {
        "x_api_status_code": status_code,
        "x_api_message": status_message,
        "x_api_request_id": request_id,
    }
    return data


def recognize_audio_file_standard(audio: Path, suffix: str, config: dict) -> dict:
    audio_url = audio_url_for_standard(audio)
    request_id = str(uuid.uuid4())
    common_headers = {
        "Content-Type": "application/json",
        "X-Api-Resource-Id": config["resource_id"],
        "X-Api-Request-Id": request_id,
        **config["auth_headers"],
    }
    submit_payload = {
        "user": {"uid": config["uid"]},
        "audio": {"url": audio_url, "format": suffix},
        "request": {
            "model_name": "bigmodel",
            "enable_itn": True,
            "enable_punc": True,
            "enable_ddc": True,
        },
    }
    submit_data, submit_headers = request_json(
        config["submit_endpoint"],
        submit_payload,
        common_headers,
        timeout=60,
    )
    submit_status = submit_headers.get("x-api-status-code")
    if submit_status and submit_status not in {"20000000", "0"}:
        raise RuntimeError(f"豆包语音 ASR 提交异常：{submit_status} {submit_headers.get('x-api-message') or ''} {json.dumps(submit_data, ensure_ascii=False)[:1000]}")
    x_tt_logid = submit_headers.get("x-tt-logid", "")

    last_data: dict = {}
    last_headers: dict[str, str] = {}
    for _ in range(90):
        time.sleep(2)
        last_data, last_headers = request_json(
            config["query_endpoint"],
            {},
            {**common_headers, **({"X-Tt-Logid": x_tt_logid} if x_tt_logid else {})},
            timeout=60,
        )
        status_code = last_headers.get("x-api-status-code")
        status_message = last_headers.get("x-api-message") or ""
        if status_code in {"20000000", "0"}:
            last_data["_headers"] = {
                "x_api_status_code": status_code,
                "x_api_message": status_message,
                "x_api_request_id": request_id,
                "audio_url": audio_url,
            }
            return last_data
        if status_code in {"20000001", "20000002", "20000003"}:
            continue
        if status_code:
            raise RuntimeError(f"豆包语音 ASR 查询异常：{status_code} {status_message} {json.dumps(last_data, ensure_ascii=False)[:1000]}")
    raise RuntimeError(f"豆包语音 ASR 查询超时：{json.dumps(last_data, ensure_ascii=False)[:1000]} {last_headers}")


def extract_transcript_text(result: Any) -> str:
    if not isinstance(result, dict):
        return ""
    candidates = [
        result.get("text"),
        result.get("result", {}).get("text") if isinstance(result.get("result"), dict) else None,
        result.get("data", {}).get("text") if isinstance(result.get("data"), dict) else None,
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    for container_key in ("result", "data"):
        container = result.get(container_key)
        if not isinstance(container, dict):
            continue
        utterances = container.get("utterances") or container.get("utterance") or []
        if isinstance(utterances, list):
            texts = [item.get("text", "").strip() for item in utterances if isinstance(item, dict)]
            joined = "\n".join(t for t in texts if t)
            if joined:
                return joined
    return ""


def save_asr_result(video_id: int, result: dict) -> Path:
    ASR_RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out = ASR_RESULT_DIR / f"video_{video_id}_asr.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return out
