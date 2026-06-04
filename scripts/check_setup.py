#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXTERNAL = ROOT / "external" / "Douyin_TikTok_Download_API"
ENV_PATH = ROOT / ".env"


def load_env_file() -> dict[str, str]:
    values: dict[str, str] = {}
    if not ENV_PATH.exists():
        return values
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


def status(kind: str, message: str) -> None:
    print(f"[{kind}] {message}")


def command_ok(name: str) -> bool:
    path = shutil.which(name)
    if path:
        status("OK", f"{name}: {path}")
        return True
    status("FAIL", f"缺少命令：{name}")
    return False


def import_ok(module: str, label: str | None = None) -> bool:
    if importlib.util.find_spec(module):
        status("OK", f"Python 模块可导入：{label or module}")
        return True
    status("FAIL", f"Python 模块不可导入：{label or module}")
    return False


def warn_import(module: str, label: str | None = None) -> bool:
    if importlib.util.find_spec(module):
        status("OK", f"Python 模块可导入：{label or module}")
        return True
    status("WARN", f"Python 模块不可导入：{label or module}")
    return False


def main() -> int:
    failures = 0
    warnings = 0

    version = sys.version_info
    if version >= (3, 10):
        status("OK", f"Python 版本：{version.major}.{version.minor}.{version.micro}")
    else:
        status("FAIL", f"Python 版本过低：{version.major}.{version.minor}.{version.micro}，需要 3.10+")
        failures += 1

    for command in ("git", "curl", "ffmpeg"):
        if not command_ok(command):
            failures += 1

    if ENV_PATH.exists():
        status("OK", ".env 已存在")
    else:
        status("FAIL", "缺少 .env，请先执行 cp .env.example .env 并填写配置")
        failures += 1

    env = {**os.environ, **load_env_file()}
    for key in ("VOLCENGINE_API_KEY", "VOLCENGINE_ENDPOINT_ID"):
        value = env.get(key, "").strip()
        if value and not value.startswith("your_") and value != "ep_xxx":
            status("OK", f"{key} 已配置")
        else:
            status("FAIL", f"{key} 未配置或仍是占位值")
            failures += 1

    auth_mode = env.get("DOUBAO_ASR_AUTH_MODE", "api_key").strip().lower()
    if auth_mode == "legacy":
        required = ("DOUBAO_ASR_APP_KEY", "DOUBAO_ASR_ACCESS_KEY")
    else:
        required = ("DOUBAO_ASR_API_KEY",)
    for key in required:
        value = env.get(key, "").strip()
        if value and not value.startswith("your_"):
            status("OK", f"{key} 已配置")
        else:
            status("FAIL", f"{key} 未配置或仍是占位值")
            failures += 1

    if EXTERNAL.exists():
        status("OK", f"外部项目目录存在：{EXTERNAL.relative_to(ROOT)}")
    else:
        status("FAIL", "缺少 external/Douyin_TikTok_Download_API，请按 SETUP.md 克隆")
        failures += 1

    if (EXTERNAL / "requirements.txt").exists():
        status("OK", "外部项目 requirements.txt 存在")
    else:
        status("WARN", "外部项目 requirements.txt 不存在，可能没有完整克隆")
        warnings += 1

    if str(EXTERNAL) not in sys.path:
        sys.path.insert(0, str(EXTERNAL))

    for module in ("httpx", "certifi"):
        if not warn_import(module):
            warnings += 1

    for module, label in (
        ("crawlers.douyin.web.web_crawler", "Douyin_TikTok_Download_API crawlers"),
        ("Cryptodome", "pycryptodomex / Cryptodome"),
        ("gmssl", "gmssl"),
    ):
        if not import_ok(module, label):
            failures += 1

    for rel in ("data", "logs", "web"):
        path = ROOT / rel
        try:
            path.mkdir(parents=True, exist_ok=True)
            marker = path / ".setup_check"
            marker.write_text("ok", encoding="utf-8")
            marker.unlink(missing_ok=True)
            status("OK", f"目录可写：{rel}/")
        except Exception as exc:
            status("FAIL", f"目录不可写：{rel}/，{exc}")
            failures += 1

    if env.get("DOUBAO_ASR_API_MODE", "standard").strip().lower() == "standard":
        provider = env.get("DOUBAO_ASR_UPLOAD_PROVIDER", "mp3tourl").strip() or "mp3tourl"
        status("WARN", f"豆包 ASR standard 模式需要公网音频 URL，当前上传方式：{provider}")
        warnings += 1

    print()
    if failures:
        status("FAIL", f"环境检查未通过：{failures} 个阻塞问题，{warnings} 个提醒")
        return 1
    if warnings:
        status("WARN", f"环境基本可用，但有 {warnings} 个提醒")
        return 0
    status("OK", "环境检查通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
