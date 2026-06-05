#!/usr/bin/env python3
"""微信视频号 A1 最小化测试：按作者拉全量作品并提交下载任务。"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Any


def request_json(base_url: str, path: str, params: dict[str, str] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def clean_filename(value: str, fallback: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    value = re.sub(r'[\\/:*?"<>|\r\n]+', "_", value)
    value = value[:80].strip(" ._")
    return value or fallback


def normalize_feed(obj: dict[str, Any], index: int, use_highest: bool) -> dict[str, Any] | None:
    media_items = (((obj.get("objectDesc") or {}).get("media")) or [])
    if not media_items:
        return None
    media = media_items[0]
    url = (media.get("url") or "") + (media.get("urlToken") or "")
    if not url:
        return None
    specs = media.get("spec") or []
    spec = None if use_highest else ((specs[0] or {}).get("fileFormat") if specs else None)
    if spec:
        url = url + "&X-snsvideoflag=" + urllib.parse.quote(str(spec))
    else:
        # 和 wx_channels_download 前端逻辑保持一致：最高画质时只保留 encfilekey 和 token。
        parsed = urllib.parse.urlparse(urllib.parse.unquote(url))
        query = urllib.parse.parse_qs(parsed.query)
        encfilekey = (query.get("encfilekey") or [""])[0]
        token = (query.get("token") or [""])[0]
        if encfilekey and token:
            url = urllib.parse.urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    "",
                    urllib.parse.urlencode({"encfilekey": encfilekey, "token": token}),
                    "",
                )
            )
    title = (obj.get("objectDesc") or {}).get("description") or obj.get("id") or f"feed_{index}"
    filename = clean_filename(title, obj.get("id") or f"feed_{index}")
    key_raw = media.get("decodeKey") or "0"
    try:
        key = int(key_raw)
    except (TypeError, ValueError):
        key = 0
    return {
        "id": str(obj.get("id") or f"feed_{index}"),
        "url": url,
        "title": title,
        "key": key,
        "filename": filename,
        "spec": spec,
        "suffix": ".mp4",
    }


def fetch_all_feeds(base_url: str, username: str, max_pages: int, sleep_seconds: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    feeds: list[dict[str, Any]] = []
    creator: dict[str, Any] = {}
    next_marker = ""
    seen_markers: set[str] = set()
    for page in range(1, max_pages + 1):
        data = request_json(
            base_url,
            "/api/channels/contact/feed/list",
            {"username": username, "next_marker": next_marker},
        )
        if data.get("code") != 0:
            raise RuntimeError(f"拉取作品列表失败：{data}")
        payload = data.get("data") or {}
        if payload.get("errCode") != 0:
            raise RuntimeError(f"视频号接口失败：{payload}")
        inner = payload.get("data") or {}
        if not creator:
            creator = inner.get("contact") or {}
        objects = inner.get("object") or []
        feeds.extend(objects)
        print(f"第 {page} 页：{len(objects)} 条，累计 {len(feeds)} 条", flush=True)
        marker = inner.get("lastBuffer") or ""
        if not marker or marker in seen_markers or len(objects) < 15:
            break
        seen_markers.add(marker)
        next_marker = marker
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return feeds, creator


def create_download_tasks(base_url: str, objects: list[dict[str, Any]], use_highest: bool) -> list[str]:
    feeds = [normalize_feed(obj, i + 1, use_highest) for i, obj in enumerate(objects)]
    body = {"feeds": [feed for feed in feeds if feed]}
    if not body["feeds"]:
        return []
    data = request_json(base_url, "/api/task/create_batch", body=body)
    if data.get("code") != 0:
        raise RuntimeError(f"创建下载任务失败：{data}")
    return ((data.get("data") or {}).get("ids")) or []


def main() -> int:
    parser = argparse.ArgumentParser(description="微信视频号 A1 最小化测试")
    parser.add_argument("--base-url", default="http://127.0.0.1:38129", help="wx_channels_download API 地址")
    parser.add_argument("--username", required=True, help="视频号 username，例如 v2_xxx@finder")
    parser.add_argument("--max-pages", type=int, default=100, help="最多分页次数")
    parser.add_argument("--sleep", type=float, default=0.4, help="每页之间的等待秒数")
    parser.add_argument("--highest", action="store_true", help="下载最高画质；默认使用列表中的第一个 spec")
    parser.add_argument("--dry-run", action="store_true", help="只拉列表，不创建下载任务")
    args = parser.parse_args()

    status = request_json(args.base_url, "/api/status")
    print("API 状态：", json.dumps(status, ensure_ascii=False), flush=True)
    if not ((status.get("data") or {}).get("channels") or {}).get("available"):
        print("提示：channels.available=false。请确认微信 PC 视频号页面已通过代理打开并初始化 socket。", flush=True)

    objects, creator = fetch_all_feeds(args.base_url, args.username, args.max_pages, args.sleep)
    print(f"作者：{creator.get('nickname') or args.username}")
    print(f"拉取作品数：{len(objects)}")
    if args.dry_run:
        return 0
    ids = create_download_tasks(args.base_url, objects, args.highest)
    print(f"已创建下载任务：{len(ids)} 个")
    if ids:
        print("任务 ID：", ", ".join(ids[:20]) + (" ..." if len(ids) > 20 else ""))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
