import json
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

from app_logging import log_event

import logging

LOGGER = logging.getLogger("douyin_live_research.wechat_channels")

DEFAULT_BASE_URL = os.environ.get("WECHAT_CHANNELS_API_BASE_URL", "http://127.0.0.1:38129")


class WeChatChannelsError(RuntimeError):
    pass


def request_json(base_url: str, path: str, params: dict[str, str] | None = None, body: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    url = base_url.rstrip("/") + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    payload = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers=headers, method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        raise WeChatChannelsError(f"无法连接视频号本地服务：{base_url}。请确认 wx_channels_download 已启动。{type(e).__name__}: {e}") from e


def check_status(base_url: str = DEFAULT_BASE_URL) -> dict[str, Any]:
    try:
        data = request_json(base_url, "/api/status", timeout=8)
    except WeChatChannelsError as e:
        return {"ok": False, "available": False, "base_url": base_url, "error": str(e)}
    channels = ((data.get("data") or {}).get("channels") or {})
    available = bool(channels.get("available"))
    return {
        "ok": True,
        "available": available,
        "base_url": base_url,
        "version": ((data.get("data") or {}).get("version")),
        "raw": data,
        "message": "视频号页面已连接" if available else "本地服务已启动，但微信 PC 视频号页面尚未完成 socket 初始化",
    }


def assert_channels_ready(base_url: str) -> None:
    status = check_status(base_url)
    if not status.get("ok"):
        raise WeChatChannelsError(status.get("error") or "视频号本地服务不可用")
    if not status.get("available"):
        raise WeChatChannelsError("视频号本地服务已启动，但微信 PC 视频号页面尚未完成 socket 初始化。请通过代理打开视频号作者页，等待页面出现下载或批量下载按钮。")


def search_contacts(keyword: str, base_url: str = DEFAULT_BASE_URL, next_marker: str = "") -> dict[str, Any]:
    assert_channels_ready(base_url)
    data = request_json(base_url, "/api/channels/contact/search", {"keyword": keyword, "next_marker": next_marker})
    if data.get("code") != 0:
        raise WeChatChannelsError(data.get("msg") or f"搜索失败：{data}")
    payload = data.get("data") or {}
    return {
        "items": (((payload.get("data") or {}).get("infoList")) or []),
        "next_marker": ((payload.get("data") or {}).get("lastBuff")) or "",
        "raw": payload,
    }


def clean_filename(value: str, fallback: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    value = re.sub(r'[\\/:*?"<>|\r\n]+', "_", value)
    value = value[:96].strip(" ._")
    return value or fallback


def feed_source_url(feed: dict[str, Any]) -> str:
    oid = str(feed.get("id") or "")
    nid = str(feed.get("objectNonceId") or "")
    if oid and nid:
        return f"https://channels.weixin.qq.com/web/pages/feed?oid={urllib.parse.quote(oid)}&nid={urllib.parse.quote(nid)}"
    return f"wechat_channels://feed/{oid or nid}"


def feed_to_download_task(feed: dict[str, Any], use_highest: bool = False) -> dict[str, Any] | None:
    media_items = (((feed.get("objectDesc") or {}).get("media")) or [])
    if not media_items:
        return None
    media = media_items[0]
    url = (media.get("url") or "") + (media.get("urlToken") or "")
    if not url:
        return None
    specs = media.get("spec") or []
    spec = None if use_highest else ((specs[0] or {}).get("fileFormat") if specs else None)
    if spec:
        url += "&X-snsvideoflag=" + urllib.parse.quote(str(spec))
    else:
        parsed = urllib.parse.urlparse(urllib.parse.unquote(url))
        query = urllib.parse.parse_qs(parsed.query)
        encfilekey = (query.get("encfilekey") or [""])[0]
        token = (query.get("token") or [""])[0]
        if encfilekey and token:
            url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", urllib.parse.urlencode({"encfilekey": encfilekey, "token": token}), ""))
    title = (feed.get("objectDesc") or {}).get("description") or feed.get("id") or "未命名视频"
    key_raw = media.get("decodeKey") or "0"
    try:
        key = int(key_raw)
    except Exception:
        key = 0
    return {
        "id": str(feed.get("id") or ""),
        "url": url,
        "title": title,
        "key": key,
        "filename": clean_filename(title, str(feed.get("id") or "wechat_channels")),
        "spec": spec,
        "suffix": ".mp4",
    }


def fetch_all_feeds(
    username: str,
    base_url: str = DEFAULT_BASE_URL,
    max_pages: int | None = 100,
    progress_callback: Callable[..., None] | None = None,
    sleep_seconds: float = 0.4,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    assert_channels_ready(base_url)
    feeds: list[dict[str, Any]] = []
    creator: dict[str, Any] = {}
    next_marker = ""
    seen_markers: set[str] = set()
    page = 0
    while True:
        page += 1
        if max_pages and page > max_pages:
            break
        data = request_json(base_url, "/api/channels/contact/feed/list", {"username": username, "next_marker": next_marker})
        if data.get("code") != 0:
            raise WeChatChannelsError(data.get("msg") or f"拉取作品列表失败：{data}")
        payload = data.get("data") or {}
        if payload.get("errCode") != 0:
            raise WeChatChannelsError(payload.get("errMsg") or f"视频号接口失败：{payload}")
        inner = payload.get("data") or {}
        if not creator:
            creator = inner.get("contact") or {}
        page_feeds = inner.get("object") or []
        feeds.extend(page_feeds)
        if progress_callback:
            progress_callback(stage="list", label=f"拉取视频号作品列表 {len(feeds)} 条", done=len(feeds), total=0, status="running")
        log_event(LOGGER, "wechat.feed_page", username=username, page=page, count=len(page_feeds), total=len(feeds))
        marker = inner.get("lastBuffer") or ""
        if not marker or marker in seen_markers or len(page_feeds) < 15:
            break
        seen_markers.add(marker)
        next_marker = marker
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return feeds, creator


def fetch_top_comments(base_url: str, feed: dict[str, Any], limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if limit <= 0:
        return [], {}
    oid = str(feed.get("id") or "")
    nid = str(feed.get("objectNonceId") or "")
    if not oid or not nid:
        return [], {}
    data = request_json(base_url, "/api/channels/feed/comment/list", {"oid": oid, "nid": nid})
    if data.get("code") != 0:
        raise WeChatChannelsError(data.get("msg") or f"评论拉取失败：{data}")
    payload = data.get("data") or {}
    inner = payload.get("data") or {}
    comments = inner.get("commentInfo") or []
    comments.sort(key=lambda item: int(item.get("likeCount") or 0), reverse=True)
    top = []
    for item in comments[:limit]:
        top.append(
            {
                "content": item.get("content"),
                "like_count": item.get("likeCount") or 0,
                "create_time": item.get("createtime"),
                "cid": item.get("commentId"),
                "raw": item,
            }
        )
    return top, inner.get("countInfo") or {}


def create_download_tasks(base_url: str, feeds: list[dict[str, Any]], use_highest: bool = False) -> list[str]:
    tasks = [feed_to_download_task(feed, use_highest=use_highest) for feed in feeds]
    valid_tasks = [task for task in tasks if task]
    if not valid_tasks:
        return []
    data = request_json(base_url, "/api/task/create_batch", body={"feeds": valid_tasks})
    if data.get("code") != 0:
        raise WeChatChannelsError(data.get("msg") or f"创建下载任务失败：{data}")
    return ((data.get("data") or {}).get("ids")) or []


def list_download_tasks(base_url: str, page_size: int = 200) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    page = 1
    while True:
        data = request_json(base_url, "/api/task/list", {"status": "all", "page": str(page), "page_size": str(page_size)})
        if data.get("code") != 0:
            raise WeChatChannelsError(data.get("msg") or f"读取下载任务失败：{data}")
        payload = data.get("data") or {}
        items = payload.get("list") or []
        result.extend(items)
        total = int(payload.get("total") or len(result))
        if len(result) >= total or not items:
            break
        page += 1
    return result


def task_media_path(task: dict[str, Any]) -> str | None:
    meta = task.get("meta") or {}
    opts = meta.get("opts") or {}
    path = opts.get("path")
    name = task.get("name") or opts.get("name")
    if not path or not name:
        return None
    return str(Path(path).expanduser() / name)


def map_downloaded_paths(base_url: str, source_ids: set[str]) -> tuple[dict[str, str], dict[str, str]]:
    done: dict[str, str] = {}
    errors: dict[str, str] = {}
    for task in list_download_tasks(base_url):
        labels = (((task.get("meta") or {}).get("req") or {}).get("labels") or {})
        feed_id = str(labels.get("id") or "")
        if feed_id not in source_ids:
            continue
        status = task.get("status")
        if status == "done":
            path = task_media_path(task)
            if path:
                done[feed_id] = path
        elif status == "error":
            errors[feed_id] = json.dumps(task.get("error") or task, ensure_ascii=False)[:500]
    return done, errors


def wait_for_downloads(
    base_url: str,
    source_ids: set[str],
    timeout_seconds: int,
    progress_callback: Callable[..., None] | None = None,
) -> tuple[dict[str, str], dict[str, str]]:
    if timeout_seconds <= 0 or not source_ids:
        return map_downloaded_paths(base_url, source_ids)
    start = time.time()
    done: dict[str, str] = {}
    errors: dict[str, str] = {}
    while time.time() - start < timeout_seconds:
        done, errors = map_downloaded_paths(base_url, source_ids)
        finished = len(done) + len(errors)
        if progress_callback:
            progress_callback(stage="download", label=f"视频号下载 {finished}/{len(source_ids)}", done=finished, total=len(source_ids), success=len(done), fail=len(errors), status="running")
        if finished >= len(source_ids):
            return done, errors
        time.sleep(3)
    return done, errors


def filter_feeds(feeds: list[dict[str, Any]], min_likes: int = 0, top_videos: int | None = None) -> list[dict[str, Any]]:
    # 视频号列表接口通常没有稳定点赞数字段；这里保留参数兼容，实际按评论接口回填后的 countInfo 为准。
    selected = list(feeds)
    if min_likes > 0:
        selected = [item for item in selected if int((((item.get("countInfo") or {}).get("likeCount")) or 0)) >= min_likes]
    if top_videos:
        selected.sort(key=lambda item: int((((item.get("countInfo") or {}).get("likeCount")) or 0)), reverse=True)
        selected = selected[:top_videos]
    return selected


def harvest_wechat_channels(
    username: str,
    base_url: str = DEFAULT_BASE_URL,
    max_pages: int | None = 100,
    top_comments: int = 10,
    download: bool = True,
    wait_download: bool = True,
    download_timeout: int = 1800,
    min_likes: int = 0,
    top_videos: int | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> dict[str, Any]:
    feeds, creator = fetch_all_feeds(username, base_url=base_url, max_pages=max_pages, progress_callback=progress_callback)
    enriched: list[dict[str, Any]] = []
    for index, feed in enumerate(feeds, start=1):
        try:
            comments, count_info = fetch_top_comments(base_url, feed, top_comments)
        except Exception as e:
            comments, count_info = [], {}
            feed["comment_error"] = f"{type(e).__name__}: {e}"
        feed["top_comments"] = comments
        feed["countInfo"] = count_info
        enriched.append(feed)
        if progress_callback:
            progress_callback(stage="comments", label=f"拉取高赞评论 {index}/{len(feeds)}", done=index, total=len(feeds), status="running")
    selected = filter_feeds(enriched, min_likes=min_likes, top_videos=top_videos)
    task_ids: list[str] = []
    media_paths: dict[str, str] = {}
    download_errors: dict[str, str] = {}
    if download and selected:
        task_ids = create_download_tasks(base_url, selected)
        if progress_callback:
            progress_callback(stage="download", label=f"已提交 {len(task_ids)} 个视频号下载任务", done=0, total=len(selected), status="running")
        source_ids = {str(feed.get("id") or "") for feed in selected if feed.get("id")}
        if wait_download:
            media_paths, download_errors = wait_for_downloads(base_url, source_ids, download_timeout, progress_callback=progress_callback)
        else:
            media_paths, download_errors = map_downloaded_paths(base_url, source_ids)
    for feed in selected:
        feed_id = str(feed.get("id") or "")
        if feed_id in media_paths:
            feed["media_path"] = media_paths[feed_id]
        if feed_id in download_errors:
            feed["download_error"] = download_errors[feed_id]
    return {
        "platform": "wechat_channels",
        "target": username,
        "username": username,
        "creator": creator,
        "creator_nickname": creator.get("nickname") or username,
        "creator_username": creator.get("username") or username,
        "fetched_video_count": len(feeds),
        "video_count": len(selected),
        "download_task_ids": task_ids,
        "filters": {"min_likes": min_likes, "top_videos": top_videos},
        "videos": selected,
    }
