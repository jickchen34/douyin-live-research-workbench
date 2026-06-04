import argparse
import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
import httpx
from app_logging import log_event

ROOT = Path(__file__).resolve().parent
EXTERNAL = ROOT / "external" / "Douyin_TikTok_Download_API"
sys.path.insert(0, str(EXTERNAL))

from crawlers.douyin.web import web_crawler as wc_module
from crawlers.douyin.web.endpoints import DouyinAPIEndpoints
from crawlers.douyin.web.utils import BogusManager, TokenManager, VerifyFpManager
from crawlers.douyin.web.web_crawler import DouyinWebCrawler
from crawlers.base_crawler import BaseCrawler

RAW_DIR = ROOT / "data" / "douyin_raw"
MEDIA_DIR = ROOT / "data" / "douyin_media"
LOGGER = logging.getLogger("douyin_live_research.harvest")

SEC_UID_RE = re.compile(r"^MS4w[\w.-]+")
URL_RE = re.compile(r"https?://")


def require_cookie() -> str:
    cookie = os.environ.get("DOUYIN_COOKIE", "").strip()
    if not cookie:
        ms_token = TokenManager.gen_real_msToken()
        ttwid = TokenManager.gen_ttwid()
        verify_fp = VerifyFpManager.gen_verify_fp()
        cookie = f"msToken={ms_token}; ttwid={ttwid}; s_v_web_id={verify_fp}; IsDouyinActive=true;"
    wc_module.config["TokenManager"]["douyin"]["headers"]["Cookie"] = cookie
    return cookie


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def get_aweme_id(item: dict) -> str:
    return safe_text(item.get("aweme_id") or item.get("awemeId") or item.get("id"))


def extract_aweme_list(payload: Any) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    for key in ("aweme_list", "awemeList", "item_list", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            nested = extract_aweme_list(value)
            if nested:
                return nested
    return []


def has_more(payload: dict) -> bool:
    value = payload.get("has_more")
    if value is None:
        value = payload.get("hasMore")
    return bool(value)


def next_cursor(payload: dict) -> int:
    for key in ("max_cursor", "maxCursor", "cursor"):
        value = payload.get(key)
        if value is not None:
            try:
                return int(value)
            except Exception:
                return 0
    return 0


def extract_video_urls(video: dict) -> list[str]:
    urls: list[str] = []
    for br in video.get("bit_rate") or []:
        play = br.get("play_addr") or br.get("playAddr") or {}
        for u in play.get("url_list") or play.get("urlList") or []:
            if isinstance(u, str) and u not in urls:
                urls.append(u)
    play_addr = video.get("play_addr") or video.get("playAddr") or {}
    for u in play_addr.get("url_list") or play_addr.get("urlList") or []:
        if isinstance(u, str) and u not in urls:
            urls.append(u)
    return urls


def normalize_video(item: dict) -> dict:
    stats = item.get("statistics") or item.get("stats") or {}
    author = item.get("author") or {}
    video = item.get("video") or {}
    url_list = extract_video_urls(video)
    return {
        "aweme_id": get_aweme_id(item),
        "desc": safe_text(item.get("desc") or item.get("caption")),
        "create_time": item.get("create_time") or item.get("createTime"),
        "author_nickname": author.get("nickname"),
        "author_sec_user_id": author.get("sec_uid") or author.get("secUid"),
        "digg_count": stats.get("digg_count") or stats.get("diggCount") or stats.get("like_count"),
        "comment_count": stats.get("comment_count") or stats.get("commentCount"),
        "share_count": stats.get("share_count") or stats.get("shareCount"),
        "collect_count": stats.get("collect_count") or stats.get("collectCount"),
        "play_count": stats.get("play_count") or stats.get("playCount"),
        "download_urls": url_list,
        "raw": item,
    }


def extract_comments(payload: Any) -> list[dict]:
    if not isinstance(payload, dict):
        return []
    for key in ("comments", "comment_list", "commentList", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            nested = extract_comments(value)
            if nested:
                return nested
    return []


def normalize_comment(item: dict) -> dict:
    user = item.get("user") or {}
    return {
        "cid": safe_text(item.get("cid") or item.get("comment_id") or item.get("id")),
        "text": safe_text(item.get("text") or item.get("content")),
        "digg_count": item.get("digg_count") or item.get("diggCount") or item.get("like_count") or 0,
        "create_time": item.get("create_time") or item.get("createTime"),
        "user_nickname": user.get("nickname"),
        "raw": item,
    }


def extract_profile_user(payload: Any) -> dict:
    if not isinstance(payload, dict):
        return {}
    for key in ("user", "user_info", "userInfo"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    for value in payload.values():
        if isinstance(value, dict):
            nested = extract_profile_user(value)
            if nested:
                return nested
    return {}


def normalize_creator(user: dict, sec_user_id: str) -> dict:
    return {
        "nickname": user.get("nickname"),
        "unique_id": user.get("unique_id") or user.get("uniqueId"),
        "sec_user_id": user.get("sec_uid") or user.get("secUid") or sec_user_id,
        "signature": user.get("signature"),
        "follower_count": user.get("follower_count") or user.get("followerCount"),
        "following_count": user.get("following_count") or user.get("followingCount"),
        "total_favorited": user.get("total_favorited") or user.get("totalFavorited"),
    }


def creator_from_videos(videos: list[dict], sec_user_id: str) -> dict:
    for video in videos:
        raw_author = ((video.get("raw") or {}).get("author") or {})
        if raw_author:
            return normalize_creator(raw_author, sec_user_id)
        if video.get("author_nickname"):
            return {
                "nickname": video.get("author_nickname"),
                "unique_id": None,
                "sec_user_id": video.get("author_sec_user_id") or sec_user_id,
                "signature": None,
                "follower_count": None,
                "following_count": None,
                "total_favorited": None,
            }
    return normalize_creator({}, sec_user_id)


async def fetch_creator_profile(crawler: DouyinWebCrawler, sec_user_id: str) -> dict:
    payload = await crawler.handler_user_profile(sec_user_id)
    return normalize_creator(extract_profile_user(payload), sec_user_id)


async def resolve_sec_user_id(crawler: DouyinWebCrawler, target: str) -> tuple[str, dict | None]:
    target = target.strip()
    if SEC_UID_RE.match(target):
        return target, None
    if URL_RE.search(target):
        return await crawler.get_sec_user_id(target), None
    candidates = await search_user_candidates(target)
    if not candidates:
        raise RuntimeError(f"无法仅凭博主名解析 sec_user_id：{target}。请提供主页 URL 或 sec_user_id，或提供可用 DOUYIN_COOKIE 后重试用户搜索。")
    return candidates[0]["sec_user_id"], {"candidates": candidates}


async def search_user_candidates(keyword: str, limit: int = 10) -> list[dict]:
    headers = await DouyinWebCrawler().get_douyin_headers()
    params = {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "search_channel": "aweme_user_web",
        "keyword": keyword,
        "offset": 0,
        "count": limit,
        "search_source": "normal_search",
        "query_correct_type": "1",
        "is_filter_search": "0",
        "from_group_id": "",
        "pc_client_type": "1",
        "version_code": "290100",
        "version_name": "29.1.0",
        "cookie_enabled": "true",
        "screen_width": "1920",
        "screen_height": "1080",
        "browser_language": "zh-CN",
        "browser_platform": "Win32",
        "browser_name": "Chrome",
        "browser_version": "130.0.0.0",
        "browser_online": "true",
        "engine_name": "Blink",
        "engine_version": "130.0.0.0",
        "os_name": "Windows",
        "os_version": "10",
        "cpu_core_num": "12",
        "device_memory": "8",
        "platform": "PC",
        "downlink": "10",
        "effective_type": "4g",
        "round_trip_time": "0",
        "webid": "",
        "msToken": "",
    }
    a_bogus = BogusManager.ab_model_2_endpoint(params, headers["headers"]["User-Agent"])
    url = f"{DouyinAPIEndpoints.USER_SEARCH}?{urlencode(params)}&a_bogus={a_bogus}"
    async with BaseCrawler(proxies=headers.get("proxies"), crawler_headers=headers["headers"]) as client:
        data = await client.fetch_get_json(url)
    candidates = []
    items = []
    if isinstance(data, dict):
        raw_data = data.get("data")
        if isinstance(raw_data, list):
            items = raw_data
        elif isinstance(raw_data, dict):
            for value in raw_data.values():
                if isinstance(value, list):
                    items = value
                    break
    for row in items:
        user = row.get("user_info") or row.get("user") or row
        sec_uid = user.get("sec_uid") or user.get("secUid")
        if sec_uid:
            candidates.append({
                "nickname": user.get("nickname"),
                "unique_id": user.get("unique_id") or user.get("uniqueId"),
                "sec_user_id": sec_uid,
                "follower_count": user.get("follower_count") or user.get("followerCount"),
            })
    return candidates


async def fetch_all_posts(crawler: DouyinWebCrawler, sec_user_id: str, page_size: int, max_pages: int | None) -> list[dict]:
    cursor = 0
    page = 0
    videos = []
    seen = set()
    while True:
        page += 1
        payload = await crawler.fetch_user_post_videos(sec_user_id, cursor, page_size)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        (RAW_DIR / f"posts_page_{page}.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        items = extract_aweme_list(payload)
        for item in items:
            aweme_id = get_aweme_id(item)
            if aweme_id and aweme_id not in seen:
                seen.add(aweme_id)
                videos.append(normalize_video(item))
        if not has_more(payload):
            break
        cursor = next_cursor(payload)
        if not cursor:
            break
        if max_pages and page >= max_pages:
            break
    return videos


async def download_video_file(headers: dict, video: dict) -> str | None:
    urls = video.get("download_urls") or []
    if not urls:
        return None
    aweme_id = video.get("aweme_id") or "unknown"
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    out = MEDIA_DIR / f"{aweme_id}.mp4"
    if out.exists() and out.stat().st_size > 0:
        return str(out)
    async with httpx.AsyncClient(headers=headers["headers"], proxies=headers.get("proxies"), timeout=60, follow_redirects=True) as client:
        last_error = None
        for url in urls:
            try:
                response = await client.get(url)
                if response.status_code < 400 and response.content:
                    out.write_bytes(response.content)
                    return str(out)
                last_error = f"HTTP {response.status_code}"
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
        raise RuntimeError(last_error or "无可用下载地址")


async def fetch_top_comments(crawler: DouyinWebCrawler, aweme_id: str, top_n: int) -> list[dict]:
    payload = await crawler.fetch_video_comments(aweme_id, 0, max(20, top_n * 3))
    comments = [normalize_comment(c) for c in extract_comments(payload)]
    comments.sort(key=lambda x: int(x.get("digg_count") or 0), reverse=True)
    return comments[:top_n]


async def harvest_target(
    target: str,
    page_size: int = 20,
    max_pages: int | None = None,
    top_comments: int = 10,
    download: bool = False,
    min_likes: int = 0,
    top_videos: int | None = None,
) -> dict:
    require_cookie()
    crawler = DouyinWebCrawler()
    sec_user_id, resolution = await resolve_sec_user_id(crawler, target)
    log_event(LOGGER, "harvest.resolve", target=target, sec_user_id=sec_user_id, used_search=bool(resolution))
    profile_error = None
    try:
        creator = await fetch_creator_profile(crawler, sec_user_id)
    except Exception as e:
        creator = normalize_creator({}, sec_user_id)
        profile_error = f"{type(e).__name__}: {e}"
        log_event(LOGGER, "harvest.profile_failure", sec_user_id=sec_user_id, error=profile_error)
    all_videos = await fetch_all_posts(crawler, sec_user_id, page_size, max_pages)
    videos = [video for video in all_videos if safe_int(video.get("digg_count")) >= min_likes]
    videos.sort(key=lambda video: safe_int(video.get("digg_count")), reverse=True)
    if top_videos:
        videos = videos[:top_videos]
    log_event(LOGGER, "harvest.filter", sec_user_id=sec_user_id, fetched=len(all_videos), selected=len(videos), min_likes=min_likes, top_videos=top_videos)
    if not creator.get("nickname"):
        creator = creator_from_videos(videos or all_videos, sec_user_id)

    headers = await crawler.get_douyin_headers()
    for video in videos:
        aweme_id = video["aweme_id"]
        if download:
            try:
                video["media_path"] = await download_video_file(headers, video)
            except Exception as e:
                video["download_error"] = f"{type(e).__name__}: {e}"
                log_event(LOGGER, "harvest.video_download_failure", aweme_id=aweme_id, error=video["download_error"])
        if not aweme_id:
            video["top_comments"] = []
            video["comment_error"] = "missing aweme_id"
            log_event(LOGGER, "harvest.comment_failure", aweme_id=aweme_id, error=video["comment_error"])
            continue
        try:
            video["top_comments"] = await fetch_top_comments(crawler, aweme_id, top_comments)
        except Exception as e:
            video["top_comments"] = []
            video["comment_error"] = f"{type(e).__name__}: {e}"
            log_event(LOGGER, "harvest.comment_failure", aweme_id=aweme_id, error=video["comment_error"])

    result = {
        "target": target,
        "sec_user_id": sec_user_id,
        "resolution": resolution,
        "creator": creator,
        "creator_nickname": creator.get("nickname"),
        "creator_unique_id": creator.get("unique_id"),
        "creator_sec_user_id": creator.get("sec_user_id"),
        "profile_error": profile_error,
        "video_count": len(videos),
        "fetched_video_count": len(all_videos),
        "filters": {
            "min_likes": min_likes,
            "top_videos": top_videos,
        },
        "videos": videos,
    }
    out = RAW_DIR / f"creator_{sec_user_id}_harvest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    result["output"] = str(out)
    return result


async def main() -> None:
    parser = argparse.ArgumentParser(description="纯程序化采集抖音博主作品和高赞评论")
    parser.add_argument("target", help="博主名、主页 URL 或 sec_user_id")
    parser.add_argument("--page-size", type=int, default=20)
    parser.add_argument("--max-pages", type=int, default=0, help="0 表示直到接口无更多数据")
    parser.add_argument("--top-comments", type=int, default=10)
    parser.add_argument("--min-likes", type=int, default=0)
    parser.add_argument("--top-videos", type=int, default=0, help="0 表示不过滤 Top N")
    parser.add_argument("--download", action="store_true", help="下载视频文件；默认只保存下载 URL 和数据")
    args = parser.parse_args()

    result = await harvest_target(
        target=args.target,
        page_size=args.page_size,
        max_pages=args.max_pages or None,
        top_comments=args.top_comments,
        download=args.download,
        min_likes=max(0, args.min_likes),
        top_videos=args.top_videos or None,
    )
    print(json.dumps({"output": result["output"], "video_count": result["video_count"], "sec_user_id": result["sec_user_id"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
