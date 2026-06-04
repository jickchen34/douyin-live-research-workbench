import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path

DEFAULT_BASE_URL = "https://ark.cn-beijing.volces.com/api/v3/chat/completions"


def load_local_env() -> None:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key and key not in os.environ:
            os.environ[key] = value


def analyze_with_doubao(title: str, description: str, transcript: str, comments: list[str] | None = None) -> str:
    load_local_env()
    api_key = os.environ.get("VOLCENGINE_API_KEY")
    endpoint_id = os.environ.get("VOLCENGINE_ENDPOINT_ID")
    base_url = os.environ.get("VOLCENGINE_BASE_URL", DEFAULT_BASE_URL)
    if not api_key:
        raise RuntimeError("缺少环境变量 VOLCENGINE_API_KEY")
    if not endpoint_id:
        raise RuntimeError("缺少环境变量 VOLCENGINE_ENDPOINT_ID")

    comments_text = "\n".join(f"- {c}" for c in (comments or [])) or "暂无评论数据"
    user_prompt = f"""
你是一个短视频内容策划和直播运营分析师。你只能基于下面提供的标题、简介、转写文本和评论做分析，严禁引入原文没有出现的主题、人物、研究、数据或新闻。如果简介和标题/转写文本明显冲突，必须以转写文本为准，并在 risk_notes 中标记“元数据可能污染”，不要把冲突简介扩展成直播话题。

标题：{title or '无'}
简介：{description or '无'}
转写文本：
{transcript[:12000]}

高赞评论：
{comments_text[:4000]}

如果素材信息很少，也必须如实说明“信息有限”，不要编造。请用 JSON 输出，字段包括：
- topic_summary：一句话主题
- hook：开头钩子判断
- content_structure：视频结构拆解
- viral_factors：爆款因素数组
- audience_focus：观众关注点数组
- live_talking_points：直播可聊话题数组，每个话题包含 title 和 talking_notes
- usable_quotes：可复用表达数组
- evidence_quotes：来自标题、简介或转写文本的证据短句数组
- risk_notes：事实、版权、表达风险提醒数组
""".strip()

    payload = {
        "model": endpoint_id,
        "messages": [
            {"role": "system", "content": "你只输出有效 JSON，不要输出 Markdown。"},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.3,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        base_url,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    context = None
    try:
        import certifi

        context = ssl.create_default_context(cafile=certifi.where())
    except Exception:
        context = ssl.create_default_context()

    try:
        with urllib.request.urlopen(req, timeout=120, context=context) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"火山方舟调用失败：HTTP {e.code} {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"火山方舟网络失败：{e}") from e

    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as e:
        raise RuntimeError(f"火山方舟返回格式异常：{json.dumps(data, ensure_ascii=False)[:1000]}") from e


def parse_json_object(text: str) -> dict | None:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.startswith("json"):
            stripped = stripped[4:].strip()
    start = stripped.find("{")
    end = stripped.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return None
    try:
        return json.loads(stripped[start : end + 1])
    except json.JSONDecodeError:
        return None
