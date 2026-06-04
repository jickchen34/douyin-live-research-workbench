import argparse
import json
from pathlib import Path

from db import connect, export_library, init_db
from pipeline import analyze_existing, run_pipeline

ROOT = Path(__file__).resolve().parent


def cmd_init(args: argparse.Namespace) -> None:
    conn = connect()
    init_db(conn)
    print("数据库已初始化")


def cmd_run(args: argparse.Namespace) -> None:
    result = run_pipeline(
        url=args.url,
        creator=args.creator,
        category=args.category,
        comment_limit=args.comment_limit,
        max_seconds=args.max_seconds,
        whisper_model=args.whisper_model,
        whisper_language=args.whisper_language,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    cmd_export(args)



def cmd_analyze(args: argparse.Namespace) -> None:
    result = analyze_existing(args.video_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    cmd_export(args)


def cmd_export(args: argparse.Namespace) -> None:
    conn = connect()
    init_db(conn)
    output = ROOT / "web" / "library.json"
    data = export_library(conn, output)
    print(f"已导出 {len(data)} 条记录到 {output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="抖音直播素材研究台 MVP")
    sub = parser.add_subparsers(required=True)

    p_init = sub.add_parser("init", help="初始化数据库")
    p_init.set_defaults(func=cmd_init)

    p_run = sub.add_parser("run", help="下载、转写并分析单条视频")
    p_run.add_argument("--url", required=True, help="公开视频链接")
    p_run.add_argument("--creator", default="未命名博主", help="博主名")
    p_run.add_argument("--category", default="未分类", help="博主分类")
    p_run.add_argument("--comment-limit", type=int, default=20, help="预留：每条视频抓取的高赞评论数量")
    p_run.add_argument("--max-seconds", type=int, default=90, help="最小测试时只转写前 N 秒，传 0 表示全量")
    p_run.add_argument("--whisper-model", default="tiny", help="whisper 模型名，例如 tiny/base/small")
    p_run.add_argument("--whisper-language", default="Chinese", help="转写语言，中文视频默认 Chinese，英文测试可传 English")
    p_run.set_defaults(func=cmd_run)

    p_analyze = sub.add_parser("analyze", help="对已转写视频重跑 AI 分析")
    p_analyze.add_argument("--video-id", type=int, required=True, help="数据库中的视频 ID")
    p_analyze.set_defaults(func=cmd_analyze)

    p_export = sub.add_parser("export", help="导出前端 JSON")
    p_export.set_defaults(func=cmd_export)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "max_seconds", None) == 0:
        args.max_seconds = None
    args.func(args)


if __name__ == "__main__":
    main()
