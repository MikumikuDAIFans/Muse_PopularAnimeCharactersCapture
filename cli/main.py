"""Muse DataLoad CLI 入口

用法:
  python -m cli.main task create --name "帖子爬取" --start-id 10576339 --end-id 10908849
  python -m cli.main task list
  python -m cli.main task start 1
  python -m cli.main task stop 1
  python -m cli.main posts list --tag "character:frieren" --limit 20
  python -m cli.main characters top --n 200 --format json
  python -m cli.main stats
"""

import argparse
import json
import os
import sys
from datetime import datetime

try:
    import httpx
except ImportError:
    httpx = None


ROOT_URL = os.environ.get("MUSE_ROOT_URL", "http://localhost:8000").rstrip("/")
BASE_URL = os.environ.get("MUSE_API_BASE_URL", f"{ROOT_URL}/api").rstrip("/")


def api_get(path: str, params: dict = None):
    if not httpx:
        print("错误: 请安装 httpx (pip install httpx)")
        sys.exit(1)
    try:
        resp = httpx.get(f"{BASE_URL}{path}", params=params or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.ConnectError:
        print(f"错误: 无法连接到服务 {BASE_URL}")
        print("提示: 确保后端服务已启动 (python -m backend.main)")
        sys.exit(1)
    except Exception as exc:
        print(f"API错误: {exc}")
        sys.exit(1)


def api_post(path: str, json_data: dict = None):
    if not httpx:
        print("错误: 请安装 httpx (pip install httpx)")
        sys.exit(1)
    try:
        resp = httpx.post(f"{BASE_URL}{path}", json=json_data or {}, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.ConnectError:
        print(f"错误: 无法连接到服务 {BASE_URL}")
        sys.exit(1)
    except Exception as exc:
        print(f"API错误: {exc}")
        sys.exit(1)


def api_delete(path: str):
    if not httpx:
        print("错误: 请安装 httpx (pip install httpx)")
        sys.exit(1)
    try:
        resp = httpx.delete(f"{BASE_URL}{path}", timeout=30)
        resp.raise_for_status()
        return resp.json()
    except httpx.ConnectError:
        print(f"错误: 无法连接到服务 {BASE_URL}")
        sys.exit(1)
    except Exception as exc:
        print(f"API错误: {exc}")
        sys.exit(1)


# ========== 命令: task ==========

def cmd_task_list(args):
    params = {}
    if args.status:
        params["status"] = args.status
    if args.project_id:
        params["project_id"] = args.project_id
    data = api_get("/tasks", params)
    if not data:
        print("暂无任务")
        return

    print(f"\n{'ID':>4}  {'名称':<30}  {'类型':<12}  {'状态':<10}  {'进度':>10}")
    print("-" * 80)
    for t in data:
        prog = f"{t['processed_count']}/{t['total_count']}"
        print(f"{t['id']:>4}  {t['name']:<30}  {t['task_type']:<12}  {t['status']:<10}  {prog:>10}")


def cmd_task_create(args):
    params = {}
    if args.start_id:
        params["start_id"] = args.start_id
    if args.end_id:
        params["end_id"] = args.end_id
    if args.tags:
        params["tags"] = args.tags.split(",")
    if args.tag_filter:
        params["tag_filter"] = args.tag_filter
    if args.danbooru_ids:
        params["danbooru_ids"] = [int(i.strip()) for i in args.danbooru_ids.split(",") if i.strip()]
    if args.limit:
        params["limit"] = args.limit
    if args.min_count:
        params["min_post_count"] = args.min_count
    if args.recent_months:
        params["recent_months"] = args.recent_months
    if args.top_n:
        params["top_n"] = args.top_n

    data = api_post("/tasks", {
        "name": args.name,
        "task_type": args.task_type,
        "params": params,
    })
    print(f"任务已创建: ID={data['id']} - {data['name']}")


def cmd_task_start(args):
    data = api_post(f"/tasks/{args.task_id}/start")
    print(f"任务 {args.task_id} 已启动 (状态: {data['status']})")


def cmd_task_stop(args):
    data = api_post(f"/tasks/{args.task_id}/stop")
    print(f"任务 {args.task_id} 已停止 (状态: {data['status']})")


def cmd_task_delete(args):
    result = api_delete(f"/tasks/{args.task_id}")
    print(result.get("message", "任务已删除"))


def cmd_task_logs(args):
    params = {}
    if args.level:
        params["level"] = args.level
    if args.limit:
        params["limit"] = args.limit

    data = api_get(f"/tasks/{args.task_id}/logs", params)
    if not data:
        print("暂无日志")
        return

    for log in data:
        ts = log["created_at"][:19] if log["created_at"] else "-"
        print(f"[{ts}] [{log['level']:5}] {log['message']}")


# ========== 命令: posts ==========

def cmd_posts_list(args):
    params = {"page_size": args.limit or 20}
    if args.tag:
        params["tag"] = args.tag
    if args.task_id:
        params["task_id"] = args.task_id

    data = api_get("/posts", params)
    print(f"\n共 {data['total']} 条帖子，第 {data['page']} 页:")
    print(f"{'ID':>12}  {'MD5':<32}  {'标签数':>6}  {'评分':>6}  {'格式':>6}")
    print("-" * 75)
    for p in data["items"]:
        print(f"{p['id']:>12}  {p['md5'] or '-':<32}  {p['tag_count']:>6}  {p['score']:>6}  {p['file_ext'] or '-':>6}")


def cmd_posts_stats(args):
    data = api_get("/posts/stats")
    print(f"\n帖子总数: {data['total_posts']}")
    print(f"总大小: {data['total_bytes']:,} bytes")
    print(f"平均评分: {data['avg_score']}")


# ========== 命令: characters ==========

def cmd_characters_list(args):
    params = {"page_size": 50}
    if args.min_count:
        params["min_count"] = args.min_count
    data = api_get("/characters", params)
    print(f"\n共 {data['total']} 个角色:")
    print(f"{'角色标签':<40}  {'帖子数':>8}  {'热度分':>10}")
    print("-" * 65)
    for c in data["items"]:
        print(f"{c['character_tag']:<40}  {c['total_post_count']:>8}  {c['popularity_score']:>10.4f}")


def cmd_characters_top(args):
    params = {
        "n": args.n,
        "recent_months": args.recent_months,
        "min_count": args.min_count,
    }
    data = api_get("/characters/top", params)
    chars = data["characters"]

    print(f"\nTop {len(chars)} 角色榜单:")
    print(f"{'#':>3}  {'角色标签':<40}  {'作品':<30}  {'帖子数':>6}")
    print("-" * 90)
    for i, c in enumerate(chars, 1):
        copyrights = ", ".join(c["copyrights"][:2]) if c["copyrights"] else "-"
        print(f"{i:>3}  {c['character_tag']:<40}  {copyrights:<30}  {c['post_count']:>6}")

    # 输出到文件
    if args.output:
        output_file = args.output
        if args.output.endswith(".json"):
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        elif args.output.endswith(".csv"):
            import csv
            with open(output_file, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["character_tag", "copyrights", "post_count"])
                writer.writeheader()
                for c in chars:
                    row = dict(c)
                    row["copyrights"] = "|".join(c["copyrights"])
                    writer.writerow(row)
        print(f"\n已保存到: {args.output}")


def cmd_characters_build(args):
    params = {
        "n": args.n,
        "recent_months": args.recent_months,
        "min_count": args.min_count,
    }
    data = api_post("/characters/build?" + "&".join(f"{k}={v}" for k, v in params.items()))
    print(f"已生成角色榜单: {data['total_count']} 条")
    print(f"过滤条件: {data.get('filters')}")


def cmd_characters_emerging(args):
    params = {
        "n": args.n,
        "min_count": args.min_count,
        "min_recent_count": args.min_recent_count,
        "max_age_days": args.max_age_days,
    }
    data = api_get("/characters/emerging", params)
    print(f"已生成新兴角色榜: {data['total_count']} 条")
    print(f"过滤条件: {data.get('filters')}")
    if data["characters"]:
        print(f"榜首角色: {data['characters'][0]['character_tag']}")


# ========== 命令: stats ==========

def cmd_stats(args):
    data = api_get("/stats")
    print(f"\n系统统计:")
    print(f"  已下载帖子: {data['total_posts']:,}")
    print(f"  总任务数: {data['total_tasks']}")
    print(f"  运行中: {data['running_tasks']}")
    print(f"  已完成: {data['completed_tasks']}")
    print(f"  失败: {data['failed_tasks']}")
    print(f"  待处理: {data['pending_tasks']}")
    print(f"  已下载数据: {data['total_download_bytes']:,} bytes")


def cmd_health(args):
    if not httpx:
        print("错误: 请安装 httpx (pip install httpx)")
        sys.exit(1)
    try:
        data = httpx.get(f"{ROOT_URL}/health", timeout=10).json()
    except httpx.ConnectError:
        print(f"错误: 无法连接到服务 {ROOT_URL}")
        sys.exit(1)
    print(f"状态: {data['status']}")
    print(f"版本: {data['version']}")
    print(f"运行时间: {data['uptime']}s")


def cmd_dataset_export(args):
    data = api_post("/datasets/export", {
        "character_tag": args.character_tag,
        "limit": args.limit,
        "min_score": args.min_score,
        "rating": args.rating,
        "include_artist": not args.no_artist,
        "download_images": not args.no_download,
    })
    print(f"已导出 {data['exported_count']} 条样本到: {data['dataset_dir']}")
    if data.get("errors"):
        print(f"错误数: {len(data['errors'])}")


# ========== 主解析器 ==========

def build_parser():
    parser = argparse.ArgumentParser(
        prog="muse",
        description="Muse DataLoad CLI — Danbooru 训练数据下载服务",
    )
    sub = parser.add_subparsers(dest="command")

    # task 子命令
    task = sub.add_parser("task", help="任务管理")
    task_sub = task.add_subparsers(dest="task_cmd")

    t_list = task_sub.add_parser("list", help="列出任务")
    t_list.add_argument("--status")
    t_list.add_argument("--project-id", type=int)
    t_list.set_defaults(func=cmd_task_list)

    t_create = task_sub.add_parser("create", help="创建任务")
    t_create.add_argument("--name", required=True)
    t_create.add_argument("--task-type", default="posts", choices=["posts", "tags", "characters"])
    t_create.add_argument("--start-id", type=int)
    t_create.add_argument("--end-id", type=int)
    t_create.add_argument("--tags")
    t_create.add_argument("--tag-filter")
    t_create.add_argument("--danbooru-ids", help="逗号分隔的帖子ID列表")
    t_create.add_argument("--limit", type=int)
    t_create.add_argument("--min-count", type=int)
    t_create.add_argument("--recent-months", type=int)
    t_create.add_argument("--top-n", type=int)
    t_create.set_defaults(func=cmd_task_create)

    t_start = task_sub.add_parser("start", help="启动任务")
    t_start.add_argument("task_id", type=int)
    t_start.set_defaults(func=cmd_task_start)

    t_stop = task_sub.add_parser("stop", help="停止任务")
    t_stop.add_argument("task_id", type=int)
    t_stop.set_defaults(func=cmd_task_stop)

    t_del = task_sub.add_parser("delete", help="删除任务")
    t_del.add_argument("task_id", type=int)
    t_del.set_defaults(func=cmd_task_delete)

    t_logs = task_sub.add_parser("logs", help="查看任务日志")
    t_logs.add_argument("task_id", type=int)
    t_logs.add_argument("--level")
    t_logs.add_argument("--limit", type=int, default=100)
    t_logs.set_defaults(func=cmd_task_logs)

    # posts 子命令
    posts = sub.add_parser("posts", help="帖子查询")
    posts_sub = posts.add_subparsers(dest="posts_cmd")

    p_list = posts_sub.add_parser("list", help="列出帖子")
    p_list.add_argument("--tag")
    p_list.add_argument("--task-id", type=int)
    p_list.add_argument("--limit", type=int, default=20)
    p_list.set_defaults(func=cmd_posts_list)

    p_stats = posts_sub.add_parser("stats", help="帖子统计")
    p_stats.set_defaults(func=cmd_posts_stats)

    # characters 子命令
    chars = sub.add_parser("characters", help="角色分析")
    chars_sub = chars.add_subparsers(dest="chars_cmd")

    c_list = chars_sub.add_parser("list", help="列出角色")
    c_list.add_argument("--min-count", type=int)
    c_list.set_defaults(func=cmd_characters_list)

    c_top = chars_sub.add_parser("top", help="Top N 角色榜单")
    c_top.add_argument("--n", type=int, default=200)
    c_top.add_argument("--recent-months", type=int, default=6)
    c_top.add_argument("--min-count", type=int, default=50)
    c_top.add_argument("--output", help="导出文件路径")
    c_top.set_defaults(func=cmd_characters_top)

    c_build = chars_sub.add_parser("build", help="生成并落盘正式 Top N 角色榜单")
    c_build.add_argument("--n", type=int, default=200)
    c_build.add_argument("--recent-months", type=int, default=6)
    c_build.add_argument("--min-count", type=int, default=50)
    c_build.set_defaults(func=cmd_characters_build)

    c_emerging = chars_sub.add_parser("emerging", help="生成新兴热门角色榜")
    c_emerging.add_argument("--n", type=int, default=200)
    c_emerging.add_argument("--min-count", type=int, default=50)
    c_emerging.add_argument("--min-recent-count", type=int, default=10)
    c_emerging.add_argument("--max-age-days", type=int, default=1095)
    c_emerging.set_defaults(func=cmd_characters_emerging)

    # stats 子命令
    sub.add_parser("stats", help="系统统计").set_defaults(func=cmd_stats)

    # dataset 子命令
    dataset = sub.add_parser("dataset", help="训练数据集导出")
    dataset_sub = dataset.add_subparsers(dest="dataset_cmd")
    d_export = dataset_sub.add_parser("export", help="按角色导出训练样本")
    d_export.add_argument("character_tag")
    d_export.add_argument("--limit", type=int, default=50)
    d_export.add_argument("--min-score", type=int)
    d_export.add_argument("--rating", choices=["g", "s", "q", "e"])
    d_export.add_argument("--no-artist", action="store_true")
    d_export.add_argument("--no-download", action="store_true")
    d_export.set_defaults(func=cmd_dataset_export)

    # health 子命令
    sub.add_parser("health", help="健康检查").set_defaults(func=cmd_health)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    args.func(args)


if __name__ == "__main__":
    main()
