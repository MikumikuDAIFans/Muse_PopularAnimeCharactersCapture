"""输出角色 post_count/recent_count 阈值分析报告。"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from sqlalchemy import func, select
from database import close_db, get_session_factory, init_db
from models import Post, PostTag, Tag


def percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    idx = min(len(sorted_values) - 1, max(0, int(round((len(sorted_values) - 1) * p))))
    return sorted_values[idx]


async def run(args) -> int:
    await init_db()
    cutoff = datetime.utcnow() - timedelta(days=args.recent_months * 30)
    factory = await get_session_factory()
    async with factory() as session:
        tag_rows = await session.execute(select(Tag).where(Tag.category == "character"))
        tags = list(tag_rows.scalars().all())
        totals = [int(tag.post_count or 0) for tag in tags]
        recent_counts: list[int] = []
        for tag in tags[: args.max_tags]:
            result = await session.execute(
                select(func.count(PostTag.post_id))
                .join(Post, Post.id == PostTag.post_id)
                .where(PostTag.tag_id == tag.id, Post.created_at >= cutoff)
            )
            recent_counts.append(int(result.scalar() or 0))

    await close_db()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 热门阈值分析",
        "",
        f"- generated_at: {datetime.utcnow().isoformat()}",
        f"- recent_months: {args.recent_months}",
        f"- character_tags: {len(tags)}",
        "",
        "## total_post_count 分布",
        f"- p50: {percentile(totals, 0.50)}",
        f"- p75: {percentile(totals, 0.75)}",
        f"- p90: {percentile(totals, 0.90)}",
        f"- p95: {percentile(totals, 0.95)}",
        "",
        "## recent_post_count 样本分布",
        f"- sampled_tags: {len(recent_counts)}",
        f"- p50: {percentile(recent_counts, 0.50)}",
        f"- p75: {percentile(recent_counts, 0.75)}",
        f"- p90: {percentile(recent_counts, 0.90)}",
        "",
        "## 当前建议",
        "- 第一版采用综合评分：0.7 * normalized_total_post_count + 0.3 * normalized_recent_post_count。",
        "- 候选池最低 total_post_count 默认 50，可按本报告分布调整。",
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK threshold report: {output}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--max-tags", type=int, default=10000)
    parser.add_argument("--output", default="docs/热门阈值分析.md")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
