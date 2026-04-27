"""从 Danbooru JSONL 元数据流式生成角色候选榜。

该脚本不依赖数据库，适合近两年全站元数据先落 JSONL 后做离线抽检聚合。
输出用于决定后续定向样本下载的候选角色；正式交付榜单仍可使用
scripts/build_character_list.py 基于数据库生成。
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from services.rules import load_rules


def split_tags(value: str | None) -> list[str]:
    return [tag.strip() for tag in (value or "").split() if tag.strip()]


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def iter_input_files(args: argparse.Namespace) -> list[Path]:
    if args.inputs:
        files = [Path(item) for item in args.inputs]
    else:
        input_root = Path(args.input_root or Path(get_settings().OUTPUT_ROOT) / "metadata")
        files = sorted(input_root.glob(args.pattern))
    return [path for path in files if path.exists() and path.is_file()]


def build_candidates(args: argparse.Namespace) -> dict[str, Any]:
    files = iter_input_files(args)
    if not files:
        raise FileNotFoundError("No JSONL input files found")

    rules = load_rules()
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=args.recent_months * 30) if args.recent_months else None
    character_counts: Counter[str] = Counter()
    copyright_by_character: dict[str, Counter[str]] = defaultdict(Counter)
    scanned = 0
    invalid = 0
    skipped_old = 0

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    post = json.loads(line)
                except json.JSONDecodeError:
                    invalid += 1
                    continue

                created_at = parse_datetime(post.get("created_at"))
                if cutoff and created_at and created_at < cutoff:
                    skipped_old += 1
                    continue

                characters = split_tags(post.get("tag_string_character"))
                copyrights = split_tags(post.get("tag_string_copyright"))
                if not characters:
                    scanned += 1
                    continue

                seen_characters = set()
                for raw_character in characters:
                    canonical = rules.canonical_character(raw_character, {})
                    include, _, _ = rules.character_decision(canonical)
                    if not include or canonical in seen_characters:
                        continue
                    seen_characters.add(canonical)
                    character_counts[canonical] += 1
                    for copyright in dict.fromkeys(copyrights):
                        copyright_by_character[canonical][copyright] += 1
                scanned += 1

    max_recent = max(character_counts.values(), default=1)
    rows: list[dict[str, Any]] = []
    for character, recent_count in character_counts.items():
        if recent_count < args.min_count:
            continue
        score = math.log1p(recent_count) / math.log1p(max_recent)
        rows.append(
            {
                "character_tag": character,
                "copyrights": [name for name, _ in copyright_by_character[character].most_common(args.copyright_limit)],
                "recent_post_count": recent_count,
                "candidate_score": round(score, 6),
            }
        )

    rows.sort(key=lambda item: (item["candidate_score"], item["recent_post_count"], item["character_tag"]), reverse=True)
    rows = rows[: args.top_n]
    for index, row in enumerate(rows, 1):
        row["rank"] = index

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "filters": {
            "recent_months": args.recent_months,
            "top_n": args.top_n,
            "min_count": args.min_count,
            "copyright_limit": args.copyright_limit,
        },
        "source_files": [str(path) for path in files],
        "stats": {
            "scanned_posts": scanned,
            "invalid_lines": invalid,
            "skipped_old_posts": skipped_old,
            "unique_characters": len(character_counts),
            "output_rows": len(rows),
        },
        "characters": rows,
    }


def write_outputs(payload: dict[str, Any], output_root: Path, recent_months: int, top_n: int) -> tuple[Path, Path]:
    export_dir = output_root / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    json_path = export_dir / f"character_candidates_recent_{recent_months}m_top_{top_n}.json"
    csv_path = export_dir / f"character_candidates_recent_{recent_months}m_top_{top_n}.csv"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["rank", "character_tag", "copyrights", "recent_post_count", "candidate_score"],
        )
        writer.writeheader()
        for row in payload["characters"]:
            csv_row = dict(row)
            csv_row["copyrights"] = "|".join(row.get("copyrights") or [])
            writer.writerow(csv_row)
    return json_path, csv_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("inputs", nargs="*", help="Optional explicit JSONL files")
    parser.add_argument("--input-root", help="Directory to scan when inputs are omitted")
    parser.add_argument("--pattern", default="task_*_posts.jsonl")
    parser.add_argument("--recent-months", type=int, default=24)
    parser.add_argument("--top-n", type=int, default=500)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--copyright-limit", type=int, default=5)
    parser.add_argument("--output-root")
    args = parser.parse_args()

    payload = build_candidates(args)
    output_root = Path(args.output_root or get_settings().OUTPUT_ROOT)
    json_path, csv_path = write_outputs(payload, output_root, args.recent_months, args.top_n)
    print(f"OK character candidates rows={payload['stats']['output_rows']}")
    print(json_path)
    print(csv_path)
    print(f"stats={payload['stats']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
