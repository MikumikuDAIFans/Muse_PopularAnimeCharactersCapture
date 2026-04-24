"""校验新兴角色榜 JSON/CSV。"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


REQUIRED = {
    "rank",
    "character_tag",
    "post_count",
    "recent_post_count",
    "first_seen_at",
    "character_age_days",
    "recent_ratio",
    "growth_score",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_path", type=Path)
    parser.add_argument("csv_path", type=Path)
    parser.add_argument("--max-age-days", type=int, default=1095)
    args = parser.parse_args()

    payload = json.loads(args.json_path.read_text(encoding="utf-8"))
    items = payload.get("characters", [])
    rows = list(csv.DictReader(open(args.csv_path, encoding="utf-8")))
    if len(items) != len(rows):
        raise SystemExit("json/csv count mismatch")
    for idx, item in enumerate(items, 1):
        missing = REQUIRED - set(item)
        if missing:
            raise SystemExit(f"missing fields row {idx}: {sorted(missing)}")
        if int(item["rank"]) != idx:
            raise SystemExit(f"rank mismatch row {idx}")
        if int(item["character_age_days"]) > args.max_age_days:
            raise SystemExit(f"age exceeds threshold row {idx}")
        rr = float(item["recent_ratio"])
        if rr < 0 or rr > 1:
            raise SystemExit(f"recent_ratio out of range row {idx}")
    print(f"OK emerging export rows={len(items)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
