"""校验角色榜单 JSON/CSV 交付物。"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path


REQUIRED = {"rank", "character_tag", "copyrights", "post_count"}


def load_json(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "characters" not in payload:
        raise ValueError("JSON must be an object with characters")
    return payload["characters"]


def load_csv(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def normalize_json_row(row: dict) -> dict:
    return {
        "rank": str(row.get("rank")),
        "character_tag": row.get("character_tag") or "",
        "copyrights": "|".join(row.get("copyrights") or []),
        "post_count": str(row.get("post_count")),
    }


def normalize_csv_row(row: dict) -> dict:
    return {
        "rank": str(row.get("rank")),
        "character_tag": row.get("character_tag") or "",
        "copyrights": row.get("copyrights") or "",
        "post_count": str(row.get("post_count")),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_path", type=Path)
    parser.add_argument("csv_path", type=Path, nargs="?")
    parser.add_argument("--expect-count", type=int)
    args = parser.parse_args()

    chars = load_json(args.json_path)
    if args.expect_count is not None and len(chars) != args.expect_count:
        raise SystemExit(f"expected {args.expect_count} characters, got {len(chars)}")
    for idx, row in enumerate(chars, 1):
        missing = REQUIRED - set(row)
        if missing:
            raise SystemExit(f"row {idx} missing fields: {sorted(missing)}")
        if int(row["rank"]) != idx:
            raise SystemExit(f"row {idx} rank mismatch: {row['rank']}")
        if not isinstance(row["copyrights"], list):
            raise SystemExit(f"row {idx} copyrights must be a list")

    if args.csv_path:
        csv_rows = load_csv(args.csv_path)
        if len(csv_rows) != len(chars):
            raise SystemExit(f"csv/json count mismatch: {len(csv_rows)} != {len(chars)}")
        for idx, (jrow, crow) in enumerate(zip(chars, csv_rows), 1):
            if normalize_json_row(jrow) != normalize_csv_row(crow):
                raise SystemExit(f"csv/json mismatch at row {idx}")

    print(f"OK character export rows={len(chars)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
