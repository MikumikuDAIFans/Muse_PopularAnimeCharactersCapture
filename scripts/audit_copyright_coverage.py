"""审计角色榜单 copyright 覆盖率。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("json_path", type=Path)
    parser.add_argument("--min-coverage", type=float, default=0.9)
    args = parser.parse_args()
    payload = json.loads(args.json_path.read_text(encoding="utf-8"))
    chars = payload.get("characters", [])
    if not chars:
        raise SystemExit("no characters found")
    with_copyright = [c for c in chars if c.get("copyrights")]
    coverage = len(with_copyright) / len(chars)
    print(f"copyright coverage={coverage:.2%} ({len(with_copyright)}/{len(chars)})")
    if coverage < args.min_coverage:
        raise SystemExit(f"coverage below threshold: {coverage:.2%} < {args.min_coverage:.2%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
