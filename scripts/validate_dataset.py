"""校验 dataset/{character}/{post_id}.{ext,txt,json} 三件套。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REQUIRED_JSON = {
    "post_id",
    "file_url",
    "source_url",
    "width",
    "height",
    "file_ext",
    "rating",
    "score",
    "fav_count",
    "created_at",
    "raw_tag_string",
    "tag_groups",
    "caption_v1",
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_dir", type=Path)
    parser.add_argument("--allow-missing-images", action="store_true")
    args = parser.parse_args()

    if not args.dataset_dir.exists():
        raise SystemExit(f"dataset dir not found: {args.dataset_dir}")

    json_files = sorted(args.dataset_dir.glob("*.json"))
    if not json_files:
        raise SystemExit("no json metadata files found")

    for json_path in json_files:
        meta = json.loads(json_path.read_text(encoding="utf-8"))
        missing = REQUIRED_JSON - set(meta)
        if missing:
            raise SystemExit(f"{json_path.name} missing fields: {sorted(missing)}")
        post_id = str(meta["post_id"])
        txt = args.dataset_dir / f"{post_id}.txt"
        if not txt.exists():
            raise SystemExit(f"missing caption file: {txt.name}")
        caption = txt.read_text(encoding="utf-8").strip()
        if caption != meta["caption_v1"]:
            raise SystemExit(f"caption mismatch for {post_id}")
        ext = meta.get("file_ext") or "jpg"
        image = args.dataset_dir / f"{post_id}.{ext}"
        if not args.allow_missing_images and not image.exists():
            raise SystemExit(f"missing image file: {image.name}")

    print(f"OK dataset samples={len(json_files)} dir={args.dataset_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
