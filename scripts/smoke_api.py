"""Docker/API smoke test。"""

from __future__ import annotations

import argparse
import sys

import httpx


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="http://localhost:8001")
    args = parser.parse_args()
    root = args.root.rstrip("/")
    with httpx.Client(timeout=20) as client:
        health = client.get(f"{root}/health")
        health.raise_for_status()
        stats = client.get(f"{root}/api/stats")
        stats.raise_for_status()
        tasks = client.get(f"{root}/api/tasks")
        tasks.raise_for_status()
    print("OK api smoke", health.json())
    return 0


if __name__ == "__main__":
    sys.exit(main())
