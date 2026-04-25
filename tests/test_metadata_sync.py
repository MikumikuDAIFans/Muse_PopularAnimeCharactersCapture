import argparse
import importlib.util
import json
import sys
from pathlib import Path

from services.danbooru import DanbooruPost
from workers.crawler import PostCrawlerWorker


ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str):
    path = ROOT / "scripts" / name
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


def test_monthly_shards_use_stable_task_ids():
    sync_recent_posts = load_script("sync_recent_posts.py")
    args = argparse.Namespace(
        start_date="2026-01-15",
        end_date="2026-03-02",
        recent_months=24,
        shard="monthly",
        tag_filter=None,
        task_id=900001,
        task_id_prefix=924000,
    )

    shards = sync_recent_posts.build_shards(args)

    assert [shard.tag_filter for shard in shards] == [
        "date:2026-01-15..2026-01-31",
        "date:2026-02-01..2026-02-28",
        "date:2026-03-01..2026-03-02",
    ]
    assert [shard.task_id for shard in shards] == [924001, 924002, 924003]


def test_crawler_search_resume_uses_checkpoint_cursor(tmp_path):
    class FakeClient:
        def __init__(self):
            self.pages = []

        def get_posts(self, **kwargs):
            self.pages.append(kwargs.get("page"))
            if kwargs.get("page") == "b10":
                return [DanbooruPost(id=9), DanbooruPost(id=8)]
            return []

    worker = PostCrawlerWorker(task_id=1, output_dir=tmp_path, tag_filter="rating:s", resume=True)
    worker._client = FakeClient()
    worker.output_file.write_text(json.dumps({"id": 10}) + "\n", encoding="utf-8")
    worker.checkpoint_file.write_text(
        json.dumps({"task_id": 1, "last_cursor": 10, "processed_pages": 1}),
        encoding="utf-8",
    )

    worker.run()

    lines = [json.loads(line)["id"] for line in worker.output_file.read_text(encoding="utf-8").splitlines()]
    assert worker._client.pages[0] == "b10"
    assert lines == [10, 9, 8]
    assert worker.result["written"] == 2


def test_character_candidates_from_jsonl(tmp_path):
    candidates = load_script("build_character_candidates_from_jsonl.py")
    jsonl = tmp_path / "task_1_posts.jsonl"
    jsonl.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "id": 1,
                        "created_at": "2026-01-01T00:00:00Z",
                        "tag_string_character": "hatsune_miku",
                        "tag_string_copyright": "vocaloid project_sekai",
                    }
                ),
                json.dumps(
                    {
                        "id": 2,
                        "created_at": "2026-01-02T00:00:00Z",
                        "tag_string_character": "hatsune_miku",
                        "tag_string_copyright": "vocaloid",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    args = argparse.Namespace(
        inputs=[str(jsonl)],
        input_root=None,
        pattern="*.jsonl",
        recent_months=0,
        top_n=10,
        min_count=1,
        copyright_limit=5,
    )

    payload = candidates.build_candidates(args)

    assert payload["stats"]["scanned_posts"] == 2
    assert payload["characters"][0]["character_tag"] == "hatsune_miku"
    assert payload["characters"][0]["recent_post_count"] == 2
    assert payload["characters"][0]["copyrights"][:2] == ["vocaloid", "project_sekai"]
