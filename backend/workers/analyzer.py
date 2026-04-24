"""角色分析Worker"""

import json
import time
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from workers.base import BaseWorker, WorkerConfig
from services.danbooru import DanbooruClient, get_danbooru_client


class CharacterAnalyzerWorker(BaseWorker):
    """角色分析Worker：从已下载的元数据中分析角色标签"""

    def __init__(
        self,
        task_id: int,
        metadata_dir: Path,
        min_post_count: int = 50,
        recent_months: int = 6,
        top_n: int = 200,
        config: Optional[WorkerConfig] = None,
    ) -> None:
        super().__init__(config)
        self.task_id = task_id
        self.metadata_dir = Path(metadata_dir)
        self.min_post_count = min_post_count
        self.recent_months = recent_months
        self.top_n = top_n
        self._stop_event = threading.Event()

    def _scan_metadata_files(self) -> List[Path]:
        """扫描所有元数据文件"""
        if not self.metadata_dir.exists():
            return []
        return list(self.metadata_dir.glob("*.jsonl"))

    def _extract_character_tags(self) -> Dict[str, Dict[str, Any]]:
        """从元数据中提取character标签"""
        char_counter: Dict[str, Dict[str, Any]] = {}

        files = self._scan_metadata_files()
        self.update_progress(
            "analyzer", status="running",
            message=f"扫描 {len(files)} 个元数据文件..."
        )

        total_lines = 0
        for meta_file in files:
            try:
                with open(meta_file, "r", encoding="utf-8") as f:
                    for line in f:
                        total_lines += 1
                        if total_lines % 10000 == 0:
                            self.update_progress(
                                "analyzer",
                                processed=total_lines,
                                status="running",
                                message=f"已扫描 {total_lines} 条元数据"
                            )

                        if self._stop_event.is_set():
                            return char_counter

                        try:
                            post = json.loads(line.strip())
                        except json.JSONDecodeError:
                            continue

                        post_chars = [
                            t.strip()
                            for t in (post.get("tag_string_character") or "").split()
                            if t.strip()
                        ]
                        post_copyrights = [
                            t.strip()
                            for t in (post.get("tag_string_copyright") or "").split()
                            if t.strip()
                        ]

                        # 兼容旧版手工数据: character:foo copyright:bar
                        if not post_chars and not post_copyrights:
                            tag_string = post.get("tag_string", "") or ""
                            for tag in [t.strip() for t in tag_string.split()]:
                                if tag.startswith("character:"):
                                    post_chars.append(tag[len("character:"):])
                                elif tag.startswith("copyright:"):
                                    post_copyrights.append(tag[len("copyright:"):])

                        for char in post_chars:
                            if char not in char_counter:
                                char_counter[char] = {
                                    "count": 0,
                                    "copyrights": {},
                                }
                            char_counter[char]["count"] += 1

                            for cr in post_copyrights:
                                if cr not in char_counter[char]["copyrights"]:
                                    char_counter[char]["copyrights"][cr] = 0
                                char_counter[char]["copyrights"][cr] += 1

                self.update_progress(
                    "analyzer",
                    message=f"完成扫描 {meta_file.name}: {total_lines} 条"
                )
            except Exception as exc:
                print(f"[WARN] 扫描 {meta_file} 失败: {exc}", flush=True)

        return char_counter

    def _calculate_popularity(self, char_counter: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """计算角色热度分数"""
        # 热度分数 = 总帖子数 * sqrt(关联作品数)
        results = []
        for char_name, data in char_counter.items():
            count = data["count"]
            if count < self.min_post_count:
                continue

            copyright_count = len(data["copyrights"])
            # 热度公式：帖子数 * log(作品数 + 1)
            score = count * (copyright_count ** 0.5 + 1)

            # 取最热门的3个作品
            top_copyrights = sorted(
                data["copyrights"].items(),
                key=lambda x: x[1],
                reverse=True
            )[:3]
            top_copyright_names = [cr[0] for cr in top_copyrights]

            results.append({
                "character_tag": char_name,
                "total_post_count": count,
                "copyrights": top_copyright_names,
                "popularity_score": round(score, 4),
            })

        # 按热度排序
        results.sort(key=lambda x: x["popularity_score"], reverse=True)
        return results[:self.top_n]

    def run(self) -> None:
        """执行分析"""
        self._stop_event.clear()
        self.update_progress("analyzer", status="running", message="开始分析")

        # 1. 扫描元数据
        char_counter = self._extract_character_tags()
        self.update_progress(
            "analyzer",
            processed=1,
            total=3,
            status="running",
            message=f"扫描完成，发现 {len(char_counter)} 个角色"
        )

        if self._stop_event.is_set():
            return

        # 2. 计算热度
        self.update_progress(
            "analyzer",
            processed=2,
            total=3,
            status="running",
            message="计算热度分数..."
        )
        results = self._calculate_popularity(char_counter)

        # 3. 保存结果
        output_file = self.metadata_dir / f"task_{self.task_id}_characters.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "min_post_count": self.min_post_count,
                "total_characters": len(char_counter),
                "top_characters": results,
            }, f, ensure_ascii=False, indent=2)

        self.update_progress(
            "analyzer",
            processed=3,
            total=3,
            status="completed",
            message=f"分析完成，生成 Top {len(results)} 角色榜单 → {output_file.name}"
        )

    def stop(self) -> None:
        """停止分析"""
        self._stop_event.set()
