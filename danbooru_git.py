#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from __future__ import annotations

import hashlib
import json
import multiprocessing as mp
import os
import queue
import shutil
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter

# =========================
# 可修改配置
# =========================

# ---------- Danbooru 基础配置 ----------
BASE_URL = "https://danbooru.donmai.us"
DANBOORU_LOGIN = None
DANBOORU_API_KEY = None
USER_AGENT = "DanbooruRangeCrawler/1.0 (+https://example.invalid)"

# ---------- 抓取区间 ----------
START_ID = 10576339
END_ID = 10908849

# ---------- 输出目录 ----------
OUTPUT_ROOT = Path("./danbooru_dump")
METADATA_DIR = OUTPUT_ROOT / "metadata"
IMAGE_DIR = OUTPUT_ROOT / "images"
LOG_DIR = OUTPUT_ROOT / "logs"
RANGE_MARK_DIR = OUTPUT_ROOT / "_range_done"
TEMP_DIR = OUTPUT_ROOT / "_tmp"

# ---------- 并发配置 ----------
METADATA_THREADS = 6
DOWNLOAD_PROCESSES = max(1, (os.cpu_count() or 4) // 2)
DOWNLOAD_QUEUE_MAXSIZE = max(256, DOWNLOAD_PROCESSES * 128)

# ---------- API 分页 / 分片配置 ----------
POSTS_LIMIT = 1000
ID_WINDOW_SIZE = 50000

# ---------- 速率与重试 ----------
GLOBAL_API_RPS = 1.5
DOWNLOAD_RPS_PER_PROCESS = 0.0
API_MAX_RETRIES = 6
DOWNLOAD_MAX_RETRIES = 6
RETRY_BACKOFF_BASE = 1.5
API_TIMEOUT = (10, 60)
DOWNLOAD_TIMEOUT = (15, 300)

# ---------- 文件保存 ----------
USE_IMAGE_SUBDIRS = True
IMAGE_SUBDIR_DIVISOR = 10000
DOWNLOAD_ORIGINAL_FILES = True
ALLOWED_FILE_EXTS = {"jpg", "jpeg", "png", "webp"}  

# ---------- 校验 ----------
VERIFY_MD5 = True
VERIFY_FILE_SIZE = True
VERIFY_EXISTING_FILE_ON_RESUME = True
VALIDATE_METADATA_SCHEMA = True

# ---------- 日志 / 进度 ----------
PROGRESS_INTERVAL_SEC = 5
PRINT_EVERY_RANGE_SUMMARY = True

# =========================
# 配置结束
# =========================

_STOP_EVENT = threading.Event()


@dataclass(frozen=True)
class RangeTask:
    lo: int
    hi: int

    @property
    def name(self) -> str:
        return f"{self.lo:09d}_{self.hi:09d}"

    @property
    def metadata_path(self) -> Path:
        return METADATA_DIR / f"{self.name}.jsonl"

    @property
    def holes_path(self) -> Path:
        return METADATA_DIR / f"{self.name}.holes.txt"

    @property
    def invalid_metadata_path(self) -> Path:
        return METADATA_DIR / f"{self.name}.invalid.jsonl"

    @property
    def done_marker_path(self) -> Path:
        return RANGE_MARK_DIR / f"{self.name}.ok"

    @property
    def summary_path(self) -> Path:
        return RANGE_MARK_DIR / f"{self.name}.summary.json"


class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.range_done = 0
        self.range_failed = 0
        self.posts_seen = 0
        self.posts_written = 0
        self.invalid_metadata = 0
        self.posts_enqueued = 0
        self.download_ok = 0
        self.download_skip = 0
        self.download_fail = 0
        self.download_bytes = 0

    def add(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return {
                "range_done": self.range_done,
                "range_failed": self.range_failed,
                "posts_seen": self.posts_seen,
                "posts_written": self.posts_written,
                "invalid_metadata": self.invalid_metadata,
                "posts_enqueued": self.posts_enqueued,
                "download_ok": self.download_ok,
                "download_skip": self.download_skip,
                "download_fail": self.download_fail,
                "download_bytes": self.download_bytes,
            }


class RateLimiter:
    """线程安全的简单全局限速器。"""

    def __init__(self, rate_per_sec: float) -> None:
        self.rate = float(rate_per_sec)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.rate <= 0:
            return
        interval = 1.0 / self.rate
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_s = self._next_allowed - now
                self._next_allowed += interval
            else:
                sleep_s = 0.0
                self._next_allowed = now + interval
        if sleep_s > 0:
            time.sleep(sleep_s)


def install_signal_handlers() -> None:
    def _handler(signum, _frame):
        print(f"\n[WARN] 收到信号 {signum}，准备优雅退出...", flush=True)
        _STOP_EVENT.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def ensure_dirs() -> None:
    for p in [OUTPUT_ROOT, METADATA_DIR, IMAGE_DIR, LOG_DIR, RANGE_MARK_DIR, TEMP_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def build_auth_params() -> Dict[str, str]:
    params: Dict[str, str] = {}
    if DANBOORU_LOGIN and DANBOORU_API_KEY:
        params["login"] = str(DANBOORU_LOGIN)
        params["api_key"] = str(DANBOORU_API_KEY)
    return params


def build_session(pool_size: int = 32) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=0)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    return session


def request_json(
    session: requests.Session,
    path: str,
    params: Dict[str, object],
    timeout: Tuple[int, int],
    limiter: Optional[RateLimiter],
    max_retries: int,
) -> object:
    url = BASE_URL.rstrip("/") + path
    merged = dict(build_auth_params())
    merged.update(params)

    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        if _STOP_EVENT.is_set():
            raise RuntimeError("收到停止信号")
        try:
            if limiter is not None:
                limiter.wait()
            resp = session.get(url, params=merged, timeout=timeout)
            if resp.status_code == 429:
                sleep_s = RETRY_BACKOFF_BASE ** attempt
                print(f"[WARN] 429 Too Many Requests, {sleep_s:.1f}s 后重试: {url}", flush=True)
                time.sleep(sleep_s)
                continue
            if 500 <= resp.status_code < 600:
                sleep_s = RETRY_BACKOFF_BASE ** attempt
                print(f"[WARN] HTTP {resp.status_code}, {sleep_s:.1f}s 后重试: {url}", flush=True)
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt == max_retries:
                break
            sleep_s = RETRY_BACKOFF_BASE ** attempt
            print(f"[WARN] JSON 请求失败，第 {attempt}/{max_retries} 次重试，{sleep_s:.1f}s 后继续: {exc}", flush=True)
            time.sleep(sleep_s)
    raise RuntimeError(f"JSON 请求最终失败: {url} params={merged} err={last_exc}")


def build_ranges(start_id: int, end_id: int, window_size: int) -> List[RangeTask]:
    tasks: List[RangeTask] = []
    cur = start_id
    while cur <= end_id:
        hi = min(end_id, cur + window_size - 1)
        tasks.append(RangeTask(cur, hi))
        cur = hi + 1
    # 倒序有利于先抓新帖，因为 Danbooru 默认按 id 递减翻页。
    tasks.reverse()
    return tasks


def is_range_done(task: RangeTask) -> bool:
    return task.done_marker_path.exists() and task.metadata_path.exists()


def atomic_write_text(path: Path, text: str) -> None:
    tmp = TEMP_DIR / (path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def atomic_move(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.replace(src, dst)


def compress_missing_ids_to_ranges(ids: List[int]) -> List[str]:
    if not ids:
        return []
    ids = sorted(ids)
    out: List[str] = []
    start = prev = ids[0]
    for x in ids[1:]:
        if x == prev + 1:
            prev = x
            continue
        out.append(f"{start}-{prev}" if start != prev else str(start))
        start = prev = x
    out.append(f"{start}-{prev}" if start != prev else str(start))
    return out


def validate_post_metadata(post: object) -> Tuple[bool, List[str]]:
    if not isinstance(post, dict):
        return False, ["post is not dict"]

    reasons: List[str] = []
    if "id" not in post:
        reasons.append("missing id")
    elif not isinstance(post["id"], int):
        reasons.append("id is not int")

    # Danbooru 官方代码里 /posts.json 对可见帖子会返回 file_url / preview_url / md5。
    # 对不可见帖子则这些字段可能缺失，因此这里只做“有则校验”，不强制要求必须存在。
    if "file_url" in post and post["file_url"] is not None and not isinstance(post["file_url"], str):
        reasons.append("file_url is not str")
    if "md5" in post and post["md5"] is not None and not isinstance(post["md5"], str):
        reasons.append("md5 is not str")
    if "file_ext" in post and post["file_ext"] is not None and not isinstance(post["file_ext"], str):
        reasons.append("file_ext is not str")
    if "file_size" in post and post["file_size"] is not None and not isinstance(post["file_size"], int):
        reasons.append("file_size is not int")

    return len(reasons) == 0, reasons


def should_download_post(post: Dict[str, object]) -> bool:
    if not DOWNLOAD_ORIGINAL_FILES:
        return False
    file_url = post.get("file_url")
    if not file_url or not isinstance(file_url, str):
        return False
    ext = str(post.get("file_ext") or "").lower().strip(".")
    if ALLOWED_FILE_EXTS is not None and ext not in ALLOWED_FILE_EXTS:
        return False
    return True


def derive_ext(post: Dict[str, object]) -> str:
    ext = str(post.get("file_ext") or "").lower().strip(".")
    if ext:
        return ext
    file_url = str(post.get("file_url") or "")
    parsed = urlparse(file_url)
    suffix = Path(parsed.path).suffix.lower().strip(".")
    return suffix or "bin"


def build_image_path(post_id: int, ext: str) -> Path:
    if USE_IMAGE_SUBDIRS:
        sub = f"{post_id // IMAGE_SUBDIR_DIVISOR:06d}"
        return IMAGE_DIR / sub / f"{post_id}.{ext}"
    return IMAGE_DIR / f"{post_id}.{ext}"


def md5_of_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def is_existing_file_valid(path: Path, md5_hex: Optional[str], file_size: Optional[int]) -> bool:
    if not path.exists() or not path.is_file():
        return False
    try:
        stat = path.stat()
        if VERIFY_FILE_SIZE and file_size is not None and stat.st_size != int(file_size):
            return False
        if VERIFY_MD5 and VERIFY_EXISTING_FILE_ON_RESUME and md5_hex:
            return md5_of_file(path) == md5_hex.lower()
        return stat.st_size > 0
    except Exception:  # noqa: BLE001
        return False


def download_one_file(
    session: requests.Session,
    task: Dict[str, object],
    limiter: Optional[RateLimiter],
) -> Dict[str, object]:
    post_id = int(task["id"])
    file_url = str(task["file_url"])
    md5_hex = str(task.get("md5") or "").lower() or None
    file_size = int(task["file_size"]) if task.get("file_size") is not None else None
    ext = str(task["file_ext"])
    out_path = Path(str(task["out_path"]))
    tmp_path = Path(str(task["tmp_path"]))

    out_path.parent.mkdir(parents=True, exist_ok=True)

    if is_existing_file_valid(out_path, md5_hex, file_size):
        return {"status": "skip", "id": post_id, "bytes": out_path.stat().st_size}

    if tmp_path.exists():
        try:
            tmp_path.unlink()
        except Exception:  # noqa: BLE001
            pass

    last_error: Optional[str] = None
    for attempt in range(1, DOWNLOAD_MAX_RETRIES + 1):
        if _STOP_EVENT.is_set():
            return {"status": "fail", "id": post_id, "error": "stopped"}
        try:
            if limiter is not None:
                limiter.wait()
            with session.get(file_url, stream=True, timeout=DOWNLOAD_TIMEOUT) as resp:
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                resp.raise_for_status()

                hasher = hashlib.md5() if VERIFY_MD5 and md5_hex else None
                total = 0
                with open(tmp_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        f.write(chunk)
                        total += len(chunk)
                        if hasher is not None:
                            hasher.update(chunk)

            if VERIFY_FILE_SIZE and file_size is not None:
                real_size = tmp_path.stat().st_size
                if real_size != file_size:
                    raise RuntimeError(f"size mismatch: got={real_size}, expect={file_size}")

            if hasher is not None:
                real_md5 = hasher.hexdigest().lower()
                if real_md5 != md5_hex:
                    raise RuntimeError(f"md5 mismatch: got={real_md5}, expect={md5_hex}")

            os.replace(tmp_path, out_path)
            return {"status": "ok", "id": post_id, "bytes": out_path.stat().st_size, "ext": ext}
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:  # noqa: BLE001
                pass
            if attempt < DOWNLOAD_MAX_RETRIES:
                sleep_s = RETRY_BACKOFF_BASE ** attempt
                time.sleep(sleep_s)
            else:
                break

    return {
        "status": "fail",
        "id": post_id,
        "error": last_error or "unknown",
        "url": file_url,
        "path": str(out_path),
    }


def download_worker_main(task_queue: mp.JoinableQueue, result_queue: mp.Queue) -> None:
    session = build_session(pool_size=8)
    limiter = RateLimiter(DOWNLOAD_RPS_PER_PROCESS) if DOWNLOAD_RPS_PER_PROCESS > 0 else None

    while True:
        task = task_queue.get()
        try:
            if task is None:
                return
            result = download_one_file(session, task, limiter)
            result_queue.put(result)
        finally:
            task_queue.task_done()


class ResultWriterThread(threading.Thread):
    def __init__(self, result_queue: mp.Queue, stats: Stats, workers: List[mp.Process]) -> None:
        super().__init__(daemon=True)
        self.result_queue = result_queue
        self.stats = stats
        self.workers = workers
        self.failure_log = LOG_DIR / "download_failures.jsonl"

    def run(self) -> None:
        with open(self.failure_log, "a", encoding="utf-8") as fail_f:
            while True:
                alive = any(p.is_alive() for p in self.workers)
                try:
                    item = self.result_queue.get(timeout=0.5)
                except queue.Empty:
                    if not alive and self.result_queue.empty():
                        break
                    continue

                status = item.get("status")
                if status == "ok":
                    self.stats.add(download_ok=1, download_bytes=int(item.get("bytes", 0)))
                elif status == "skip":
                    self.stats.add(download_skip=1, download_bytes=int(item.get("bytes", 0)))
                else:
                    self.stats.add(download_fail=1)
                    fail_f.write(json.dumps(item, ensure_ascii=False) + "\n")
                    fail_f.flush()


class ProgressThread(threading.Thread):
    def __init__(self, stats: Stats) -> None:
        super().__init__(daemon=True)
        self.stats = stats

    def run(self) -> None:
        last = time.monotonic()
        while not _STOP_EVENT.is_set():
            now = time.monotonic()
            if now - last >= PROGRESS_INTERVAL_SEC:
                s = self.stats.snapshot()
                gib = s["download_bytes"] / (1024 ** 3)
                print(
                    "[PROGRESS] "
                    f"ranges ok={s['range_done']} fail={s['range_failed']} | "
                    f"posts seen={s['posts_seen']} written={s['posts_written']} invalid={s['invalid_metadata']} queued={s['posts_enqueued']} | "
                    f"download ok={s['download_ok']} skip={s['download_skip']} fail={s['download_fail']} | "
                    f"bytes={gib:.2f} GiB",
                    flush=True,
                )
                last = now
            time.sleep(0.5)


def crawl_range(task: RangeTask, download_queue: mp.JoinableQueue, stats: Stats, api_limiter: Optional[RateLimiter]) -> Dict[str, object]:
    if is_range_done(task):
        stats.add(range_done=1)
        return {"range": task.name, "skipped": True}

    session = build_session(pool_size=16)
    tmp_metadata = TEMP_DIR / f"{task.name}.jsonl.tmp"
    tmp_invalid = TEMP_DIR / f"{task.name}.invalid.jsonl.tmp"
    tmp_holes = TEMP_DIR / f"{task.name}.holes.txt.tmp"

    seen_ids = set()
    invalid_count = 0
    written_count = 0
    queued_count = 0
    page_count = 0

    cursor = task.hi + 1

    with open(tmp_metadata, "w", encoding="utf-8") as meta_f, open(tmp_invalid, "w", encoding="utf-8") as invalid_f:
        while not _STOP_EVENT.is_set():
            page_count += 1
            data = request_json(
                session=session,
                path="/posts.json",
                params={"limit": POSTS_LIMIT, "page": f"b{cursor}"},
                timeout=API_TIMEOUT,
                limiter=api_limiter,
                max_retries=API_MAX_RETRIES,
            )

            if not isinstance(data, list):
                raise RuntimeError(f"/posts.json 返回不是 list: range={task.name}")
            if not data:
                break

            min_id_on_page = None
            advanced = False

            for post in data:
                if not isinstance(post, dict) or "id" not in post:
                    invalid_count += 1
                    invalid_f.write(json.dumps({"_error": ["bad page item"], "_raw": post}, ensure_ascii=False) + "\n")
                    continue

                post_id = int(post["id"])
                if min_id_on_page is None or post_id < min_id_on_page:
                    min_id_on_page = post_id

                # page=b<cursor> 可能返回低于窗口 lo 的内容；窗口外不写。
                if post_id > task.hi:
                    advanced = True
                    continue
                if post_id < task.lo:
                    advanced = True
                    continue

                seen_ids.add(post_id)
                stats.add(posts_seen=1)

                if VALIDATE_METADATA_SCHEMA:
                    ok, reasons = validate_post_metadata(post)
                    if not ok:
                        invalid_count += 1
                        invalid_post = {"_error": reasons, "_raw": post}
                        invalid_f.write(json.dumps(invalid_post, ensure_ascii=False) + "\n")
                        stats.add(invalid_metadata=1)

                meta_f.write(json.dumps(post, ensure_ascii=False) + "\n")
                written_count += 1
                stats.add(posts_written=1)

                if should_download_post(post):
                    ext = derive_ext(post)
                    out_path = build_image_path(post_id, ext)
                    tmp_path = Path(str(out_path) + ".part")
                    download_task = {
                        "id": post_id,
                        "file_url": post["file_url"],
                        "md5": post.get("md5"),
                        "file_size": post.get("file_size"),
                        "file_ext": ext,
                        "out_path": str(out_path),
                        "tmp_path": str(tmp_path),
                    }
                    download_queue.put(download_task)
                    queued_count += 1
                    stats.add(posts_enqueued=1)
                advanced = True

            if min_id_on_page is None:
                break
            if min_id_on_page >= cursor:
                raise RuntimeError(f"分页游标未推进: range={task.name}, cursor={cursor}, min_id={min_id_on_page}")

            # 下一页继续向更小 id 翻。
            cursor = min_id_on_page
            if min_id_on_page < task.lo:
                break
            if not advanced:
                break

    missing_ids = [x for x in range(task.lo, task.hi + 1) if x not in seen_ids]
    missing_ranges = compress_missing_ids_to_ranges(missing_ids)
    with open(tmp_holes, "w", encoding="utf-8") as holes_f:
        for line in missing_ranges:
            holes_f.write(line + "\n")

    atomic_move(tmp_metadata, task.metadata_path)
    atomic_move(tmp_invalid, task.invalid_metadata_path)
    atomic_move(tmp_holes, task.holes_path)
    atomic_write_text(task.done_marker_path, "ok\n")

    summary = {
        "range": task.name,
        "lo": task.lo,
        "hi": task.hi,
        "page_count": page_count,
        "seen_count": len(seen_ids),
        "written_count": written_count,
        "invalid_count": invalid_count,
        "queued_count": queued_count,
        "missing_count": len(missing_ids),
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    atomic_write_text(task.summary_path, json.dumps(summary, ensure_ascii=False, indent=2))
    stats.add(range_done=1)
    return summary


def start_download_workers(task_queue: mp.JoinableQueue, result_queue: mp.Queue) -> List[mp.Process]:
    workers: List[mp.Process] = []
    for idx in range(DOWNLOAD_PROCESSES):
        p = mp.Process(target=download_worker_main, args=(task_queue, result_queue), name=f"dl-{idx}")
        p.daemon = True
        p.start()
        workers.append(p)
    return workers


def stop_download_workers(task_queue: mp.JoinableQueue, workers: List[mp.Process]) -> None:
    for _ in workers:
        task_queue.put(None)
    task_queue.join()
    for p in workers:
        p.join()


def main() -> int:
    ensure_dirs()
    install_signal_handlers()

    if START_ID > END_ID:
        print("[ERROR] START_ID 不能大于 END_ID", flush=True)
        return 2
    if POSTS_LIMIT <= 0:
        print("[ERROR] POSTS_LIMIT 必须 > 0", flush=True)
        return 2

    stats = Stats()
    progress_thread = ProgressThread(stats)
    progress_thread.start()

    task_queue: mp.JoinableQueue = mp.JoinableQueue(maxsize=DOWNLOAD_QUEUE_MAXSIZE)
    result_queue: mp.Queue = mp.Queue()
    workers = start_download_workers(task_queue, result_queue)
    result_writer = ResultWriterThread(result_queue, stats, workers)
    result_writer.start()

    api_limiter = RateLimiter(GLOBAL_API_RPS) if GLOBAL_API_RPS > 0 else None

    all_ranges = build_ranges(START_ID, END_ID, ID_WINDOW_SIZE)
    pending_ranges = [r for r in all_ranges if not is_range_done(r)]
    print(
        f"[INFO] 总区间数: {len(all_ranges)}, 待抓取区间数: {len(pending_ranges)}, "
        f"metadata_threads={METADATA_THREADS}, download_processes={DOWNLOAD_PROCESSES}",
        flush=True,
    )

    failures: List[Tuple[str, str]] = []

    try:
        with ThreadPoolExecutor(max_workers=METADATA_THREADS) as executor:
            future_map = {
                executor.submit(crawl_range, task, task_queue, stats, api_limiter): task
                for task in pending_ranges
            }
            for fut in as_completed(future_map):
                task = future_map[fut]
                try:
                    summary = fut.result()
                    if PRINT_EVERY_RANGE_SUMMARY:
                        if summary.get("skipped"):
                            print(f"[RANGE] {task.name} 已完成，跳过", flush=True)
                        else:
                            print(
                                f"[RANGE] {task.name} done | pages={summary['page_count']} "
                                f"seen={summary['seen_count']} written={summary['written_count']} "
                                f"invalid={summary['invalid_count']} queued={summary['queued_count']} "
                                f"missing={summary['missing_count']}",
                                flush=True,
                            )
                except Exception as exc:  # noqa: BLE001
                    stats.add(range_failed=1)
                    failures.append((task.name, str(exc)))
                    print(f"[ERROR] 区间失败 {task.name}: {exc}", flush=True)
                    # 区间失败不立刻终止其他任务；最终统一返回非 0。
    finally:
        stop_download_workers(task_queue, workers)
        result_writer.join()
        _STOP_EVENT.set()
        progress_thread.join(timeout=1.0)

    if failures:
        fail_log = LOG_DIR / "range_failures.jsonl"
        with open(fail_log, "a", encoding="utf-8") as f:
            for name, err in failures:
                f.write(json.dumps({"range": name, "error": err}, ensure_ascii=False) + "\n")
        print(f"[WARN] 有 {len(failures)} 个区间失败，详情见: {fail_log}", flush=True)
        return 1

    s = stats.snapshot()
    print(
        "[DONE] "
        f"ranges={s['range_done']} failed_ranges={s['range_failed']} | "
        f"posts_seen={s['posts_seen']} metadata_written={s['posts_written']} invalid_metadata={s['invalid_metadata']} queued={s['posts_enqueued']} | "
        f"download_ok={s['download_ok']} skip={s['download_skip']} fail={s['download_fail']} | "
        f"download_bytes={s['download_bytes'] / (1024 ** 3):.2f} GiB",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    # Windows / macOS / Linux 都更稳，尤其是含 requests.Session 时。
    mp.freeze_support()
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass
    raise SystemExit(main())
