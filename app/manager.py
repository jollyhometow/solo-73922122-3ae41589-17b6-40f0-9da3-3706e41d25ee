"""任务编排：名额控制、状态机、持久化与重启恢复。

并发模型：
- asyncio 单事件循环；MAX_CONCURRENT_BATCHES 个重算名额，用 Condition 管理；
- 每个任务一个 asyncio.Task 协程；取消用 asyncio.Event 通知；
- 取消/超时后必须等计算进程及子进程退出，才进入收尾；
- 收尾 = 清理暂存 + 落终态 + 释放名额，三者绑定：
  * 暂存（原件/目录）删除失败 → 任务转入 cleanup_pending（待清理），
    保留取消/超时等原收尾原因，不发布判定、不释放该批名额；
  * 后台重试协程按 CLEANUP_RETRY_INTERVAL_SECONDS 自动续清，
    全部暂存实际清除后才落对应终态并释放名额（恰好一次）；
  * 服务重启后接续 cleanup_pending 批次：不重新重算、不遗忘原收尾原因；
- 清理绝不 chmod 擅改管理员设置的父目录权限；
- 另一个批次与状态查询不受影响（状态查询只读锁保护的字典，
  清理在线程中执行，不阻塞事件循环）。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .models import TERMINAL_STATUSES, TaskStatus
from .recalc import (
    RecalcCancelled,
    RecalcFailed,
    RecalcTimeout,
    cleanup_task_dir,
    run_recalc,
)
from .security import inspect_workbook, scan_container
from .workbook_io import (
    UnexpectedVerdict,
    WorkbookFormulaError,
    read_cached_verdicts,
    read_recalculated_verdicts,
)

_TERMINAL_VALUES = {s.value for s in TERMINAL_STATUSES}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _try_cleanup_sync(task_dir: Path) -> tuple[bool, str | None]:
    """同步尝试清理暂存目录；返回 (是否已完全清除, 失败原因)。"""
    try:
        cleanup_task_dir(task_dir)
        return True, None
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"


class SlotManager:
    """固定数量的重算名额，支持排队等待。"""

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._in_use = 0
        self._cond = asyncio.Condition()

    async def acquire(self) -> None:
        async with self._cond:
            while self._in_use >= self._capacity:
                await self._cond.wait()
            self._in_use += 1

    def acquire_nowait(self) -> None:
        """重启恢复时为待清理批次直接占位。

        可能瞬时超过容量；待清理批次释放后恢复正常，期间新批次排队等待。
        """
        self._in_use += 1

    async def release(self) -> None:
        async with self._cond:
            self._in_use = max(0, self._in_use - 1)
            self._cond.notify_all()

    @property
    def in_use(self) -> int:
        return self._in_use


class TaskManager:
    def __init__(self):
        self._tasks: dict[str, dict] = {}
        self._jobs: dict[str, asyncio.Task] = {}
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        self.slots = SlotManager(config.MAX_CONCURRENT_BATCHES)
        # task_id -> 是否仍持有重算名额（名额只在落终态时释放一次）
        self._slot_held: dict[str, bool] = {}
        # 待清理重试协程：key 为 task_id 或 "orphan:<目录名>"
        self._retry_tasks: dict[str, asyncio.Task] = {}
        # 无事件循环时（同步恢复路径）登记的重试，待 resume 时启动
        self._retry_pending: list[tuple[str, str | None, Path]] = []

    # ---------- 持久化 ----------

    def _task_file(self, task_id: str) -> Path:
        return config.TASKS_DIR / f"{task_id}.json"

    def _persist(self, record: dict) -> None:
        config.TASKS_DIR.mkdir(parents=True, exist_ok=True)
        tmp = self._task_file(record["task_id"]).with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        tmp.replace(self._task_file(record["task_id"]))

    async def _update(self, task_id: str, **fields) -> dict:
        async with self._lock:
            record = self._tasks[task_id]
            record.update(fields)
            self._persist(record)
            return dict(record)

    async def get(self, task_id: str) -> dict | None:
        async with self._lock:
            record = self._tasks.get(task_id)
            return dict(record) if record else None

    async def list_all(self) -> list[dict]:
        async with self._lock:
            return [dict(r) for r in self._tasks.values()]

    async def cleanup_pending_count(self) -> int:
        async with self._lock:
            return sum(
                1
                for r in self._tasks.values()
                if r["status"] == TaskStatus.CLEANUP_PENDING.value
            )

    # ---------- 名额（每个任务至多释放一次） ----------

    def _hold_slot(self, task_id: str) -> None:
        """登记某任务仍占用一个名额（重启恢复待清理批次时使用）。"""
        if not self._slot_held.get(task_id):
            self._slot_held[task_id] = True
            self.slots.acquire_nowait()

    async def _release_slot_once(self, task_id: str) -> None:
        """释放名额；每个任务至多释放一次，绝不重复释放。"""
        async with self._lock:
            held = self._slot_held.pop(task_id, False)
        if held:
            await self.slots.release()

    async def _release_slot_if_terminal(self, task_id: str) -> None:
        record = await self.get(task_id)
        if record is None:
            return
        if record["status"] in _TERMINAL_VALUES:
            await self._release_slot_once(task_id)

    # ---------- 重启恢复 ----------

    def recover_on_startup(self) -> None:
        """重启恢复。

        - cleanup_pending 批次：接续待清理（不重新重算、保留原收尾原因），
          立即尝试清理一次；存储未恢复则保持待清理、占住名额并注册重试；
        - 其它未走完终态的批次：标记 interrupted 并清理暂存，
          清理失败同样转入 cleanup_pending（终态推迟到实际清除后）；
        - 无主暂存目录尽力清理，失败也注册重试。
        """
        config.TASKS_DIR.mkdir(parents=True, exist_ok=True)
        config.STAGING_DIR.mkdir(parents=True, exist_ok=True)
        known_ids: set[str] = set()
        for path in sorted(config.TASKS_DIR.glob("*.json")):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            task_id = record["task_id"]
            known_ids.add(task_id)
            status = record.get("status")
            if status == TaskStatus.CLEANUP_PENDING.value:
                self._resume_cleanup_pending(record)
            elif status not in _TERMINAL_VALUES:
                self._mark_interrupted(record)
            self._tasks[task_id] = record
        # 无主暂存目录（无任务记录）：尽力清理，失败注册重试
        for child in config.STAGING_DIR.glob("*"):
            if child.is_dir() and child.name not in known_ids:
                ok, _err = _try_cleanup_sync(child)
                if not ok:
                    self._ensure_cleanup_retry(
                        f"orphan:{child.name}", None, child
                    )

    def _resume_cleanup_pending(self, record: dict) -> None:
        """接续重启前的待清理批次：保留原收尾原因，清完才落原终态。"""
        task_id = record["task_id"]
        ok, err = _try_cleanup_sync(config.STAGING_DIR / task_id)
        if ok:
            # 存储已恢复：直接落原终态（cancelled/failed/...），原因保留
            record["status"] = (
                record.get("pending_final_status") or TaskStatus.FAILED.value
            )
            record["finished_at"] = _now_iso()
            record["pending_final_status"] = None
            record["cleanup_error"] = None
        else:
            # 存储仍未恢复：保持待清理，恢复名额占用并注册后台重试
            record["cleanup_error"] = err
            self._hold_slot(task_id)
            self._ensure_cleanup_retry(
                task_id, task_id, config.STAGING_DIR / task_id
            )
        self._persist(record)

    def _mark_interrupted(self, record: dict) -> None:
        """重启时未走完终态的批次：标记中断；暂存清不掉则转待清理。"""
        task_id = record["task_id"]
        was_running = record.get("status") == TaskStatus.RUNNING.value
        record["error_code"] = "INTERRUPTED_BY_RESTART"
        record["error_message"] = "服务重启，该批次未完成，标记为中断"
        record["samples"] = None
        record["summary"] = None
        ok, err = _try_cleanup_sync(config.STAGING_DIR / task_id)
        if ok:
            record["status"] = TaskStatus.INTERRUPTED.value
            record["finished_at"] = _now_iso()
        else:
            record["status"] = TaskStatus.CLEANUP_PENDING.value
            record["pending_final_status"] = TaskStatus.INTERRUPTED.value
            record["cleanup_error"] = err
            if was_running:
                self._hold_slot(task_id)
            self._ensure_cleanup_retry(
                task_id, task_id, config.STAGING_DIR / task_id
            )
        self._persist(record)

    # ---------- 创建 ----------

    async def create_task(self, content: bytes, filename: str) -> dict:
        """容器与结构校验通过后落盘原件并启动后台协程。"""
        scan_container(content, filename)  # 拒收抛 WorkbookRejected
        inspect = inspect_workbook(content)  # 同上
        rows_info = inspect["rows"]

        task_id = uuid.uuid4().hex
        task_dir = config.STAGING_DIR / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        (task_dir / "input.xlsx").write_bytes(content)  # 原件副本，只读使用

        record = {
            "task_id": task_id,
            "status": TaskStatus.QUEUED.value,
            "filename": filename,
            "size_bytes": len(content),
            "created_at": _now_iso(),
            "started_at": None,
            "finished_at": None,
            "error_code": None,
            "error_message": None,
            "cancel_requested": False,
            "pending_final_status": None,
            "cleanup_error": None,
            "samples": None,
            "summary": None,
        }
        async with self._lock:
            self._tasks[task_id] = record
            self._cancel_events[task_id] = asyncio.Event()
            self._persist(record)

        job = asyncio.create_task(
            self._run(task_id, task_dir, rows_info), name=f"recalc-{task_id}"
        )
        self._jobs[task_id] = job
        job.add_done_callback(lambda _j: self._jobs.pop(task_id, None))
        return dict(record)

    # ---------- 取消 ----------

    async def request_cancel(self, task_id: str) -> tuple[dict, bool]:
        """返回 (记录, 是否受理成功)。

        - 终态与 cleanup_pending（清理收尾中）均不可取消；
        - QUEUED：通知排队协程取消并等它完成收尾（清理成功落 cancelled，
          失败转 cleanup_pending），返回时状态已可查；
        - RUNNING：立即受理（置取消事件），工作协程待进程组退出后
          自行收尾（清理+落终态+释放名额）。
        """
        status: str | None = None
        job_to_wait: asyncio.Task | None = None
        async with self._lock:
            record = self._tasks.get(task_id)
            if record is None:
                raise KeyError(task_id)
            status = record["status"]
            if (
                status in _TERMINAL_VALUES
                or status == TaskStatus.CLEANUP_PENDING.value
            ):
                return dict(record), False
            record["cancel_requested"] = True
            self._persist(record)
            event = self._cancel_events.get(task_id)
            if event is not None:
                event.set()
            if status == TaskStatus.QUEUED.value:
                job_to_wait = self._jobs.get(task_id)
                if job_to_wait is not None:
                    job_to_wait.cancel()
        if status == TaskStatus.QUEUED.value:
            if job_to_wait is not None:
                # 等排队协程完成收尾，保证取消结果（含待清理）立即可查
                try:
                    await asyncio.wait_for(
                        asyncio.shield(job_to_wait), timeout=10
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            else:
                # 防御：排队协程不存在，直接在本协程收尾
                await self._finalize(
                    task_id,
                    config.STAGING_DIR / task_id,
                    final_status=TaskStatus.CANCELLED.value,
                    error_code="CANCELLED",
                    error_message="排队期间取消，结果不发布",
                )
            record = await self.get(task_id)
            return (dict(record) if record else {}), True
        record = await self.get(task_id)
        return (dict(record) if record else {}), True

    # ---------- 主流程 ----------

    async def _run(
        self, task_id: str, task_dir: Path, rows_info: list[tuple]
    ) -> None:
        event = self._cancel_events.get(task_id)
        try:
            await self.slots.acquire()
            self._slot_held[task_id] = True
        except asyncio.CancelledError:
            # 排队期间被取消：在本协程完成收尾（清理成功才落终态）
            await self._finalize(
                task_id,
                task_dir,
                final_status=TaskStatus.CANCELLED.value,
                error_code="CANCELLED",
                error_message="排队期间取消，结果不发布",
            )
            raise
        try:
            try:
                if event is not None and event.is_set():
                    await self._finish_cancelled(task_id, task_dir)
                    return
                await self._update(
                    task_id,
                    status=TaskStatus.RUNNING.value,
                    started_at=_now_iso(),
                )

                # 先读原件缓存（原件不做任何修改）
                content = (task_dir / "input.xlsx").read_bytes()
                cached = read_cached_verdicts(content, rows_info)

                out_file = await run_recalc(task_dir, cancel_event=event)
                recalculated = read_recalculated_verdicts(
                    str(out_file), rows_info
                )

                if event is not None and event.is_set():
                    await self._finish_cancelled(task_id, task_dir)
                    return

                await self._publish_results(
                    task_id, task_dir, rows_info, cached, recalculated
                )
            except WorkbookFormulaError as exc:
                await self._finish_failed(
                    task_id, task_dir, "FORMULA_ERROR", str(exc)
                )
            except RecalcTimeout as exc:
                await self._finish_failed(
                    task_id, task_dir, "RECALC_TIMEOUT", str(exc)
                )
            except RecalcCancelled:
                await self._finish_cancelled(task_id, task_dir)
            except RecalcFailed as exc:
                await self._finish_failed(
                    task_id, task_dir, "RECALC_FAILED", str(exc)
                )
            except UnexpectedVerdict as exc:
                await self._finish_failed(
                    task_id, task_dir, "UNEXPECTED_VERDICT", str(exc)
                )
            except Exception as exc:  # 防御性兜底，保证任务必有收尾
                import sys
                import traceback
                print(
                    f"[manager] 任务 {task_id} 未预期异常：{exc!r}\n"
                    + "".join(traceback.format_exc()),
                    file=sys.stderr, flush=True,
                )
                await self._finish_failed(
                    task_id, task_dir, "INTERNAL_ERROR", repr(exc)[:300]
                )
        finally:
            # 防御：协程异常退出（如被外部 cancel）而未收尾时按原因收尾
            record = await self.get(task_id)
            if (
                record is not None
                and record["status"] not in _TERMINAL_VALUES
                and record["status"] != TaskStatus.CLEANUP_PENDING.value
            ):
                if record.get("cancel_requested"):
                    await self._finalize(
                        task_id,
                        task_dir,
                        final_status=TaskStatus.CANCELLED.value,
                        error_code="CANCELLED",
                        error_message="任务已取消，结果不发布",
                    )
                else:
                    await self._finalize(
                        task_id,
                        task_dir,
                        final_status=TaskStatus.FAILED.value,
                        error_code="INTERNAL_ERROR",
                        error_message="工作协程异常退出，按失败收尾",
                    )
            # 名额只在任务进入终态后释放一次；
            # cleanup_pending 的名额由清理重试成功后释放
            await self._release_slot_if_terminal(task_id)

    # ---------- 收尾：清理暂存 → 落终态 → 释放名额 ----------

    async def _try_cleanup(self, task_dir: Path) -> tuple[bool, str | None]:
        """在线程中执行清理，避免阻塞事件循环影响其它批次与查询。"""
        try:
            await asyncio.to_thread(cleanup_task_dir, task_dir)
            return True, None
        except OSError as exc:
            return False, f"{type(exc).__name__}: {exc}"

    async def _finalize(
        self,
        task_id: str,
        task_dir: Path,
        *,
        final_status: str,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """统一收尾：清理暂存成功才落终态并释放名额。

        删除失败时不落终态、不释放名额，转入 cleanup_pending 并注册
        后台重试；取消/超时等原收尾原因完整保留在记录里（error_code、
        error_message、pending_final_status），可供查询。
        """
        ok, err = await self._try_cleanup(task_dir)
        if ok:
            await self._update(
                task_id,
                status=final_status,
                finished_at=_now_iso(),
                error_code=error_code,
                error_message=error_message,
                pending_final_status=None,
                cleanup_error=None,
            )
            await self._release_slot_once(task_id)
        else:
            await self._update(
                task_id,
                status=TaskStatus.CLEANUP_PENDING.value,
                pending_final_status=final_status,
                error_code=error_code,
                error_message=error_message,
                cleanup_error=err or "暂存目录删除失败",
            )
            self._ensure_cleanup_retry(task_id, task_id, task_dir)

    def _ensure_cleanup_retry(
        self, key: str, task_id: str | None, path: Path
    ) -> None:
        """为待清理目录注册后台重试协程（幂等）。"""
        if key in self._retry_tasks:
            return
        if any(k == key for k, _tid, _p in self._retry_pending):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 同步恢复路径（无事件循环）：先登记，resume 时启动
            self._retry_pending.append((key, task_id, path))
            return
        self._retry_tasks[key] = loop.create_task(
            self._cleanup_retry_loop(key, task_id, path),
            name=f"cleanup-retry-{key}",
        )

    async def _cleanup_retry_loop(
        self, key: str, task_id: str | None, path: Path
    ) -> None:
        """周期性重试清理；全部清除后落对应终态并释放名额（一次）。"""
        try:
            while True:
                await asyncio.sleep(config.CLEANUP_RETRY_INTERVAL_SECONDS)
                if task_id is not None:
                    record = await self.get(task_id)
                    if (
                        record is None
                        or record["status"] != TaskStatus.CLEANUP_PENDING.value
                    ):
                        return
                ok, err = await self._try_cleanup(path)
                if not ok:
                    if task_id is not None:
                        try:
                            await self._update(task_id, cleanup_error=err)
                        except KeyError:
                            return
                    continue
                if task_id is None:
                    return  # 无主暂存目录清完即可
                record = await self.get(task_id)
                if (
                    record is None
                    or record["status"] != TaskStatus.CLEANUP_PENDING.value
                ):
                    return
                # 全部暂存实际清除：落对应终态并释放名额（恰好一次）
                final = (
                    record.get("pending_final_status")
                    or TaskStatus.FAILED.value
                )
                await self._update(
                    task_id,
                    status=final,
                    finished_at=_now_iso(),
                    pending_final_status=None,
                    cleanup_error=None,
                )
                await self._release_slot_once(task_id)
                return
        finally:
            self._retry_tasks.pop(key, None)

    async def resume_cleanup_retries(self) -> None:
        """启动恢复阶段（无事件循环时）登记的全部清理重试。"""
        pending, self._retry_pending = self._retry_pending, []
        for key, task_id, path in pending:
            self._ensure_cleanup_retry(key, task_id, path)
        # 防御：确保所有 cleanup_pending 记录都有重试协程
        async with self._lock:
            ids = [
                r["task_id"]
                for r in self._tasks.values()
                if r["status"] == TaskStatus.CLEANUP_PENDING.value
            ]
        for task_id in ids:
            self._ensure_cleanup_retry(
                task_id, task_id, config.STAGING_DIR / task_id
            )

    async def shutdown(self) -> None:
        """停止全部清理重试协程（服务关闭时调用）。"""
        tasks = list(self._retry_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # ---------- 终态路径 ----------

    async def _publish_results(
        self, task_id, task_dir, rows_info, cached, recalculated
    ) -> None:
        samples = []
        counts = {
            "total": len(rows_info),
            "unchanged": 0,
            "changed": 0,
            "pass_to_fail": 0,
            "fail_to_pass": 0,
            "cache_unknown": 0,
        }
        for row_idx, sample_id, _cell in rows_info:
            c = cached.get(row_idx)
            r = recalculated[row_idx]
            if c is None:
                change_type = "cache_unknown"
                changed = True
            elif c == r:
                change_type = "unchanged"
                changed = False
            elif c == "PASS" and r == "FAIL":
                change_type = "pass_to_fail"
                changed = True
            elif c == "FAIL" and r == "PASS":
                change_type = "fail_to_pass"
                changed = True
            else:
                change_type = "unchanged"
                changed = False
            counts[change_type] += 1
            samples.append(
                {
                    "row": row_idx,
                    "sample_id": sample_id,
                    "cached_verdict": c,
                    "recalculated_verdict": r,
                    "changed": changed,
                    "change_type": change_type,
                }
            )

        summary = {
            "total": len(rows_info),
            "unchanged": counts["unchanged"],
            "changed": (
                counts["pass_to_fail"]
                + counts["fail_to_pass"]
                + counts["cache_unknown"]
            ),
            "pass_to_fail": counts["pass_to_fail"],
            "fail_to_pass": counts["fail_to_pass"],
            "cache_unknown": counts["cache_unknown"],
            "drift_pass_to_fail": counts["pass_to_fail"],
        }
        # 先发布（持久化）结果，再统一收尾（清理+终态+释放名额）；
        # 若清理失败转入待清理，已发布的结果不受影响
        await self._update(task_id, samples=samples, summary=summary)
        await self._finalize(
            task_id, task_dir, final_status=TaskStatus.COMPLETED.value
        )

    async def _finish_failed(
        self, task_id: str, task_dir: Path, code: str, message: str
    ) -> None:
        # 此时进程已退出（超时/失败路径均已 kill/wait）；统一收尾
        await self._finalize(
            task_id,
            task_dir,
            final_status=TaskStatus.FAILED.value,
            error_code=code,
            error_message=message,
        )

    async def _finish_cancelled(self, task_id: str, task_dir: Path) -> None:
        # 进程已退出；统一收尾（清理成功才落 CANCELLED 终态）
        await self._finalize(
            task_id,
            task_dir,
            final_status=TaskStatus.CANCELLED.value,
            error_code="CANCELLED",
            error_message="任务已取消，结果不发布",
        )
