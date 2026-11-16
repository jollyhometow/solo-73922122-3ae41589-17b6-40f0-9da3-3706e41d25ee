"""暂存清理失败（如父目录权限被收回）时的收尾语义。

覆盖：
- 取消/超时后删除失败 → cleanup_pending（可查询、保留原因、不发布判定、
  不释放名额）；存储恢复后自动续清，全部清除才落对应终态并释放一次名额；
- 排队取消、正常完成路径的清理失败同样转入待清理；
- 待清理批次占满名额时新批次排队，清理成功后自动推进；
- 服务重启接续待清理批次：不重新重算、不遗忘原收尾原因；
- 清理不擅改管理员设置的父目录权限（真实文件系统权限，非 root 时）。
"""

from __future__ import annotations

import asyncio
import json
import os
import stat

import pytest

from app.recalc import RecalcCancelled, RecalcTimeout
from app.recalc import cleanup_task_dir as real_cleanup_task_dir


# ---------- 测试工具 ----------


def _wb_bytes(samples=(("S001", 1.0),)):
    """构造合法工作簿（标定表 + 样本表，B 列为公式）。"""
    import io

    import openpyxl

    wb = openpyxl.Workbook()
    wb.active.title = "标定表"
    ws = wb.create_sheet("样本表")
    ws["A1"] = "样本号"
    ws["B1"] = "判定"
    for i, (sid, val) in enumerate(samples, start=2):
        ws.cell(row=i, column=1, value=sid)
        ws.cell(row=i, column=2, value='=IF(1=1,"PASS","FAIL")')
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class CleanupBlocker:
    """可开关的清理故障注入：blocked 期间抛 PermissionError。"""

    def __init__(self):
        self.blocked = True
        self.attempts = 0

    def __call__(self, path):
        self.attempts += 1
        if self.blocked:
            raise PermissionError(
                f"[Errno 13] Permission denied (simulated): {path}"
            )
        return real_cleanup_task_dir(path)


async def fake_recalc_wait(task_dir, cancel_event=None):
    """模拟重算进行中，直到收到取消。"""
    while True:
        await asyncio.sleep(0.02)
        if cancel_event is not None and cancel_event.is_set():
            raise RecalcCancelled("任务已取消")


async def fake_recalc_timeout(task_dir, cancel_event=None):
    """模拟重算超时。"""
    raise RecalcTimeout("重算超过 10 秒")


async def fake_recalc_ok(task_dir, cancel_event=None):
    """模拟重算成功：产出 B 列写死 PASS 的“重算输出”。"""
    import openpyxl

    wb = openpyxl.load_workbook(task_dir / "input.xlsx")
    ws = wb["样本表"]
    for row in ws.iter_rows(min_row=2, min_col=2, max_col=2):
        row[0].value = "PASS"
    out = task_dir / "output" / "input.xlsx"
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out


async def _wait_status(mgr, task_id, want, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        rec = await mgr.get(task_id)
        if rec is not None and rec["status"] == want:
            return rec
        await asyncio.sleep(0.02)
    rec = await mgr.get(task_id)
    raise AssertionError(
        f"任务 {task_id} 未在 {timeout}s 内变为 {want}，"
        f"当前 {rec['status'] if rec else None}"
    )


async def _wait_dir_gone(path, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if not path.exists():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"目录未在 {timeout}s 内清除：{path}")


@pytest.fixture()
def mgr_env(monkeypatch, tmp_path):
    from app import config

    data_dir = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "TASKS_DIR", data_dir / "tasks")
    monkeypatch.setattr(config, "STAGING_DIR", data_dir / "staging")
    monkeypatch.setattr(config, "CLEANUP_RETRY_INTERVAL_SECONDS", 0.05)
    (data_dir / "tasks").mkdir(parents=True)
    (data_dir / "staging").mkdir(parents=True)
    return data_dir


@pytest.fixture()
async def make_manager(mgr_env):
    from app.manager import TaskManager

    created = []

    def _make():
        mgr = TaskManager()
        created.append(mgr)
        return mgr

    yield _make
    for mgr in created:
        await mgr.shutdown()


def _base_record(task_id, **overrides):
    record = {
        "task_id": task_id,
        "status": "running",
        "filename": "w.xlsx",
        "size_bytes": 4,
        "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": "2026-01-01T00:00:01+00:00",
        "finished_at": None,
        "error_code": None,
        "error_message": None,
        "cancel_requested": False,
        "pending_final_status": None,
        "cleanup_error": None,
        "samples": None,
        "summary": None,
    }
    record.update(overrides)
    return record


def _plant_record(data_dir, record, with_staging=True):
    tasks_dir = data_dir / "tasks"
    tasks_dir.mkdir(parents=True, exist_ok=True)
    (tasks_dir / f"{record['task_id']}.json").write_text(
        json.dumps(record, ensure_ascii=False), encoding="utf-8"
    )
    if with_staging:
        staging = data_dir / "staging" / record["task_id"]
        staging.mkdir(parents=True, exist_ok=True)
        (staging / "input.xlsx").write_bytes(b"original-bytes")


# ---------- 运行中取消 + 删除失败 ----------


@pytest.mark.asyncio
async def test_cancel_running_cleanup_failure_then_resume(
    make_manager, monkeypatch, mgr_env
):
    from app import config
    from app import manager as manager_mod

    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)
    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_wait)

    mgr = make_manager()
    rec = await mgr.create_task(_wb_bytes(), "w.xlsx")
    tid = rec["task_id"]
    job = mgr._jobs[tid]
    await _wait_status(mgr, tid, "running")

    _got, accepted = await mgr.request_cancel(tid)
    assert accepted is True

    # 删除失败：保持可查询的待清理状态，保留取消原因，不发布判定
    pending = await _wait_status(mgr, tid, "cleanup_pending")
    assert pending["error_code"] == "CANCELLED"
    assert pending["pending_final_status"] == "cancelled"
    assert pending["samples"] is None
    assert pending["summary"] is None
    assert pending["finished_at"] is None  # 未到终态
    assert "PermissionError" in (pending["cleanup_error"] or "")
    # 不释放该批名额
    assert mgr.slots.in_use == 1
    # 原件与目录确实残留（尚未清退）
    assert (config.STAGING_DIR / tid / "input.xlsx").exists()
    # 待清理状态不可再取消
    _rec, again = await mgr.request_cancel(tid)
    assert again is False

    await asyncio.wait_for(job, timeout=5)  # 工作协程收尾完毕
    assert blocker.attempts >= 1

    # 存储恢复：自动续清，全部清除后才落 cancelled 并释放名额一次
    blocker.blocked = False
    done = await _wait_status(mgr, tid, "cancelled")
    assert done["error_code"] == "CANCELLED"
    assert done["error_message"]
    assert done["pending_final_status"] is None
    assert done["cleanup_error"] is None
    assert done["finished_at"]
    assert done["samples"] is None
    assert mgr.slots.in_use == 0
    assert not (config.STAGING_DIR / tid).exists()
    # 名额只释放一次：不会继续下降
    await asyncio.sleep(0.1)
    assert mgr.slots.in_use == 0


# ---------- 超时 + 删除失败 ----------


@pytest.mark.asyncio
async def test_timeout_cleanup_failure_keeps_reason(
    make_manager, monkeypatch, mgr_env
):
    from app import config
    from app import manager as manager_mod

    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)
    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_timeout)

    mgr = make_manager()
    rec = await mgr.create_task(_wb_bytes(), "w.xlsx")
    tid = rec["task_id"]
    job = mgr._jobs[tid]

    # 超时批次删除失败：待清理并保留超时原因
    pending = await _wait_status(mgr, tid, "cleanup_pending")
    assert pending["error_code"] == "RECALC_TIMEOUT"
    assert "超过" in (pending["error_message"] or "")
    assert pending["pending_final_status"] == "failed"
    assert pending["samples"] is None
    assert mgr.slots.in_use == 1
    await asyncio.wait_for(job, timeout=5)

    # 存储恢复后落 failed 终态，原因仍是超时
    blocker.blocked = False
    done = await _wait_status(mgr, tid, "failed")
    assert done["error_code"] == "RECALC_TIMEOUT"
    assert done["samples"] is None
    assert mgr.slots.in_use == 0
    assert not (config.STAGING_DIR / tid).exists()


# ---------- 排队取消 + 删除失败 ----------


@pytest.mark.asyncio
async def test_queued_cancel_cleanup_failure(make_manager, monkeypatch, mgr_env):
    from app import config
    from app import manager as manager_mod

    mgr = make_manager()
    mgr.slots._capacity = 1
    await mgr.slots.acquire()  # 占住唯一名额，让新任务排队
    try:
        blocker = CleanupBlocker()
        monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

        rec = await mgr.create_task(_wb_bytes(), "q.xlsx")
        tid = rec["task_id"]
        await asyncio.sleep(0.05)
        assert (await mgr.get(tid))["status"] == "queued"

        got, accepted = await mgr.request_cancel(tid)
        assert accepted is True
        # request_cancel 已等收尾完成：清理失败 → 待清理
        assert got["status"] == "cleanup_pending"
        assert got["error_code"] == "CANCELLED"
        assert got["pending_final_status"] == "cancelled"
        # 排队任务从未占用重算名额（在用的仍是测试占住的那一个）
        assert mgr.slots.in_use == 1
        assert (config.STAGING_DIR / tid / "input.xlsx").exists()

        blocker.blocked = False
        done = await _wait_status(mgr, tid, "cancelled")
        assert done["error_code"] == "CANCELLED"
        assert not (config.STAGING_DIR / tid).exists()
        # 待清理的排队批次不额外占名额
        assert mgr.slots.in_use == 1
    finally:
        await mgr.slots.release()


# ---------- 名额被待清理占满时新批次排队，清理成功后自动推进 ----------


@pytest.mark.asyncio
async def test_slot_blocked_until_cleanup_then_queue_advances(
    make_manager, monkeypatch, mgr_env
):
    from app import config
    from app import manager as manager_mod

    mgr = make_manager()
    mgr.slots._capacity = 1  # 只留一个名额
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)
    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_wait)

    rec_a = await mgr.create_task(_wb_bytes(), "a.xlsx")
    a = rec_a["task_id"]
    await _wait_status(mgr, a, "running")

    rec_b = await mgr.create_task(_wb_bytes(), "b.xlsx")
    b = rec_b["task_id"]
    await asyncio.sleep(0.05)
    assert (await mgr.get(b))["status"] == "queued"

    # 取消 A：清理失败 → 待清理，名额仍被占，B 继续排队
    await mgr.request_cancel(a)
    await _wait_status(mgr, a, "cleanup_pending")
    assert mgr.slots.in_use == 1
    await asyncio.sleep(0.2)
    assert (await mgr.get(b))["status"] == "queued"  # 没有推进

    # B 启动前把重算换成成功版；随后恢复存储
    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_ok)
    blocker.blocked = False

    # A 续清成功 → cancelled → 释放名额 → B 自动推进直至完成
    await _wait_status(mgr, a, "cancelled")
    done_b = await _wait_status(mgr, b, "completed", timeout=10)
    assert done_b["samples"] is not None
    assert mgr.slots.in_use == 0
    assert not (config.STAGING_DIR / a).exists()
    assert not (config.STAGING_DIR / b).exists()


# ---------- 正常完成路径的清理失败 ----------


@pytest.mark.asyncio
async def test_completed_cleanup_failure_then_resume(
    make_manager, monkeypatch, mgr_env
):
    from app import config
    from app import manager as manager_mod

    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_ok)
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

    mgr = make_manager()
    rec = await mgr.create_task(_wb_bytes(), "w.xlsx")
    tid = rec["task_id"]

    # 结果已发布，但暂存未清退前不报 completed、不放名额
    pending = await _wait_status(mgr, tid, "cleanup_pending")
    assert pending["samples"] is not None  # 判定已发布（结果有效）
    assert pending["pending_final_status"] == "completed"
    assert mgr.slots.in_use == 1

    blocker.blocked = False
    done = await _wait_status(mgr, tid, "completed")
    assert done["samples"] is not None
    assert done["finished_at"]
    assert mgr.slots.in_use == 0
    assert not (config.STAGING_DIR / tid).exists()


# ---------- 重启接续待清理批次 ----------


@pytest.mark.asyncio
async def test_restart_resumes_cleanup_pending_storage_recovered(
    make_manager, mgr_env
):
    """重启时存储已恢复：立即落原终态，保留取消原因，不重新重算。"""
    tid = "pending01"
    _plant_record(
        mgr_env,
        _base_record(
            tid,
            status="cleanup_pending",
            error_code="CANCELLED",
            error_message="任务已取消，结果不发布",
            cancel_requested=True,
            pending_final_status="cancelled",
            cleanup_error="PermissionError: [Errno 13]",
        ),
    )

    mgr = make_manager()
    mgr.recover_on_startup()

    rec = await mgr.get(tid)
    assert rec["status"] == "cancelled"
    assert rec["error_code"] == "CANCELLED"  # 原收尾原因不遗忘
    assert rec["finished_at"]
    assert rec["pending_final_status"] is None
    assert not (mgr_env / "staging" / tid).exists()
    assert mgr.slots.in_use == 0
    # 没有启动任何重算协程
    assert not mgr._jobs


@pytest.mark.asyncio
async def test_restart_resumes_cleanup_pending_storage_still_broken(
    make_manager, monkeypatch, mgr_env
):
    """重启时存储仍未恢复：保持待清理、占住名额，恢复后自动续清。"""
    from app import manager as manager_mod

    tid = "pending02"
    _plant_record(
        mgr_env,
        _base_record(
            tid,
            status="cleanup_pending",
            error_code="RECALC_TIMEOUT",
            error_message="重算超过 10 秒",
            pending_final_status="failed",
            cleanup_error="PermissionError: [Errno 13]",
        ),
    )
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

    mgr = make_manager()
    mgr.recover_on_startup()
    await mgr.resume_cleanup_retries()

    rec = await mgr.get(tid)
    assert rec["status"] == "cleanup_pending"
    assert rec["error_code"] == "RECALC_TIMEOUT"  # 原因保留
    assert rec["pending_final_status"] == "failed"
    assert mgr.slots.in_use == 1  # 名额继续占用
    assert (mgr_env / "staging" / tid / "input.xlsx").exists()

    blocker.blocked = False
    done = await _wait_status(mgr, tid, "failed")
    assert done["error_code"] == "RECALC_TIMEOUT"
    assert mgr.slots.in_use == 0
    assert not (mgr_env / "staging" / tid).exists()


@pytest.mark.asyncio
async def test_restart_interrupted_cleanup_failure_becomes_pending(
    make_manager, monkeypatch, mgr_env
):
    """重启中断批次清理失败：转待清理（终态 interrupted 推迟到清除后）。"""
    from app import manager as manager_mod

    tid = "running01"
    _plant_record(mgr_env, _base_record(tid, status="running"))
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

    mgr = make_manager()
    mgr.recover_on_startup()
    await mgr.resume_cleanup_retries()

    rec = await mgr.get(tid)
    assert rec["status"] == "cleanup_pending"
    assert rec["error_code"] == "INTERRUPTED_BY_RESTART"
    assert rec["pending_final_status"] == "interrupted"
    assert mgr.slots.in_use == 1

    blocker.blocked = False
    done = await _wait_status(mgr, tid, "interrupted")
    assert done["error_code"] == "INTERRUPTED_BY_RESTART"
    assert mgr.slots.in_use == 0
    assert not (mgr_env / "staging" / tid).exists()


@pytest.mark.asyncio
async def test_restart_orphan_dir_cleanup_retried(
    make_manager, monkeypatch, mgr_env
):
    """无主暂存目录：启动时清理失败也注册重试，存储恢复后自动清除。"""
    from app import manager as manager_mod

    orphan = mgr_env / "staging" / "orphan-x"
    orphan.mkdir(parents=True)
    (orphan / "input.xlsx").write_bytes(b"x")
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

    mgr = make_manager()
    mgr.recover_on_startup()
    await mgr.resume_cleanup_retries()
    assert orphan.exists()

    blocker.blocked = False
    await _wait_dir_gone(orphan)


# ---------- HTTP 层：/docs、上传、查询、取消全流程 ----------


def test_http_docs_upload_query_cancel_cleanup_pending(monkeypatch, tmp_path):
    """不依赖 LibreOffice：假重算 + 故障注入，验证 HTTP 全流程。"""
    import time

    from fastapi.testclient import TestClient

    from app import config
    from app import manager as manager_mod

    data_dir = tmp_path / "http-data"
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(config, "TASKS_DIR", data_dir / "tasks")
    monkeypatch.setattr(config, "STAGING_DIR", data_dir / "staging")
    monkeypatch.setattr(config, "CLEANUP_RETRY_INTERVAL_SECONDS", 0.05)
    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_wait)
    blocker = CleanupBlocker()
    monkeypatch.setattr(manager_mod, "cleanup_task_dir", blocker)

    from app.main import app

    def _wait_http_status(client, tid, want, timeout=5.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            body = client.get(f"/tasks/{tid}").json()
            if body["status"] == want:
                return body
            time.sleep(0.02)
        raise AssertionError(f"HTTP 任务 {tid} 未变为 {want}：{body['status']}")

    with TestClient(app) as client:
        # /docs 与 OpenAPI 可用
        assert client.get("/docs").status_code == 200
        assert client.get("/openapi.json").status_code == 200

        # 上传
        resp = client.post(
            "/tasks",
            files={
                "file": (
                    "w.xlsx",
                    _wb_bytes(),
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet",
                )
            },
        )
        assert resp.status_code == 202, resp.text
        tid = resp.json()["task_id"]
        _wait_http_status(client, tid, "running")

        # 取消（清理将失败 → 待清理）
        resp = client.post(f"/tasks/{tid}/cancel")
        assert resp.status_code == 200
        assert resp.json()["cancel_requested"] is True

        # 查询到待清理：保留取消原因、不发布判定、名额未释放
        body = _wait_http_status(client, tid, "cleanup_pending")
        assert body["error_code"] == "CANCELLED"
        assert body["pending_final_status"] == "cancelled"
        assert body["samples"] is None
        health = client.get("/health").json()
        assert health["slots_in_use"] == 1
        assert health["cleanup_pending"] == 1

        # 待清理不可取消
        resp = client.post(f"/tasks/{tid}/cancel")
        assert resp.status_code == 200
        assert "不可取消" in resp.json()["message"]

        # 存储恢复 → 自动续清 → cancelled，名额释放
        blocker.blocked = False
        body = _wait_http_status(client, tid, "cancelled")
        assert body["error_code"] == "CANCELLED"
        health = client.get("/health").json()
        assert health["slots_in_use"] == 0
        assert health["cleanup_pending"] == 0
        assert not (config.STAGING_DIR / tid).exists()


# ---------- 不擅改管理员设置的父目录权限（真实文件系统） ----------


@pytest.mark.asyncio
async def test_cleanup_failure_with_real_permissions_no_chmod(
    make_manager, monkeypatch, mgr_env
):
    if os.geteuid() == 0:
        pytest.skip("root 下权限约束不生效，无法模拟权限丧失")
    from app import config
    from app import manager as manager_mod

    monkeypatch.setattr(manager_mod, "run_recalc", fake_recalc_wait)
    mgr = make_manager()
    rec = await mgr.create_task(_wb_bytes(), "w.xlsx")
    tid = rec["task_id"]
    task_dir = config.STAGING_DIR / tid
    await _wait_status(mgr, tid, "running")

    # 管理员收回任务暂存目录的写权限（原件因此无法删除）
    os.chmod(task_dir, 0o555)
    try:
        await mgr.request_cancel(tid)
        pending = await _wait_status(mgr, tid, "cleanup_pending")
        assert pending["error_code"] == "CANCELLED"
        assert mgr.slots.in_use == 1
        # 服务没有擅改管理员设置的目录权限
        mode = stat.S_IMODE(os.stat(task_dir).st_mode)
        assert mode == 0o555, f"目录权限被擅改：{oct(mode)}"
        # 原件与目录都残留，等待恢复
        assert (task_dir / "input.xlsx").exists()
    finally:
        # 管理员恢复权限
        os.chmod(task_dir, 0o755)

    # 自动续清：全部清除后落 cancelled 并释放名额
    done = await _wait_status(mgr, tid, "cancelled", timeout=10)
    assert done["error_code"] == "CANCELLED"
    assert mgr.slots.in_use == 0
    assert not (config.STAGING_DIR / tid).exists()
