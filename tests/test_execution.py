"""执行层测试：名额、超时、断连、工作进程崩溃后的名额回收。"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from fastapi.testclient import TestClient

from app.execution import SlotManager, execute_reconcile, timeout_seconds
from app.main import app

PAYLOAD = {
    "settlements": [
        {"id": "S001", "date": "2026-11-01", "currency": "CNY", "amount_cents": 1000}
    ],
    "bank_entries": [
        {"id": "B001", "date": "2026-11-02", "currency": "CNY", "amount_cents": 1000}
    ],
}


class _ConnectedRequest:
    async def is_disconnected(self) -> bool:
        return False


class _DisconnectingRequest:
    def __init__(self, after: float = 0.1) -> None:
        self._deadline = time.monotonic() + after

    async def is_disconnected(self) -> bool:
        return time.monotonic() >= self._deadline


def _configure(monkeypatch, tmp_path: Path, *, slots: int = 2, timeout: str = "5",
               delay: str = "0") -> None:
    monkeypatch.setenv("RECONCILE_SLOT_DIR", str(tmp_path / "slots"))
    monkeypatch.setenv("RECONCILE_SLOTS", str(slots))
    monkeypatch.setenv("RECONCILE_TIMEOUT_SECONDS", timeout)
    monkeypatch.setenv("RECONCILE_DEMO_DELAY_SECONDS", delay)


def test_slot_capacity_is_shared_and_released(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, slots=2)
    manager = SlotManager()
    first = manager.try_acquire()
    second = manager.try_acquire()
    assert first is not None and second is not None
    # 两个名额均已占用：不排队，立即返回 None。
    assert manager.try_acquire() is None

    first.release()
    third = manager.try_acquire()
    assert third is not None
    second.release()
    third.release()
    # 全部释放后可再次拿到两个名额。
    a = manager.try_acquire()
    b = manager.try_acquire()
    assert a is not None and b is not None
    a.release()
    b.release()


def test_timeout_config_from_env(monkeypatch):
    monkeypatch.setenv("RECONCILE_TIMEOUT_SECONDS", "0.25")
    assert timeout_seconds() == 0.25
    monkeypatch.setenv("RECONCILE_TIMEOUT_SECONDS", "not-a-number")
    assert timeout_seconds() == 5.0


def test_success_runs_in_child_and_returns_full_result(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="5")
    slot = SlotManager().try_acquire()
    assert slot is not None
    status, value = asyncio.run(
        execute_reconcile(slot, PAYLOAD, 5, _ConnectedRequest())
    )
    assert status == "ok"
    assert value["optimal_total_matched_cents"] == 1000
    assert value["is_unique"] is True
    # execute_reconcile 已释放名额。
    assert SlotManager().try_acquire() is not None


def test_timeout_terminates_child_and_returns_no_result(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="0.3", delay="5")
    slot = SlotManager().try_acquire()
    assert slot is not None
    started = time.monotonic()
    status, value = asyncio.run(
        execute_reconcile(slot, PAYLOAD, 0.3, _ConnectedRequest())
    )
    elapsed = time.monotonic() - started
    assert status == "timeout"
    assert value is None
    assert elapsed < 1.2  # 超时 + SIGTERM/SIGKILL 的终止时间
    # 名额已释放且可立即复用。
    extra = SlotManager().try_acquire()
    assert extra is not None
    extra.release()


def test_client_disconnect_terminates_within_one_second(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="30", delay="30")
    slot = SlotManager().try_acquire()
    assert slot is not None
    started = time.monotonic()
    status, value = asyncio.run(
        execute_reconcile(slot, PAYLOAD, 30, _DisconnectingRequest(after=0.1))
    )
    elapsed = time.monotonic() - started
    assert status == "disconnect"
    assert value is None
    assert elapsed < 1.0
    extra = SlotManager().try_acquire()
    assert extra is not None
    extra.release()


def test_worker_crash_does_not_leak_slot(tmp_path, monkeypatch):
    """工作进程被 SIGKILL 后，继承名额锁的子进程退出即释放名额。"""
    _configure(monkeypatch, tmp_path, slots=1, timeout="60", delay="30")
    slot_dir = tmp_path / "slots"

    helper = r"""
import asyncio
import os
import threading

from app.execution import SlotManager, execute_reconcile

# 通过 stdin（父进程持有的管道）模拟“工作进程被强制杀死”：父进程关闭写端后
# 本进程直接 os._exit，没有任何清理机会。
def _crash_guard():
    try:
        while os.read(0, 4096):
            pass
    except OSError:
        pass
    os._exit(1)

threading.Thread(target=_crash_guard, daemon=True).start()

class Req:
    async def is_disconnected(self):
        return False

PAYLOAD = {
    "settlements": [{"id": "S001", "date": "2026-11-01", "currency": "CNY", "amount_cents": 1000}],
    "bank_entries": [{"id": "B001", "date": "2026-11-02", "currency": "CNY", "amount_cents": 1000}],
}

async def main():
    lease = await asyncio.to_thread(SlotManager().try_acquire)
    await execute_reconcile(lease, PAYLOAD, 60, Req())

asyncio.run(main())
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", helper],
        cwd=str(Path(__file__).resolve().parent.parent),
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    child_pid = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        for meta_path in slot_dir.glob("slot-*.lock"):
            try:
                meta = json.loads(meta_path.read_text() or "{}")
            except json.JSONDecodeError:
                continue
            if meta.get("child_pid"):
                child_pid = int(meta["child_pid"])
                break
        if child_pid:
            break
        time.sleep(0.05)
    assert child_pid is not None, "helper 未能启动计算子进程"

    # 模拟 API 工作进程异常退出（没有任何清理机会）。
    proc.stdin.close()
    proc.wait(timeout=5)

    manager = SlotManager()
    started = time.monotonic()
    recovered = None
    while time.monotonic() - started < 2.0:
        recovered = manager.try_acquire()
        if recovered is not None:
            break
        time.sleep(0.05)
    assert recovered is not None, "工作进程退出后名额永久丢失"
    assert time.monotonic() - started < 1.5
    recovered.release()


def test_http_timeout_returns_504_without_partial_result(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="0.3", delay="5")
    client = TestClient(app)
    started = time.monotonic()
    resp = client.post("/api/v1/reconcile", json=PAYLOAD)
    assert resp.status_code == 504
    assert "detail" in resp.json()
    assert time.monotonic() - started < 2.0
    # 504 响应中不包含任何方案字段。
    assert "solutions" not in resp.json()


def test_http_503_when_all_slots_busy(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="10", delay="1.5")

    def slow_request() -> int:
        with TestClient(app) as client:
            return client.post("/api/v1/reconcile", json=PAYLOAD).status_code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(slow_request) for _ in range(2)]
        time.sleep(0.5)  # 等两个慢请求都占满名额
        with TestClient(app) as client:
            busy = client.post("/api/v1/reconcile", json=PAYLOAD)
        assert busy.status_code == 503
        assert busy.headers["retry-after"] == "1"
        assert "detail" in busy.json()
        assert all(f.result() == 200 for f in futures)


def test_health_responds_during_computation(tmp_path, monkeypatch):
    _configure(monkeypatch, tmp_path, timeout="10", delay="1.5")

    def slow_request() -> int:
        with TestClient(app) as client:
            return client.post("/api/v1/reconcile", json=PAYLOAD).status_code

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(slow_request)
        time.sleep(0.2)
        started = time.monotonic()
        with TestClient(app) as client:
            health = client.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}
        assert time.monotonic() - started < 0.5
        assert future.result() == 200
