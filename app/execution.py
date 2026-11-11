"""核对执行层：跨进程并发名额 + 可终止的独立计算子进程。

并发模型
========
Docker Compose 启动两个 API 工作进程（``uvicorn --workers 2``）。两个工作进程通过
``flock`` 对固定数量的名额文件加排他锁，从而共享同一份并发额度（默认 2，可用
``RECONCILE_SLOTS`` 配置）。拿不到任何名额时立即返回 ``None``（API 层转成 503），
不做排队等待。

名额与子进程的绑定
==================
名额锁的 fd 会通过 ``pass_fds`` 继承给计算子进程。``flock`` 锁属于“打开文件描述”，
父进程意外退出时其 fd 关闭，但子进程仍持有同一描述，因此**锁不会提前释放**；
只有子进程真正退出、所有 fd 关闭后，名额才重新可用。这同时满足了：

* 工作进程异常退出后，名额不会因为父进程消失而被提前复用；
* 子进程通过控制管道看门狗 ``os._exit`` 立即退出（不依赖 kill 信号），并且还
  叠加了 ``PR_SET_PDEATHSIG(SIGKILL)`` 作为内核级兜底，名额不会永久丢失。

父进程持有一根控制管道的写端；超时或客户端断连时关闭写端，子进程内看门狗线程
读到 EOF 后执行 ``os._exit(137)``，无论主线程当时在计算还是 sleep 都能立即结束。
工作进程被强制杀死时内核也会关闭该写端，效果相同；子进程以 ``start_new_session``
运行，另有 SIGKILL 升级路径作为受限环境之外的兜底。
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_BASE_DIR = Path(__file__).resolve().parent.parent
_WORKER_MODULE = "app.worker"

_DEFAULT_TIMEOUT_SECONDS = 5.0
_DEFAULT_SLOTS = 2
_DEFAULT_SLOT_DIR = "/tmp/reconcile-slots"

_PDEATHSIG = 1  # Linux PR_SET_PDEATHSIG
try:  # pragma: no cover - 仅在没有 ctypes 的平台上退化
    import ctypes

    _LIBC = ctypes.CDLL("libc.so.6", use_errno=True)
except Exception:  # pragma: no cover
    _LIBC = None


# --------------------------------------------------------------------------- 配置


def timeout_seconds() -> float:
    """读取超时秒数（``RECONCILE_TIMEOUT_SECONDS``，默认 5 秒）。"""
    raw = os.environ.get("RECONCILE_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT_SECONDS))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else _DEFAULT_TIMEOUT_SECONDS


def concurrency_limit() -> int:
    """读取全局并发名额数（``RECONCILE_SLOTS``，默认 2）。"""
    raw = os.environ.get("RECONCILE_SLOTS", str(_DEFAULT_SLOTS))
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return _DEFAULT_SLOTS
    return value if value > 0 else _DEFAULT_SLOTS


def _slot_dir() -> Path:
    directory = Path(os.environ.get("RECONCILE_SLOT_DIR", _DEFAULT_SLOT_DIR))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


# ------------------------------------------------------------------ 进程元数据


def _proc_info(pid: int) -> tuple[str, str] | None:
    """返回 ``/proc/<pid>/stat`` 中的 (状态, 启动时间) ；进程不存在时返回 None。"""
    try:
        data = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        # comm 字段可能包含空格/括号，从最后一个 ')' 之后再切分。
        rest = data.rsplit(")", 1)[1].split()
        return rest[0], rest[19]  # state 是字段 3，starttime 是字段 22
    except (OSError, IndexError, ValueError):
        return None


def _process_matches(pid: Any, expected_start: Any = None) -> bool:
    """pid 存在、不是僵尸，且（若提供）启动时间一致。"""
    if not isinstance(pid, int) or pid <= 0:
        return False
    info = _proc_info(pid)
    if info is None:
        return False
    state, start = info
    if state == "Z":
        return False
    if expected_start is not None and str(start) != str(expected_start):
        return False
    return True


def _read_meta(fd: int) -> dict[str, Any]:
    try:
        raw = os.pread(fd, 4096, 0)
        if not raw:
            return {}
        value = json.loads(raw.decode("utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _write_meta(fd: int, meta: dict[str, Any]) -> None:
    data = json.dumps(meta, separators=(",", ":")).encode("utf-8")
    os.ftruncate(fd, 0)
    os.pwrite(fd, data, 0)


# --------------------------------------------------------------------- 名额租约


@dataclass
class SlotLease:
    """一个已取得的名额。``release`` 只能在对应子进程退出之后调用。"""

    index: int
    path: Path
    fd: int
    acquired_at: float
    _released: bool = field(default=False, init=False)

    def publish_child(self, pid: int) -> None:
        """把子进程 pid / 启动时间写入名额文件，便于崩溃后回收。"""
        _write_meta(
            self.fd,
            {
                "owner_pid": os.getpid(),
                "owner_start": (_proc_info(os.getpid()) or (None, None))[1],
                "child_pid": pid,
                "child_start": (_proc_info(pid) or (None, None))[1],
                "acquired_at": self.acquired_at,
            },
        )

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            # 先清空元数据再解锁；即使这里异常，close 仍会释放 flock。
            try:
                os.ftruncate(self.fd, 0)
            except OSError:
                pass
            try:
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            except OSError:
                pass
        finally:
            try:
                os.close(self.fd)
            except OSError:
                pass


class SlotManager:
    """基于 flock 的跨进程名额管理器（不排队）。"""

    def try_acquire(self) -> SlotLease | None:
        directory = _slot_dir()
        for index in range(concurrency_limit()):
            lease = self._acquire_one(directory / f"slot-{index}.lock", index)
            if lease is not None:
                return lease
        return None

    def _acquire_one(self, path: Path, index: int) -> SlotLease | None:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        success = False
        try:
            acquired = self._lock(fd)
            if not acquired:
                # 名额被占用。若持有者工作进程已死，子进程仍可能通过继承的 fd
                # 持有锁；此时主动终止孤儿子进程并做一次很短的回收等待。
                meta = _read_meta(fd)
                if not self._is_orphaned(meta):
                    return None
                self._kill_orphan(meta)
                deadline = time.monotonic() + 0.5
                while time.monotonic() < deadline:
                    acquired = self._lock(fd)
                    if acquired:
                        break
                    time.sleep(0.02)
                if not acquired:
                    return None

            lease = SlotLease(
                index=index,
                path=path,
                fd=fd,
                acquired_at=time.monotonic(),
            )
            # 取得名额的瞬间开始计时；随后写入本进程的租约元数据。
            _write_meta(
                lease.fd,
                {
                    "owner_pid": os.getpid(),
                    "owner_start": (_proc_info(os.getpid()) or (None, None))[1],
                    "acquired_at": lease.acquired_at,
                },
            )
            success = True
            return lease
        finally:
            if not success:
                try:
                    os.close(fd)
                except OSError:
                    pass

    @staticmethod
    def _lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    @staticmethod
    def _is_orphaned(meta: dict[str, Any]) -> bool:
        owner_pid = meta.get("owner_pid")
        if not isinstance(owner_pid, int):
            return False
        # 元数据来自已退出（或 pid 已被复用）的工作进程。
        return not _process_matches(owner_pid, meta.get("owner_start"))

    @staticmethod
    def _kill_orphan(meta: dict[str, Any]) -> None:
        child_pid = meta.get("child_pid")
        if not _process_matches(child_pid, meta.get("child_start")):
            return
        logger.warning("回收孤儿核对子进程 pid=%s（工作进程已退出）", child_pid)
        try:
            os.killpg(child_pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError, OSError):
            try:
                os.kill(child_pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError, OSError):
                pass


# --------------------------------------------------------------- 子进程执行


def _make_preexec(expected_parent: int):
    """在 exec 前设置 PDEATHSIG，并处理父进程已先退出的竞态。"""

    def _preexec() -> None:  # pragma: no cover - 在 fork 后的子进程内运行
        if _LIBC is not None:
            try:
                _LIBC.prctl(_PDEATHSIG, signal.SIGKILL, 0, 0, 0)
            except Exception:
                pass
        if os.getppid() != expected_parent:
            os._exit(127)

    return _preexec


def _signal_group(pid: int, sig: signal.Signals) -> None:
    try:
        pgid = os.getpgid(pid)
    except (ProcessLookupError, PermissionError, OSError):
        pgid = pid
    try:
        os.killpg(pgid, sig)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            os.kill(pid, sig)
        except (ProcessLookupError, PermissionError, OSError):
            pass


async def _watch(
    communicate: "asyncio.Task[tuple[bytes, bytes]]",
    deadline: float,
    request: Any,
) -> str | None:
    """等待子进程结束；返回 None 表示正常结束，否则为 ``timeout`` / ``disconnect``。"""
    while True:
        if communicate.done():
            return None
        now = time.monotonic()
        if now >= deadline:
            return "timeout"
        try:
            disconnected = await request.is_disconnected()
        except Exception:
            disconnected = False
        if disconnected:
            return "disconnect"
        await asyncio.sleep(min(0.05, max(0.001, deadline - now)))


class _ControlPipe:
    """父子进程控制管道：关闭写端即让子进程看门狗立即 ``os._exit``。

    该机制不依赖向子进程发送信号，因此在受限环境（例如 seccomp 拒绝 kill）
    或工作进程被 SIGKILL 时，子进程也能确定性地自行退出。
    """

    def __init__(self) -> None:
        self._read_fd: int | None
        self._write_fd: int | None
        self._read_fd, self._write_fd = os.pipe()

    @property
    def read_fd(self) -> int:
        assert self._read_fd is not None
        return self._read_fd

    def close_read(self) -> None:
        if self._read_fd is not None:
            try:
                os.close(self._read_fd)
            except OSError:
                pass
            self._read_fd = None

    def close_write(self) -> None:
        if self._write_fd is not None:
            try:
                os.close(self._write_fd)
            except OSError:
                pass
            self._write_fd = None


async def _stop(
    proc: subprocess.Popen[bytes],
    communicate: "asyncio.Task[Any]",
    control: _ControlPipe,
) -> None:
    """终止计算并等到子进程真正退出（总耗时控制在 1 秒以内）。"""
    # 关闭控制管道写端 -> 子进程内看门狗读到 EOF -> os._exit(137)。
    control.close_write()
    if proc.poll() is not None:
        return
    try:
        await asyncio.wait_for(asyncio.shield(communicate), timeout=0.7)
        return
    except Exception:
        pass
    # 兜底：在允许发送信号的环境中直接升级为 SIGKILL。
    _signal_group(proc.pid, signal.SIGKILL)
    try:
        await asyncio.wait_for(asyncio.shield(communicate), timeout=0.2)
    except Exception:
        logger.error("核对子进程 pid=%s 在终止后仍未退出", proc.pid)


async def execute_reconcile(
    slot: SlotLease,
    payload: dict[str, Any],
    timeout: float,
    request: Any,
) -> tuple[str, Any]:
    """在独立子进程中执行一次核对。

    返回 ``(status, value)``：

    * ``("ok", result_dict)``：完整成功结果；
    * ``("timeout", None)``：超时，已终止子进程；
    * ``("disconnect", None)``：客户端断开，已终止子进程；
    * ``("error", message)``：子进程启动 / 退出异常。

    ``finally`` 中总是等子进程退出后才释放名额。
    """
    deadline = slot.acquired_at + timeout
    input_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    proc: subprocess.Popen[bytes] | None = None
    communicate: "asyncio.Task[tuple[bytes, bytes]]" | None = None
    control = _ControlPipe()

    try:
        try:
            proc = subprocess.Popen(
                # 控制管道读端作为 argv 传入，子进程启动看门狗线程后阻塞读取。
                [sys.executable, "-m", _WORKER_MODULE, str(control.read_fd)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(_BASE_DIR),
                start_new_session=True,
                # 名额锁 fd + 控制管道读端由子进程继承。
                pass_fds=(slot.fd, control.read_fd),
                preexec_fn=_make_preexec(os.getpid()),
                close_fds=True,
            )
        except OSError as exc:
            logger.exception("无法启动核对子进程")
            return "error", f"无法启动核对子进程: {exc}"
        finally:
            # 写端只留在父进程；读端已复制给子进程。
            control.close_read()

        slot.publish_child(proc.pid)
        communicate = asyncio.create_task(asyncio.to_thread(proc.communicate, input_bytes))

        reason = await _watch(communicate, deadline, request)
        if reason is not None:
            await _stop(proc, communicate, control)
            return reason, None

        stdout, stderr = communicate.result()
        if proc.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace").strip()
            logger.warning("核对子进程异常退出 rc=%s: %s", proc.returncode, detail[:500])
            return "error", detail[:500] or "核对子进程异常退出"

        try:
            result = json.loads(stdout.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("核对子进程输出无法解析: %s", exc)
            return "error", "核对子进程输出无法解析"

        return "ok", result
    finally:
        # 兜底：任务被取消等非常规路径下，也必须先终止并等待子进程退出，
        # 再释放名额。关闭控制管道写端是最可靠、无需信号的终止方式。
        control.close_write()
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=0.5)
            except subprocess.TimeoutExpired:
                _signal_group(proc.pid, signal.SIGKILL)
                try:
                    proc.wait(timeout=0.4)
                except subprocess.TimeoutExpired:
                    logger.error("核对子进程 pid=%s 在终止后仍未退出", proc.pid)
        slot.release()


__all__ = [
    "SlotLease",
    "SlotManager",
    "concurrency_limit",
    "execute_reconcile",
    "timeout_seconds",
]
