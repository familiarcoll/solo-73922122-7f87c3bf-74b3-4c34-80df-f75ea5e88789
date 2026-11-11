"""核对计算子进程。

父进程通过标准输入传入一条已校验的请求 JSON，本进程在同一独立子进程中调用
纯函数 ``solve``，并把完整结果 JSON 写到标准输出。成功时退出码为 0；任何异常
都会写入标准错误并以非 0 退出，父进程因此不会拿到部分结果。

该模块不启动服务、不读取环境变量中的敏感信息，只用于被 ``app.execution`` 调用。
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time


def _start_parent_guard(fd: int) -> None:
    """父进程存活看门狗。

    父进程持有控制管道的写端；父进程正常结束、超时/断连主动关闭写端、或工作
    进程被强制杀死时，内核都会关闭写端，这里读到 EOF 后立即 ``os._exit``。
    这种“自行退出”不依赖向子进程发送信号，因此终止是确定性的；``os._exit``
    会直接结束整个进程（包括正在进行计算的主线程），不会产生部分结果。
    """

    def _guard() -> None:
        try:
            while os.read(fd, 4096):
                pass
        except OSError:
            pass
        os._exit(137)

    threading.Thread(target=_guard, name="parent-guard", daemon=True).start()


def _demo_delay() -> float:
    """仅供演示 / 测试的超时场景：在计算前额外等待若干秒，默认 0。"""
    raw = os.environ.get("RECONCILE_DEMO_DELAY_SECONDS", "0")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return value if value > 0 else 0.0


def main() -> int:
    try:
        if len(sys.argv) > 1:
            _start_parent_guard(int(sys.argv[1]))

        payload = json.loads(sys.stdin.buffer.read() or b"{}")
        delay = _demo_delay()
        if delay:
            time.sleep(delay)

        # 延迟导入，避免父进程仅做配置解析时加载求解器。
        from .schemas import ReconcileRequest
        from .solver import solve

        request = ReconcileRequest.model_validate(payload)
        result = solve(request.settlements, request.bank_entries)
        sys.stdout.write(json.dumps(result, ensure_ascii=False))
        sys.stdout.flush()
        return 0
    except BaseException as exc:  # noqa: BLE001 - 子进程边界，统一转为非 0 退出
        try:
            sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
            sys.stderr.flush()
        except Exception:
            pass
        return 1


if __name__ == "__main__":
    sys.exit(main())
