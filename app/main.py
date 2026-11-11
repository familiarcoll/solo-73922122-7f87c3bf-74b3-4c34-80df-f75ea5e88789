"""FastAPI 应用入口：支付渠道日结对账服务。"""

from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse

from .execution import SlotManager, execute_reconcile, timeout_seconds
from .schemas import (
    MAX_DATE_DELTA_DAYS,
    MAX_RECORDS,
    MAX_SETTLEMENTS_PER_ENTRY,
    ReconcileRequest,
    ReconcileResponse,
)
from .solver import solve  # noqa: F401 - 公开给测试 / 其他调用方

logger = logging.getLogger(__name__)

BUSY_DETAIL = "对账服务繁忙：并发核对名额已满，请稍后重试"
TIMEOUT_DETAIL = "核对计算超时，本次结果已丢弃（不返回部分方案）"

app = FastAPI(
    title="支付渠道日结对账 API",
    description=(
        "核对外渠道日结单（扣费后应结金额）与银行贷记流水，求解全局匹配入账总"
        "金额最大的方案，并按「所有最优方案中的一致性」区分自动确认、人工核对"
        "与始终未匹配记录。\n\n"
        "## 匹配规则\n"
        f"* 一笔银行入账可匹配 1~{MAX_SETTLEMENTS_PER_ENTRY} 条**同币种**日结记录；\n"
        "* 日结合计金额（正整数分）必须与入账金额**严格相等**；\n"
        f"* 入账日须在每条结算日之后 0~{MAX_DATE_DELTA_DAYS} 个自然日内（含端点）；\n"
        "* 每条记录在一套方案中只能使用一次；\n"
        f"* 日结记录与银行流水各不超过 {MAX_RECORDS} 条。\n\n"
        "## 结果口径\n"
        "* `auto_confirm`：在**所有**最优方案中均对应同一日结组的入账；\n"
        "* `manual_review`：不同最优方案中对应不同日结组的入账（歧义）；\n"
        "* `never_matched`：在任何最优方案中始终未被匹配的记录；\n"
        "* 存在歧义时 `solutions` 返回两套不同的完整最优方案。\n\n"
        "## 执行与资源限制\n"
        "* 服务以 **2 个 API 工作进程**运行，两个进程共享 **2 个**核对计算名额；\n"
        "* 每次核对在**可终止的独立子进程**中执行，不在 API 事件循环里做重计算，"
        "因此计算期间 `/health` 仍可响应；\n"
        "* 名额已满时**立即**返回 `503`，不排队；\n"
        "* 超时秒数由环境变量 `RECONCILE_TIMEOUT_SECONDS` 配置（默认 **5 秒**），"
        "从**取得名额**起计时；超时立即终止子进程并返回 `504`，不返回部分方案；\n"
        "* 客户端断连或 API 工作进程异常退出时，对应计算子进程会在 **1 秒内**被终止；"
        "名额仅在子进程真正退出后才释放。\n\n"
        "完整规则、环境变量与演示命令见项目 README。"
    ),
    version="1.1.0",
)

_slot_manager = SlotManager()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/health", tags=["运维"], summary="健康检查")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/api/v1/reconcile",
    response_model=ReconcileResponse,
    tags=["对账"],
    summary="求解全局最优对账匹配",
    description=(
        "在独立子进程中执行一次精确求解。请求与成功响应格式保持不变。\n\n"
        "* `503`：全局 2 个计算名额均被占用（立即返回，不排队）；\n"
        "* `504`：从取得名额起超过 `RECONCILE_TIMEOUT_SECONDS`（默认 5 秒），"
        "子进程已被终止，不返回部分结果。"
    ),
    responses={
        422: {"description": "输入校验失败（编号重复、金额非正整数、超出条数等）"},
        503: {
            "description": "并发核对名额已满（不排队），可稍后重试",
            "content": {
                "application/json": {
                    "example": {"detail": BUSY_DETAIL}
                }
            },
        },
        504: {
            "description": "核对计算超时，已终止且不返回部分结果",
            "content": {
                "application/json": {
                    "example": {"detail": TIMEOUT_DETAIL}
                }
            },
        },
    },
)
async def reconcile(payload: ReconcileRequest, request: Request) -> ReconcileResponse:
    # 请求体已由 FastAPI / Pydantic 校验；序列化后交给独立子进程。
    payload_json = payload.model_dump(mode="json")
    configured_timeout = timeout_seconds()

    # 名额获取只在文件锁上做非阻塞尝试，不排队。放到线程里避免极端的
    # 孤儿名额回收等待阻塞事件循环。
    slot = await asyncio.to_thread(_slot_manager.try_acquire)
    if slot is None:
        raise HTTPException(
            status_code=503,
            detail=BUSY_DETAIL,
            headers={"Retry-After": "1"},
        )

    status, value = await execute_reconcile(
        slot, payload_json, configured_timeout, request
    )

    if status == "timeout":
        raise HTTPException(
            status_code=504,
            detail=f"{TIMEOUT_DETAIL}（超时阈值 {configured_timeout:g} 秒）",
            headers={"Retry-After": "1"},
        )
    if status == "disconnect":
        # 客户端已经离开，响应通常不会被读取；仍返回明确的 499 语义状态。
        return JSONResponse(
            status_code=499,
            content={"detail": "客户端已断开，核对计算已终止"},
        )
    if status != "ok":
        logger.warning("核对执行失败: %s", value)
        raise HTTPException(
            status_code=500,
            detail="核对计算执行失败，请稍后重试",
        )

    return ReconcileResponse(**value)
