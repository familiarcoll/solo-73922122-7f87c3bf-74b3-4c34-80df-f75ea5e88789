# 轻量运行镜像：slim 基础镜像 + 纯 Python 依赖，无需编译工具链
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    RECONCILE_TIMEOUT_SECONDS=5 \
    RECONCILE_SLOTS=2 \
    RECONCILE_SLOT_DIR=/tmp/reconcile-slots

WORKDIR /srv

# 先装依赖，利用 Docker 层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 再拷应用代码
COPY app ./app

# 非 root 用户运行
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /srv
USER appuser

EXPOSE 8000

# 容器内健康检查：uvicorn 起来后探测 /health
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"

# 两个 API 工作进程共享 2 个核对名额（名额由 app/execution.py 的跨进程文件锁管理）
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
