# 支付渠道日结对账 API

核对外渠道**日结单**（扣费后应结金额）与**银行贷记流水**，防止合并打款被错误核销。
服务基于 FastAPI，提供精确的全局最优匹配求解、歧义检测与自动确认分类。

## 业务规则

| 规则 | 说明 |
| --- | --- |
| 匹配基数 | 一笔银行入账匹配 **1～3 条**同币种日结记录 |
| 金额 | 日结金额合计与入账金额**严格相等**，单位为正整数「分」，无浮点误差 |
| 日期 | 入账日在每条结算日之后 **0～3 个自然日**内（含 0 和 3） |
| 币种 | 必须同币种（币种码大小写不敏感，统一转大写处理） |
| 唯一性 | 每条日结记录 / 每条入账在一套方案中至多使用一次 |
| 规模 | 日结记录与银行流水各不超过 **12** 条 |

**优化目标**：最大化全局已匹配入账总金额（加权集合装载问题，n≤12 用位掩码 DP 精确求解）。

## 结果分类口径

- `auto_confirm`：在**所有**最优方案中都对应同一日结组的入账 → 可自动确认核销；
- `manual_review`：在不同最优方案中对应不同日结组的入账 → 有歧义，转人工核对，
  响应中列出该入账的全部候选日结组；
- `never_matched`：在任何最优方案中始终未被匹配的日结记录 / 入账，单列处理；
- `solutions`：唯一时返回 1 套最优方案；存在歧义时返回 **2 套不同的完整最优方案**。

## 执行层与并发控制

请求格式、匹配规则与成功响应（HTTP 200）完全保持不变；本次改造只替换
`/api/v1/reconcile` 的执行方式。

- **两个工作进程**：Docker Compose 以 `uvicorn --workers 2` 启动两个 API 工作进程。
- **全局两个名额**：两个进程通过 `/tmp/reconcile-slots` 下的 `flock` 文件锁共享
  `RECONCILE_SLOTS=2` 个计算名额，合计最多同时执行 **2** 个核对计算。
- **满额立即 503**：没有任何可用名额时立即返回 `503 Service Unavailable`，不排队；
  响应带 `Retry-After`。
- **独立子进程**：每次核对都启动一个独立 Python 子进程执行求解，API 事件循环
  不做重计算，因此核对期间 `/health` 仍可正常响应。
- **超时 504**：`RECONCILE_TIMEOUT_SECONDS` 默认 **5 秒**，从**取得名额**的瞬间开始
  计时。超时后立即终止子进程并返回 `504 Gateway Timeout`，**不返回部分方案**。
- **断连 / 工作进程退出**：客户端断连会被每 50 ms 轮询检测，父进程关闭控制管道
  写端后子进程内看门狗立即 `os._exit`；工作进程异常退出时内核也会关闭该写端，
  （并叠加 `PR_SET_PDEATHSIG(SIGKILL)` 兜底），对应计算在 1 秒内终止。
- **名额安全复用**：名额锁 fd 由子进程继承，只有子进程真正退出（fd 关闭）后锁才
  释放；若工作进程先退出，残留子进程会被新工作进程识别并回收，因此不会因工作
  进程退出而永久丢失名额。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RECONCILE_TIMEOUT_SECONDS` | `5` | 单次核对超时秒数（可为小数），从取得名额起计时 |
| `RECONCILE_SLOTS` | `2` | 跨工作进程的全局并发计算名额数 |
| `RECONCILE_SLOT_DIR` | `/tmp/reconcile-slots` | 工作进程共享的锁文件目录 |
| `RECONCILE_DEMO_DELAY_SECONDS` | `0` | **仅演示/测试用**：子进程计算前额外等待的秒数，正常部署保持 0 |

### 繁忙与超时响应

名额已满（不排队，立即可重试）：

```http
HTTP/1.1 503 Service Unavailable
Retry-After: 1
Content-Type: application/json

{"detail":"对账服务繁忙：并发核对名额已满，请稍后重试"}
```

超时（已终止子进程，不返回部分结果）：

```http
HTTP/1.1 504 Gateway Timeout
Retry-After: 1
Content-Type: application/json

{"detail":"核对计算超时，本次结果已丢弃（不返回部分方案）（超时阈值 0.5 秒）"}
```

## 目录结构

```
.
├── app/
│   ├── main.py        # FastAPI 入口、路由与交互式文档
│   ├── schemas.py     # Pydantic 请求/响应模型与输入校验
│   ├── solver.py      # 核心求解器（前后缀位掩码 DP + SOS + DFS）
│   ├── execution.py   # 跨进程名额管理、可终止子进程、超时/断连处理
│   └── worker.py      # 核对计算子进程入口（stdin JSON -> stdout JSON）
├── tests/
│   ├── test_solver.py    # 求解器与 API 单元/集成测试
│   ├── test_execution.py # 名额、超时、断连、子进程退出测试
│   └── fuzz_verify.py    # 暴力穷举参考实现的随机交叉验证（400 组）
├── examples/          # 样例请求与对应响应
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 方式一：Docker Compose 启动（推荐）

```bash
docker compose up --build
```

启动后：

- 交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
  —— 其中的 **POST /api/v1/reconcile** 即为可直接演示的核对入口（Try it out）；
- ReDoc 文档：<http://localhost:8000/redoc>
- OpenAPI Schema：<http://localhost:8000/openapi.json>
- 健康检查：<http://localhost:8000/health>

容器以非 root 用户运行，内置 `HEALTHCHECK`；`init: true` 会及时回收被终止的
核对子进程。停止服务：`docker compose down`。

### 演示繁忙（503）

用演示延迟让每次计算持续 2 秒，然后并发发起 3 个请求：

```bash
RECONCILE_DEMO_DELAY_SECONDS=2 docker compose up --build
```

```bash
# 两个终端各执行一次（占满 2 个名额）
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' -d @examples/request_unique.json
# 第三个请求立即得到 503，而不是排队
curl -i -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' -d @examples/request_unique.json
```

### 演示超时（504）

```bash
RECONCILE_TIMEOUT_SECONDS=0.5 RECONCILE_DEMO_DELAY_SECONDS=2 docker compose up --build
curl -i -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' -d @examples/request_unique.json
```

请求会在约 0.5 秒后返回 `504`；此时用 `docker top reconcile-api` 可确认没有残留的
`app.worker` 计算子进程。

### 演示断连与名额回收

```bash
RECONCILE_DEMO_DELAY_SECONDS=10 docker compose up --build
# 客户端 1 秒后主动断开
curl --max-time 1 -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' -d @examples/request_unique.json
# 约 1 秒内对应的 app.worker 子进程即被终止，名额随后可再次使用
docker top reconcile-api
```

即使计算期间（未超时），`curl http://localhost:8000/health` 也会立即返回
`{"status":"ok"}`。

## 方式二：本地 Python 启动

需要 Python 3.11+：

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# 单进程便于调试；要与 Compose 行为一致可加 --workers 2
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 2
```

## 样例请求

### 1. 唯一最优方案（自动确认 + 始终未匹配）

```bash
curl -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' \
  -d @examples/request_unique.json
```

要点：`BANK-CR-1001` 合并核销两条日结（10000+5800=15800）；`BANK-CR-1004`
因金额无法凑出而进入 `never_matched.bank_entries`。

### 2. 有歧义（返回两套不同最优方案，全部转人工）

```bash
curl -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' \
  -d @examples/request_ambiguous.json
```

两条 30000 分入账都可以由不同的「10000+20000」组合构成，`is_unique=false`，
响应包含两套不同的完整最优方案，两条入账均出现在 `manual_review` 中并列出候选组。

### 3. 边界场景（超日期窗口 / 币种不符 / 金额无法凑齐）

```bash
curl -X POST http://localhost:8000/api/v1/reconcile \
  -H 'Content-Type: application/json' \
  -d @examples/request_edge_cases.json
```

- `BANK-E2` 入账日距结算日 9 天，超出 3 天窗口；
- `BANK-E3` 为 USD，无同币种日结；
- `BANK-E4` 金额 99999 无法由任意 1～3 条日结凑出。

### 请求体字段

```json
{
  "settlements": [
    {"id": "STL-001", "date": "2026-11-01", "currency": "CNY", "amount_cents": 10000}
  ],
  "bank_entries": [
    {"id": "BANK-001", "date": "2026-11-02", "currency": "CNY", "amount_cents": 10000}
  ]
}
```

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `id` | string | 组内唯一，非空，最长 64 |
| `date` | string(`YYYY-MM-DD`) | ISO 日期 |
| `currency` | string(3) | ISO 4217 三位币种码，大小写不敏感 |
| `amount_cents` | integer | **正整数**分，严格 > 0 |

两组记录各允许 0～12 条；编号重复、金额非正、多余字段、超条数等均返回 `422`。

## 算法说明

1. 枚举每条入账的全部合法候选日结组（1～3 条、同币种、金额相等、日期合规）；
2. 按入账逐条推进的前/后缀位掩码 DP（2^n，n≤12），从机制上保证每条入账至多使用一次，
   求得全局最优匹配金额 `optimum`；
3. 利用 SOS 子集最大值表，判定每个候选组能否嵌入某个最优方案：
   - 某入账仅有 1 个最优兼容组 → 所有最优方案中映射恒定 → 自动确认；
   - 多于 1 个 → 存在歧义 → 人工核对；
4. 带上界剪枝的 DFS 枚举两套不同的完整最优方案；
5. 汇总任何最优方案中都未出现的记录为 `never_matched`。

满规模（12×12）极端高歧义输入的求解时间在 1 秒量级，常规输入为毫秒级。

## 测试

```bash
pip install -r requirements.txt
pytest                       # 单元 / 集成 / 执行层测试
python tests/fuzz_verify.py  # 400 组随机输入与暴力穷举实现交叉验证
```

交叉验证会逐一比对最优值、方案合法性（记录不重复使用）、唯一/歧义标记以及
自动确认/人工核对分类，确保 DP 实现与独立暴力枚举完全一致。
