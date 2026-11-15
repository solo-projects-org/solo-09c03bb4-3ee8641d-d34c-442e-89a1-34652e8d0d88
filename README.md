# 断面水质逐时氨氮采集平台

县站分页接口的模拟服务 + FastAPI 采集网页（SQLite 落库），附带"采集中途县站更正记录"与"污染复核组原因撤回 + 修订围栏"的完整样例。

## 要解决的问题

断面水质数据员要按 **断面 + UTC 日期** 从县站分页接口拉取某日逐时氨氮（NH₃-N）观测，交给污染复核组。县站可能在采集途中更正记录。平台保证：

1. **逐页核对修订号**：首页取得的 `revision` 被钉住，后续任何一页修订号不一致即中止。
2. **游标随修订失效**：县站更正后修订号递增，旧游标再用返回 `409 stale_cursor`，杜绝把两个修订版本的页拼成"弗兰肯数据"。
3. **当日且时刻唯一**：每行必须是该 UTC 日期的整点时刻，跨页出现重复时刻即中止。
4. **整日发布（all-or-nothing）**：收齐 `total_rows` 且收尾再次确认源端修订号未变，才在一个 SQLite 事务里替换整天；失败时旧版（若有）原样保留，不完整数据永远查不到。
5. **超时重试当前页**：网络错误、超时、5xx 只重试当前页（游标不前进），最多 3 次；`409` 属永久性错误不重试。
6. **双 Web 进程并发安全**：同一断面+日期同时只允许一个活动采集任务（SQLite 部分唯一索引，跨进程生效）；两个进程共享同一 SQLite 卷。
7. **迟到采集不能盖掉较新修订**：发布事务里比对已发布修订号，旧修订号写入直接拒绝（`StaleRevisionError`）。
8. **复核组原因撤回 + 修订围栏**：污染复核组可在网页填写原因、按"所见修订号"撤回某日已发布版本。撤回在一个 SQLite 事务里删除整天观测并写入审计行，下游立刻读不到旧数据，但原因和被撤回修订仍可查。乐观并发保证：提交的修订号与当前不一致（页面已换成新修订）即返回 `409 revision_conflict`，重复提交不会误伤新档。撤回形成**修订围栏**——同修订或更早的在途采集即使迟到也不能重新发布（`WithdrawnRevisionError`），只有**严格更高修订**重新通过原有整日校验后才能恢复可用，恢复同时把撤回记录标记为"已被取代"。
9. **读取来自同一已提交状态**：页面读取在单个读事务（WAL 快照）里同时取撤回状态与观测，两者不可能来自不同提交；任何写入失败都整体回滚，原状态原样保留。

## 架构

```
docker compose
├── station  县站模拟接口（可切换/递增修订号，分页 + 游标）   :8001
├── web1     采集 Web 进程 A（FastAPI + SQLite WAL）          :8011
└── web2     采集 Web 进程 B（同一份代码，同一 SQLite 卷）    :8012

卷 quality-data:/data  -> 两个 web 进程共享 quality.db
```

### 县站接口（`station/`，本地模拟，带管理端用于演示更正）

| 方法/路径 | 说明 |
|---|---|
| `GET /metadata?section=&date=` | 当前修订号、总行数、页大小 |
| `GET /observations?section=&date=&cursor=` | 分页返回 `revision`、`total_rows`、`page`、`next_cursor` 和行（含 `source_page`） |
| `POST /admin/correct` | 更正某时刻数值：修订号 +1，旧游标立即失效 |
| `POST /admin/reset` | 该日数据重置回修订号 1 |
| `POST /admin/fail-next` | 下一次分页读取返回 500（一次性，演示重试） |
| `POST /admin/latency` | 调整每页模拟延迟（演示并发用） |

游标是携带 `(section, date, revision, page)` 的不透明 token；服务端发现 token 里的修订号与当前不符即返回 409。

### Web 接口（`web/`）

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 采集网页：选断面、UTC 日期，触发采集、按原因撤回所见修订，查看任务、已发布/已撤回版本与撤回审计 |
| `GET /api/sections` | 县站支持的断面 |
| `POST /api/collect` | 启动后台采集（同断面+日期已有活动任务时返回 409） |
| `POST /api/withdraw` | 原因撤回：body 含 `section/date/expected_revision/reason`；乐观校验所见修订号，冲突返回 409 `revision_conflict`，已撤回返回 409 `already_withdrawn`，无发布版本返回 404 |
| `GET /api/runs/{id}` | 单个任务状态（running/success/failed/interrupted、修订号、行数、页尝试次数、错误） |
| `GET /api/runs?section=&date=` | 该日全部采集任务（含两个 worker 的记录） |
| `GET /api/withdrawals?section=&date=` | 该日撤回审计轨迹（含撤回修订、原因、撤回人、被哪个更高修订取代及时间） |
| `GET /api/published?section=&date=` | 当日当前可读状态（单事务快照）：`status=published` 时返回整日观测；`status=withdrawn` 时返回原因、被撤回修订且 `rows=[]`；从未发布返回 404 |

`GET /api/published` 的撤回响应示例：

```json
{
  "status": "withdrawn",
  "section": "SECTION-A",
  "date": "2026-11-14",
  "revision": null,
  "rows": [],
  "withdrawn_revision": 1,
  "reason": "污染复核：仪器故障数据混入，县站尚未更正，先阻止下游取用",
  "withdrawn_at": "2026-11-15T08:30:00+00:00",
  "withdrawn_by": "web2",
  "withdrawal_id": 7
}
```

## 快速启动（Docker Compose）

```bash
./run-demo.sh                 # 构建、起栈、等待健康、自动跑全部演示检查
# 或手动：
docker compose up -d --build
# 网页 http://localhost:8011 （或 8012），县站文档 http://localhost:8001/docs
```

手动的中途更正样例（curl 逐步可观察）：

```bash
./demo/mid-correction.sh      # 采集中途县站更正
./demo/withdraw.sh            # 复核组原因撤回 -> 围栏挡住旧修订 -> 高修订恢复
```

### 不用 Docker 的本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r web/requirements.txt -r station/requirements.txt
PAGE_SIZE=8 .venv/bin/python -m uvicorn station.app.main:app --port 8001 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web1 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8011 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web2 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8012 &
python3 -I demo/demo.py
```

## 演示场景（`demo/demo.py`，59 项断言）

1. **中途更正**：采集开始后县站更正 08:00 数值（修订号 1→2）→ 采集因 409 失败、无半成品公布、旧游标确认为 409；重新采集后整日按修订号 2 原子发布，24 行时刻唯一、来源页齐全。
2. **读取失败重试**：注入一次性 500 → 仅当前页重试（`page_attempts` 大于页数），最终成功发布。
3. **双进程并发 + 陈旧修订防护**：两进程同时采集同日 → 一个 202、一个 409；再构造"源端退回旧修订号的迟到采集"，发布事务拒绝旧修订号，已发布的新版本原样保留；另有直接对 DB 事务的防护单测。
4. **复核组撤回（28 项）**：
   - 无发布版本撤回 → 404；旧页面带过期修订号撤回 → 409 `revision_conflict` 且数据原样保留；
   - 按所见修订号 + 原因撤回成功，两个 Web 实例读取均为 `withdrawn`、零旧观测、带原因与被撤回修订；重复撤回 → 409 `already_withdrawn`；
   - 同修订/更早修订的在途采集迟到 → 发布事务被修订围栏拒绝（`WithdrawnRevisionError`），县站未更正前始终无法重新发布；
   - 慢采集与撤回交错：撤回立即生效，两实例读到同一已提交快照，在途采集发布时被围栏挡住；
   - 县站更正产生严格更高修订（1→2）→ 重新通过整日校验后恢复可用，撤回记录同事务标记被修订 2 取代；恢复后迟到的旧修订仍被拒绝；
   - DB 层单测：乐观冲突不改数据、围栏拒绝同修订、高修订解除围栏、写锁失败时整体回滚且原状态完整保留。

> 演示每次运行使用唯一断面名（`DEMO-<时间>-<pid>`），因此在持久化 SQLite 卷上重复执行互不污染。

另外验证过：worker 进程被 `kill -9` 后重启，遗留的 running 任务自动标记为 `interrupted`，可重新采集。

## 关键设计

- **发布事务**（`web/app/db.py: publish_version`）：`BEGIN IMMEDIATE` 下先查活动撤回围栏（`revision <= 被撤回修订` 即拒绝）→ 再查已发布修订号（旧修订拒绝）→ 删除旧整天数据 → 插入新整天 → 任务置成功 → 若有围栏则同事务标记撤回被取代；任何异常都经 `rollback_safely` 整体回滚。
- **撤回事务**（`web/app/db.py: withdraw_published`）：`BEGIN IMMEDIATE` 下依次检查"是否已撤回 / 是否存在发布版本 / 期望修订号是否等于当前修订号"，通过后插入 `withdrawals` 审计行并删除整天观测，单事务提交；乐观冲突不产生任何写入。
- **快照一致读取**（`web/app/db.py: get_published`）：单个 `BEGIN` 读事务在 WAL 同一快照内读取撤回记录与观测，撤回状态和观测必然来自同一已提交状态。
- **修订围栏**：`withdrawals` 表上 `WHERE superseded_at IS NULL` 的部分唯一索引保证每断面+日期至多一个生效围栏；同修订或更早的迟到采集无法重新发布，只有严格更高修订能恢复，并关闭围栏。
- **活动任务互斥**：`runs` 表上 `WHERE status='running'` 的部分唯一索引，跨进程互斥，不依赖单机内存锁。
- **采集状态机**（`web/app/collector.py`）：metadata 钉修订号 → 逐页（重试仅当前页、校验修订号/总行数/页序/时刻唯一）→ 收齐行数 → 再查一次 metadata 复核修订号 → 整日发布（围栏/陈旧修订异常均置任务失败）。
- **崩溃恢复**：worker 启动时把本库遗留的 `running` 任务标记为 `interrupted`，避免互斥锁泄漏。
- **SQLite 并发**：WAL + `busy_timeout=30s`，两个 uvicorn 进程共享同一数据库文件。

## 目录

```
station/            县站模拟服务（FastAPI）
  app/main.py       分页/游标/修订号/管理端
  app/data.py       确定性 24 小时合成数据
web/                采集网页（FastAPI + SQLite）
  app/main.py       HTTP API 与网页（含撤回表单与审计展示）
  app/collector.py  逐页核对、重试、整日发布状态机（含撤回围栏处理）
  app/station_client.py
  app/db.py         SQLite 模式、互斥、发布/撤回事务与快照一致读取
demo/demo.py        四场景自动化演示（python -I，59 项断言，仅标准库 + 本仓库 db 模块）
demo/mid-correction.sh  curl 手动中途更正样例
demo/withdraw.sh        curl 手动原因撤回/围栏/恢复样例
docker-compose.yml  station + web1 + web2 + 共享卷
```
