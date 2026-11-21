# Thin Volume Service

本地精简卷（thin-provisioned volume）API，用于 CI 作业从“装好依赖的系统盘”克隆测试卷。
FastAPI + SQLite，按 4 KiB 块读写，克隆共享物理块，池满时按**整批净变化**判定容量。

- 定长空卷：创建时只登记逻辑大小，不占任何物理块；未分配块读出来是全零。
- 克隆：复制块映射而不是数据，共享块只占一个物理块；各卷写时复制，互不影响。
- 批量更新：一次请求可同时改写和释放多个不同块，整批一个事务。
- 满池换块：只要整批更新后放得下（例如释放 2 块、写入 2 块）就成功；超限整批 409，内容和用量都不变。
- 并发：`BEGIN IMMEDIATE` 串行化写入者，WAL 快照让读取者只看到批次前或批次后的完整状态。

## 快速开始

```bash
docker compose up --build -d
docker compose ps                 # 等到 healthy
curl -s localhost:8000/health

# 演示脚本需要 httpx；不在容器里跑，装到宿主机虚拟环境
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/demo.py            # 端到端演示 + 断言，要求空数据库
```

`POOL_BLOCKS` 是存储池的物理块数（默认 64，见 `docker-compose.yml`）。一个共享块只占一块；
删除/释放最后一个引用时才归还容量。

演示脚本会依次：

1. 建一个 16 块（64 KiB）空卷，读未分配块得到 4 KiB 零；
2. 写块 0、1，确认池用量为 2；
3. 克隆，确认用量仍为 2（共享）；
4. 在克隆里改写块 0，确认用量 3，且源卷读到的还是旧数据；
5. 克隆释放共享块 1，确认没有归还容量；源卷仍能读；
6. 用 filler 卷把池填满；
7. 满池下“释放块 0 + 写入块 10”同一批成功，用量保持满；
8. 满池下只写两块不释放 → 409 `pool_full`，用量和内容完全不变。

脚本任一步断言失败都会以非 0 退出。

重置环境（会删数据卷）：`docker compose down -v`。

## API

所有响应为 JSON（块读取为 `application/octet-stream`）。`base64` 必须是严格编码、解码后恰好
4096 字节。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 存活探针 |
| GET | `/pool` | 池容量 / 已用 / 剩余（块与字节） |
| GET | `/volumes` | 卷列表，含每卷已分配块数 |
| POST | `/volumes` | 建卷或克隆：`{"name","size_bytes","clone_from"?}` |
| GET | `/volumes/{id}` | 卷详情 |
| DELETE | `/volumes/{id}` | 删卷；只归还最后一个引用消失的块 |
| POST | `/volumes/{id}/blocks` | 批量写/释放：`{"writes":[{"lba","data_b64"}],"frees":[lba]}` |
| GET | `/volumes/{id}/blocks/{lba}` | 读一个块，返回 4096 字节 |
| GET | `/volumes/{id}/blocks?start=&count=` | 连续读多个块，`count<=1024` |

### 示例

```bash
# 建 64 KiB 空卷（16 个 4 KiB 块）
curl -s -X POST localhost:8000/volumes \
  -H 'content-type: application/json' \
  -d '{"name":"base","size_bytes":65536}'
# -> {"id":1,...,"allocated_blocks":0,"pool":{"used_blocks":0,...}}

# 写 1 个块（这里用 Python 生成 4096 字节的 'A'）
python - <<'PY'
import base64, json, urllib.request
body = json.dumps({"writes":[{"lba":0,"data_b64":base64.b64encode(b"A"*4096).decode()}]}).encode()
req = urllib.request.Request("http://localhost:8000/volumes/1/blocks", body,
                             {"content-type":"application/json"}, method="POST")
print(urllib.request.urlopen(req).read().decode())
PY

# 读块（未分配的块返回全零）
curl -s localhost:8000/volumes/1/blocks/0 | wc -c        # 4096

# 克隆
curl -s -X POST localhost:8000/volumes \
  -H 'content-type: application/json' \
  -d '{"name":"clone","size_bytes":65536,"clone_from":1}'

# 克隆里改写块 0（copy-on-write，只新增 1 个物理块）
python - <<'PY'
import base64, json, urllib.request
body = json.dumps({"writes":[{"lba":0,"data_b64":base64.b64encode(b"C"*4096).decode()}]}).encode()
req = urllib.request.Request("http://localhost:8000/volumes/2/blocks", body,
                             {"content-type":"application/json"}, method="POST")
print(urllib.request.urlopen(req).read().decode())
PY

# 满池时一次释放+改写多个块
curl -s -X POST localhost:8000/volumes/2/blocks \
  -H 'content-type: application/json' \
  -d '{"writes":[{"lba":10,"data_b64":"<base64 4096B>"}],"frees":[0,1]}'
```

完整可执行版本见 `scripts/demo.py`，它会把每个请求和响应都打印出来。

### 错误约定

| 状态 | 含义 |
| --- | --- |
| 404 | 卷不存在 |
| 409 `pool_full` | 整批净增量超出池容量；响应带 `capacity_blocks` / `used_blocks` / `required_blocks` / `over_by_blocks`，内容与用量不变 |
| 409 `size_mismatch` / 名称重复 | 克隆大小不一致 / 卷名已存在 |
| 416 | LBA 或读取范围越界 |
| 422 | 请求体校验失败（含 base64 长度、一批里重复 LBA、批量操作数 > 10000） |
| 503 `database_busy` | 等 SQLite 写锁超时，可重试（带 `Retry-After`） |

## 设计要点

**数据模型**（`app/db.py`）

- `physical_blocks`：物理块，每行持有 4096 字节 BLOB 和 `refcount`。共享块的 `refcount > 1`。
- `block_map(volume_id, lba) -> physical_block_id`：逻辑块映射。未映射即空洞，读为零。
- `volumes`：逻辑大小、`version`（每次成功批次 +1）、`cloned_from`。

池容量存在 `meta.pool_blocks`，启动时由 `POOL_BLOCKS` 写入；已用量大于新容量时启动失败，
不会悄悄丢数据。

**批次与容量**（`app/storage.py`）

一次 `POST /volumes/{id}/blocks` 在一个 `BEGIN IMMEDIATE` 事务里完成：

1. 读出被触及 LBA 的旧映射，计算每块会减少几个引用；
2. `使用后 = 使用前 - 本批释放的物理块数 + 本批新写块数`。注意“释放的物理块数”只统计
   `refcount` 降到 0 的块——共享块只掉引用，不腾容量；
3. 超限 → 抛 409，事务回滚，映射、引用计数、版本号都没动；
4. 未超限 → 删旧映射、减引用、删除归零块、插入新块、卷版本 +1，一次提交。

因此“池满但整批净零”能成功；"池满且净增"整批失败。

**并发**（`app/db.py`）

- 写事务用 `BEGIN IMMEDIATE`，一开始就拿 RESERVED 锁，容量检查与修改在同一临界区，
  多进程也不会同时花掉最后一块。测试里 20 个进程抢 6 块，正好 6 成功 14 个 409。
- 读事务用普通 `BEGIN`，WAL 模式下拿到稳定快照：一次 `count=8` 的读不可能看到半批新数据。
- 等待锁超过 `SQLITE_BUSY_TIMEOUT_MS`（默认 30s）返回 503，而不是 500。
- 请求处理用同步端点，FastAPI 在线程池里并发执行；每个请求独立 SQLite 连接。

**写时复制**：改写总是分配新物理块再重指向 LBA，绝不原地改共享块。副本只在引用计数
归零时删除。

## 配置

| 环境变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_BLOCKS` | 1024（compose 里 64） | 物理池块数 |
| `DB_PATH` | `/data/volumes.sqlite3` | SQLite 文件；必须是文件，不能是 `:memory:` |
| `WEB_CONCURRENCY` | 4 | uvicorn worker 数 |
| `SQLITE_BUSY_TIMEOUT_MS` | 30000 | 写锁等待上限，超时返回 503 |
| `ROOT_PATH` | 空 | 反向代理子路径 |
| `THINVOL_PORT` | 8000 | compose 暴露端口 |

## 自测

```bash
pip install -r requirements-dev.txt
python -m pytest tests -q
```

- `tests/test_api.py`：建卷/零读、越界、克隆共享与写时复制、删卷引用释放、批量多块、
  满池净零、超限原子拒绝、零容量、启动拒绝缩小池。
- `tests/test_concurrency.py`：真实 HTTP socket 上并发；20 个写者抢 8 块（8 成功 12 个 409）、
  满池并发净零批次全部成功、3 个克隆读者在 30 轮整卷改写中从未看到撕裂批次。
- `tests/test_multiprocess.py`：20 个独立进程（各自 SQLite 连接）抢 6 块，恰好 6 成功，
  最终池用量 6。

## 已知限制

- SQLite 是单写者：写入串行化。容量和正确性没问题，但吞吐上限是单个 SQLite 文件；
  这是本任务“本地精简卷 API”的范围，不是分布式存储。
- 每个物理块一行 BLOB，读取按 LBA 逐块查；没有做顺序预读/缓存。单请求读取上限 1024 块（4 MiB），
  批量更新上限 10000 个操作。
- 没有鉴权、多租户、快照回滚和离线压缩；空间在最后一个引用消失时立即归还，无需后台 GC。
- 克隆要求与源卷等大，且只能克隆当前状态，没有时点快照链。
- 容器内以非 root 用户运行；`/data` 是命名卷。`docker compose down -v` 会清空数据。
- **未验证项**：交付环境没有 Docker daemon（无 `/var/run/docker.sock`），所以 `docker compose
  build/up` 本身没有实际执行。Dockerfile/compose 只做了 YAML 与配置静态检查；应用、镜像启动
  命令中的同一套 uvicorn 参数、以及演示脚本均已在本机真实进程上跑通。首次在有 Docker 的机器
  上启动时请执行 `docker compose up --build` 并确认 healthcheck 通过。
- 未做时点快照/回滚链；克隆只反映创建时刻的父卷内容，之后父卷写入不会出现在克隆里（写时复制
  的预期行为）。
