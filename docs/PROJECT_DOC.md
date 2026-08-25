# WAL 崩溃恢复库 —— 项目设计文档

> 项目提示词：**WAL 崩溃恢复库：用 Go 实现预写日志库，支持追加写、checksum 校验、批量刷盘与启动重放，保证崩溃一致性。**

- 文档版本：v1.0
- 文档状态：已评审（设计阶段）
- 配套约束：Go 代码行数（不含测试代码）**2000 < N < 2200**；Go 代码文件数 **20 < M < 25**；附带**简单前端界面**。

---

## 1. 项目概述

### 1.1 项目背景

在数据库、消息队列与分布式存储系统中，**崩溃一致性（Crash Consistency）** 是最核心的可靠性诉求之一。当进程在写入数据的过程中突然崩溃（断电、kill -9、OOM 被杀），内存中的脏数据全部丢失，已落盘的数据也可能出现"写了一半"的撕裂状态。**预写日志（Write-Ahead Log, WAL）** 是解决该问题的通用范式：在任何数据变更落盘之前，先把描述该变更的日志记录追加写入持久化日志文件，并保证日志先于数据落盘；崩溃恢复时重放日志即可把存储恢复到一致状态。

本项目以 Go 语言实现一个**通用的、嵌入式 WAL 崩溃恢复库**（包名 `wal`），在其之上封装一个**基于内存 + 快照的键值存储**（包名 `store`）用于演示与联调，并通过 **HTTP API + Web 前端** 提供可视化的写入、刷盘、崩溃模拟与恢复演示能力。

### 1.2 项目目标

| 编号 | 目标 | 说明 |
|------|------|------|
| G1 | 追加写 | 支持将任意字节负载（payload）追加写入日志，追加是 O(1) 的顺序 IO，支持任意大小的记录（超长记录自动分片）。 |
| G2 | Checksum 校验 | 每条记录携带 CRC32 校验和；读取与重放时逐条校验，发现损坏立即报错并可定位损坏点。 |
| G3 | 批量刷盘 | 支持组提交（group commit）：多条记录合并为一批，一次 `fsync` 落盘，显著提升吞吐；支持同步/异步/批量三种刷盘模式。 |
| G4 | 启动重放 | 库启动时扫描既有日志段，校验 checksum、重建索引、重放到最后一条完整记录，丢弃尾部撕裂记录。 |
| G5 | 崩溃一致性 | 任何时刻崩溃，恢复后日志内容等于"某一时刻已刷盘的完整前缀"，不出现半条记录；先写日志后落数据。 |
| G6 | 段管理与归档 | 日志按固定大小分段滚动；支持旧段清理/归档，防止磁盘无限增长。 |
| G7 | 演示层 | 内存 KV 存储基于 WAL 保证崩溃一致性；提供快照与恢复；HTTP API 与简单 Web 界面可交互演示。 |

### 1.3 非功能需求

- **性能**：批量模式下吞吐 ≥ 10 万条/秒（本机 SSD 参考值）；单条追加延迟 < 50µs（异步模式）。
- **正确性**：随机崩溃注入测试（kill -9）后重放，数据零丢失（已确认刷盘）与零损坏（checksum 全覆盖）。
- **可观测**：暴露累计写入字节、记录数、段数、fsync 次数、校验失败次数等指标。
- **简洁性**：仅依赖 Go 标准库（`crypto` 用 `hash/crc32`），无第三方运行时依赖；前端为内嵌静态资源（`go:embed`）。
- **代码规模**：非测试 Go 代码 2000~2200 行，21~24 个 `.go` 文件（详见第 5 章代码量预算）。

---

## 2. 需求分析与功能扩展

### 2.1 核心需求展开（来自提示词）

| 提示词关键词 | 展开为功能点 | 对应模块 |
|--------------|--------------|----------|
| 追加写 | 顺序追加、记录分片/重组、位置（offset）返回、并发追加串行化 | wal |
| checksum 校验 | CRC32 写入期计算、读取期校验、损坏点定位、损坏注入（供演示） | wal |
| 批量刷盘 | 组提交批、3 种刷盘策略、`Sync` 精确刷盘、`fsync` 统计 | wal |
| 启动重放 | 启动扫描、尾部截断、索引重建、损坏段隔离 | wal |
| 崩溃一致性 | 数据-日志双写顺序、恢复协议、快照协同 | wal + store |

### 2.2 扩展功能（超出提示词的最小增强）

1. **日志分段（Segments）**：日志按 `MaxSegmentBytes` 滚动为多个文件（`00000000000000000001.wal` 等），支持索引与清理。
2. **块缓冲（Buffer）**：追加先写内存缓冲，达到阈值或定时器触发批量刷盘。
3. **组提交（Group Commit）**：多个 goroutine 的追加合并为一次 `fsync`。
4. **索引（Index）**：记录 `(逻辑序号, offset)` 映射，支持按序号随机定位读取。
5. **快照（Snapshot）**：存储层周期性把内存状态落盘，并记录快照对应的日志水位，恢复时"快照 + 增量重放"。
6. **指标（Metrics）**：线程安全计数器，供 API 与前端展示。
7. **崩溃模拟**：通过 API 注入"模拟崩溃"，前端演示恢复后数据不丢失。
8. **校验损坏注入**：通过 API 篡改某条记录的 checksum，演示校验失败与损坏定位。

### 2.3 业务场景（用例）

- **场景 A（数据库写路径）**：业务进程写 KV → 先追加 WAL 记录 → 批量刷盘 → 更新内存态 → 返回成功。
- **场景 B（崩溃恢复）**：进程崩溃 → 重启 → WAL 扫描全部段 → 校验 → 截断尾部 → 重建索引 → 重放到 store 状态机 → 对外提供一致性视图。
- **场景 C（运维演示）**：Web 前端发起"写入 1000 条 + 立即 kill"→ 重启进程 → 前端展示恢复后的记录数与写入数一致。
- **场景 D（损坏检测）**：前端点击"破坏第 N 条记录"→ 重启/触发校验 → 前端展示"定位到段 X 偏移 Y，该校验失败记录及其之后内容被丢弃"。

---

## 3. 业务逻辑详细设计

### 3.1 记录格式（Record Format）

每条 WAL 记录采用固定头 + 变长负载的二进制帧：

```
┌─────────────────────────────────────────────────────────────┐
│ Magic(2B) │ Flags(1B) │ CRC32(4B) │ PayloadLen(4B) │ Seq(8B) │
├─────────────────────────────────────────────────────────────┤
│                          Payload(Bytes)                     │
└─────────────────────────────────────────────────────────────┘
```

| 字段 | 长度 | 说明 |
|------|------|------|
| Magic | 2B | 固定 `0x57 0x41`（"WA"），用于快速识别记录边界 |
| Flags | 1B | 位 0：`First`（分片首片）；位 1：`Last`（分片末片）；位 2：`Padding` |
| CRC32 | 4B | 对 `Flags + PayloadLen + Seq + Payload` 计算（头部 Magic 不参与） |
| PayloadLen | 4B | 负载长度（大端 uint32），单片 ≤ 32KB，整条逻辑记录 ≤ 1GB |
| Seq | 8B | 全局单调递增逻辑序号（大端 uint64），从 1 开始 |
| Payload | nB | 用户数据；超长记录按 `MaxRecordBytes` 分片，除末片外均满片 |

**设计要点**：
- 记录之间无分隔符，靠 Magic + 长度自描述，便于顺序扫描与随机跳读。
- 校验和覆盖"内容 + 序号 + 标志"，防止错位/错序导致的静默损坏。
- 尾部分片（`First` 置位但无对应 `Last`）视为**未刷完整**，重放时丢弃。

### 3.2 追加写流程（Append Path）

```
业务调用 Append(payload)
  → 加写锁（或 CAS 原子自旋）
  → 计算 Seq（全局计数器 +1）
  → 若 payload > MaxRecordBytes：按 32KB 分片，生成多条 Record
  → 每条 Record 序列化 → 写入内存缓冲 buffer（仅内存拷贝，不落盘）
  → 若超过 BatchThreshold 或 SyncPolicy=Sync：触发批量刷盘
  → 刷盘：write(2) 全部缓冲 → fsync(2) → 更新 durable 水位（durableSeq）
  → 释放锁，返回 (seq, offset, error)
```

**崩溃一致性要点**：`Append` 的"成功返回"仅当该记录已被刷盘（`durableSeq ≥ seq`）。异步模式下 `Append` 立即返回，由 `Sync()` 或批量水位保证语义；库默认提供 `AppendSync`（单条即时刷盘）与 `AppendBatch`（组提交）两种入口。

### 3.3 批量刷盘与组提交（Batch Flush / Group Commit）

- **缓冲结构**：`buffer` 持有未刷盘字节切片与待确认的 seq 区间 `[pendingLow, pendingHigh]`。
- **触发条件**：累计字节 ≥ `BatchSize`（默认 64KB）或距上次刷盘 ≥ `FlushInterval`（默认 10ms）或显式 `Sync()`。
- **组提交**：多个并发 `AppendBatch` 共享同一刷盘循环；任一调用者触发刷盘时，整个缓冲中的全部记录一并 `write + fsync`，所有等待者通过 channel/条件变量同时被唤醒——即"一次 fsync 确认一批"。
- **刷盘顺序**：严格按记录写入顺序落盘，保证磁盘上的日志始终是**完整前缀**（prefix property），这是崩溃一致性的根基。

### 3.4 段管理与滚动（Segment Rotation）

- 目录布局：`<Dir>/00000000000000000001.wal`，文件名即段起始 Seq（20 位十进制）。
- 当前段累计大小超过 `MaxSegmentBytes`（默认 64MB）时：
  1. 刷盘并关闭当前段；
  2. 以 `durableSeq+1` 创建新段；
  3. 更新段表 `segments`。
- 段清理：`CleanOldSegments(beforeSeq)` 删除已归档（快照水位已覆盖）的旧段；`TrimSuffix` 删除尾部无效字节。

### 3.5 启动重放与崩溃恢复（Replay / Recovery）

启动时序（`Open` 内部自动执行，也可调用 `Recover` 手动触发）：

```
Open(dir)
  → 枚举 *.wal 段文件，按文件名排序，检查序号连续性
  → 逐段扫描：
       读 19B 头 → 校验 Magic → 校验 CRC32
       失败 → 记录损坏点 (segment, offset, reason) → 停止该段后续扫描
       成功 → 收集 (seq, offset, payload) → 重建索引
  → 尾部处理：
       若最后一条为"分片首片"或"未闭合分片" → 截断（Truncate）
       若段文件末尾存在不足 19B 的残片 → 整段截断至最后完整记录末尾
  → 将完整记录序列回调 ReplayFunc(seq, payload)（store 借此重放状态机）
  → 设置 WAL 起点水位 = 最后完整记录的 seq；损坏点之后的日志视为不可信
```

**恢复语义保证**：
- **原子性**：恢复结果只可能是"某次刷盘时刻的完整前缀"的超集（未刷盘部分被截断），不可能出现半条记录。
- **幂等性**：store 的 `Apply` 按 seq 去重（已存在同 seq 则跳过），保证重复重放安全。
- **损坏隔离**：单段损坏不影响其他段；损坏段之后的记录以"可读但未验证"方式处理，默认丢弃并告警。

### 3.6 数据与日志的双写顺序（Write Ordering）

```
业务写路径：
  1. WAL.Append(变更描述)  → 落盘（fsync）
  2. store 内存状态应用变更
  3. 返回成功
恢复路径：
  1. 加载最近快照（若有）
  2. 重放快照水位之后的全部 WAL 记录 → 重建内存状态
  3. 对外提供服务
```

任何时刻崩溃，磁盘状态满足：WAL 先于数据 → 恢复后数据 = WAL 重放结果 → 数据不丢失、不重复、不撕裂。

### 3.7 快照协同（Snapshot）

- `store.Snapshot()`：序列化内存 KV 为字节流，写入 `snapshot.dat`，并记录此刻 `durableSeq` 为快照水位。
- 恢复时：快照水位之前的日志段可安全清理（`CleanOldSegments`）。
- 崩溃于"快照写了一半"：快照文件头含 Magic + 自身 checksum，校验失败则丢弃快照，回退为全量重放。

---

## 4. 架构设计

### 4.1 总体架构图

```
┌────────────────────────────────────────────────────────────────┐
│                        Web 前端（静态页）                        │
│   写入 / 刷盘 / 崩溃模拟 / 损坏注入 / 指标面板 / 恢复演示        │
└───────────────────────────────┬────────────────────────────────┘
                                │ HTTP (JSON)
┌───────────────────────────────▼────────────────────────────────┐
│                        api 包（HTTP 服务层）                     │
│   server.go  handlers.go  router.go  （绑定 127.0.0.1:8888）    │
└───────────────┬───────────────────────────────┬────────────────┘
                │                               │
┌───────────────▼───────────────┐   ┌───────────▼────────────────┐
│        store 包（KV 存储层）    │   │    wal 包（WAL 核心库）      │
│  store.go  state.go           │   │  wal.go  record.go          │
│  snapshot.go                  │──▶│  segment.go  buffer.go      │
│   内存KV + 快照 + 状态机重放   │   │  checksum.go  fsync.go      │
└───────────────────────────────┘   │  reader.go  replay.go       │
                                    │  recovery.go  truncate.go   │
                                    │  index.go  metrics.go       │
                                    │  options.go  errors.go      │
                                    └───────────┬────────────────┘
                                                │ os.File + fsync
                                    ┌───────────▼────────────────┐
                                    │  磁盘：<dir>/*.wal 段文件     │
                                    │  snapshot.dat               │
                                    └────────────────────────────┘
```

### 4.2 分层职责

| 层 | 包 | 职责 | 依赖 |
|----|----|------|------|
| 应用入口 | `cmd/wald` | 解析参数、组装依赖、启动 HTTP 服务、优雅退出 | 全部 |
| 服务层 | `internal/api` | HTTP 路由、JSON 编解码、调用 wal/store | wal, store |
| 存储层 | `internal/store` | KV 内存态、状态机 Apply、快照序列化 | wal |
| 核心库 | `internal/wal` | 日志追加/读取/校验/刷盘/重放/恢复/指标 | 仅标准库 |
| 资源层 | `web` | `go:embed` 内嵌前端静态文件 | — |

> 说明：`internal/wal` 是**通用可复用库**，不依赖 store 与 api，保证"库"的独立性；store/api 为演示与联调层。

### 4.3 关键数据结构

```go
// wal 包
type WAL struct {              // 对外主句柄
    dir      string
    opts     Options
    buf      *buffer           // 批量缓冲
    cur      *segment          // 当前段
    segments map[uint64]*segment
    index    *index
    metrics  *Metrics
    durable  atomic.Uint64     // 已刷盘的最大 seq（水位）
    mu       sync.Mutex        // 追加/刷盘串行化
}

type segment struct {          // 单个日志段
    startSeq uint64            // 段起始逻辑序号
    file     *os.File
    size     int64             // 当前已写字节
    closed   bool
}

type record struct {           // 单条（或单片）记录
    magic  uint16
    flags  uint8               // first/last/padding
    crc    uint32
    length uint32
    seq    uint64
    data   []byte
}
```

```go
// store 包
type Store struct {
    wal      *wal.WAL
    kv       map[string][]byte
    snap     *snapshot
    applied  atomic.Uint64     // 已应用的最大 seq
}
```

---

## 5. 文件规划与代码量预算（关键约束）

### 5.1 约束

- 非测试 Go 代码总行数：**2000 < N < 2200**（以 `gofmt` 后 `wc -l` 计，不含 `_test.go`）。
- Go 代码文件数：**20 < M < 25**，即 21~24 个非测试 `.go` 文件。

### 5.2 文件清单与行数预算（合计 22 个文件 / 2030 行）

| # | 文件路径 | 主要职责 | 预算行数 |
|---|----------|----------|----------|
| 1 | `cmd/wald/main.go` | 入口：参数解析、日志目录初始化、启动 API、信号处理 | 60 |
| 2 | `internal/wal/options.go` | Options 结构、默认值、校验 | 70 |
| 3 | `internal/wal/record.go` | 记录编解码（encode/decode/分片） | 110 |
| 4 | `internal/wal/checksum.go` | CRC32 计算与校验、损坏错误构造 | 60 |
| 5 | `internal/wal/segment.go` | 段打开/创建/滚动/清理 | 130 |
| 6 | `internal/wal/buffer.go` | 批量缓冲、组提交水位 | 120 |
| 7 | `internal/wal/wal.go` | WAL 主句柄：Open/Append/AppendSync/AppendBatch/Sync/Close | 170 |
| 8 | `internal/wal/reader.go` | 顺序/随机读取（按 seq、offset） | 90 |
| 9 | `internal/wal/replay.go` | 启动重放：回调驱动、分片重组 | 120 |
| 10 | `internal/wal/recovery.go` | 崩溃恢复协调：扫描→校验→截断→重建 | 150 |
| 11 | `internal/wal/truncate.go` | 尾部截断、残片处理、段收缩 | 90 |
| 12 | `internal/wal/fsync.go` | 刷盘策略、组提交实现、同步语义 | 90 |
| 13 | `internal/wal/index.go` | seq→offset 索引与查询 | 70 |
| 14 | `internal/wal/metrics.go` | 原子指标计数器与快照 | 70 |
| 15 | `internal/wal/errors.go` | 错误类型与哨兵错误定义 | 40 |
| 16 | `internal/store/store.go` | KV 存储：Get/Set/Delete、WAL 联动 | 120 |
| 17 | `internal/store/state.go` | 状态机 Apply（幂等重放）、双写顺序 | 80 |
| 18 | `internal/store/snapshot.go` | 快照序列化/反序列化、水位清理 | 80 |
| 19 | `internal/api/server.go` | HTTP Server 生命周期、优雅关闭 | 120 |
| 20 | `internal/api/handlers.go` | 各端点处理器：写入/读取/指标/崩溃/损坏/恢复 | 110 |
| 21 | `internal/api/router.go` | 路由注册、中间件（日志/恢复 panic） | 50 |
| 22 | `web/embed.go` | `go:embed` 前端静态资源暴露 | 30 |
| — | **合计** | **22 个非测试 Go 文件** | **2030** |

### 5.3 预算校验

```
M = 22          → 20 < M < 25 ✅
N = 2030        → 2000 < N < 2200 ✅（上下各留约 70~90 行余量）
```

**余量与调控策略**：上表为设计预算。实现阶段以 `wc -l $(git ls-files '*.go' | grep -v _test)` 每周校验一次：
- 若逼近 2200：精简注释空行、合并 `errors.go` 与 `checksum.go` 的重复逻辑、压缩 handler 样板。
- 若逼近 2000：为公开 API 补充文档注释（`// XXX 实现细节…`）、拆分 `recovery.go` 的扫描函数、为 Options 增加字段说明。
- 测试文件（`*_test.go`）不计入约束，规模建议另计 800~1200 行（见第 8 章）。

---

## 6. 前端界面设计（简单前端）

### 6.1 技术选型

- 纯静态三件套：单个 `index.html` + `style.css` + `app.js`，无构建步骤、无框架、无 CDN 依赖。
- 通过 `web/embed.go` 的 `go:embed` 编译进二进制，由 `api` 层在 `/` 提供，实现"单二进制开箱即用"。
- 与后端交互：`fetch('/api/v1/...')`，JSON 协议。

### 6.2 页面布局

```
┌──────────────────────────────────────────────────────────────┐
│  WAL 崩溃恢复库 · 演示控制台            [运行状态: ● 正常]     │
├──────────────────────────┬───────────────────────────────────┤
│ ① 写入面板                │ ② 指标面板                        │
│   Key: [____]            │  累计写入记录: 12,345              │
│   Value: [__________]    │  累计写入字节: 6.2 MB              │
│   批量条数: [100]         │  当前段文件数: 3                   │
│   [追加] [批量追加] [Sync]│  已刷盘水位 seq: 12,345            │
│                          │  fsync 次数: 87                    │
│ ③ 日志浏览                │  校验失败次数: 0                   │
│   [读取前 50 条] [按 seq] │                                   │
│   ┌──────────────────────┼───────────────────────────────────┐
│   │ #12001 | key=a | ... │ ④ 故障演练                          │
│   │ #12002 | key=b | ... │  [模拟崩溃(杀进程)]                 │
│   │ ...                  │  [破坏第 N 条记录 checksum]  N=[_]  │
│   └──────────────────────┼───────────────────────────────────┤
│                          │ ⑤ 恢复演示                          │
│ ⑥ 事件日志（最近 20 条）   │  [重启并重放]                      │
│   [10:00:01] 批量追加 1000 条成功                             │
│   ...                     │ 重放结果：恢复 12,345 条记录       │
└──────────────────────────┴───────────────────────────────────┘
```

### 6.3 页面功能与后端对应

| 前端操作 | 调用的 API | 后端行为 |
|----------|-----------|----------|
| 追加单条 | `POST /api/v1/write` | `store.Set` → `wal.AppendSync` |
| 批量追加 | `POST /api/v1/write/batch?n=100` | 循环 `wal.AppendBatch`，末次 `Sync` |
| 手动刷盘 | `POST /api/v1/sync` | `wal.Sync()` 返回刷盘水位 |
| 读取日志 | `GET /api/v1/log?limit=50&from=seq` | `wal.Reader` 顺序读取 |
| 指标面板 | `GET /api/v1/metrics` | `wal.Metrics` + store 统计 |
| 模拟崩溃 | `POST /api/v1/crash` | 进程内 `os.Exit(137)`（演示用，可配置为仅重启 WAL） |
| 损坏注入 | `POST /api/v1/corrupt?seq=N` | 直接改写段文件中第 N 条记录某字节 |
| 重启重放 | `POST /api/v1/recover` | 关闭 WAL → 重新 Open → 触发 Recover → 返回重放统计 |

> 崩溃模拟端点默认**仅允许本地回环访问**，并在响应中给出说明，避免误用。

### 6.4 前端代码规模（不计入 Go 约束）

| 文件 | 预估行数 |
|------|----------|
| `web/index.html` | 约 120 行 |
| `web/style.css` | 约 180 行 |
| `web/app.js` | 约 260 行 |

---

## 7. 接口设计（公共 API 摘要）

### 7.1 wal 包对外 API

```go
// 打开/恢复：目录不存在则创建；自动执行启动扫描与尾部截断
func Open(dir string, opts Options) (*WAL, error)

// 追加：立即返回 seq，刷盘语义由 SyncPolicy 决定
func (w *WAL) Append(payload []byte) (seq uint64, err error)

// 追加并保证本次返回前已刷盘（Sync 语义）
func (w *WAL) AppendSync(payload []byte) (seq uint64, err error)

// 组提交追加：返回后数据已在缓冲中，等待下一次批量刷盘确认
func (w *WAL) AppendBatch(payload []byte) (seq uint64, err error)

// 显式刷盘：阻塞直到缓冲全部落盘，返回已刷盘水位
func (w *WAL) Sync() (durableSeq uint64, err error)

// 启动重放：从磁盘重放全部完整记录，逐条回调 fn
func (w *WAL) Replay(fn ReplayFunc) (ReplayStats, error)

// 崩溃恢复：扫描校验 + 尾部截断 + 索引重建 + 回调重放
func (w *WAL) Recover(fn ReplayFunc) (RecoveryResult, error)

// 按 seq 读取单条；按 offset 顺序读取
func (w *WAL) Read(seq uint64) ([]byte, error)
func (w *WAL) NewReader() *Reader

// 清理/关闭
func (w *WAL) CleanOldSegments(beforeSeq uint64) error
func (w *WAL) Close() error

// 指标与内部状态
func (w *WAL) Metrics() MetricsSnapshot
func (w *WAL) DurableSeq() uint64
```

### 7.2 Options 摘要

```go
type Options struct {
    SyncPolicy       SyncPolicy   // SyncAlways | SyncBatch | SyncAsync
    BatchSize        int          // 批量刷盘阈值（默认 64KB）
    FlushInterval    time.Duration// 定时刷盘间隔（默认 10ms）
    MaxSegmentBytes  int64        // 段滚动阈值（默认 64MB）
    MaxRecordBytes   int          // 单片最大字节（默认 32KB）
    ReplayFn         ReplayFunc   // Open 时的自动重放回调
}
```

### 7.3 HTTP API 摘要

| 方法 | 路径 | 请求体 | 响应 |
|------|------|--------|------|
| POST | `/api/v1/write` | `{"key":"a","value":"b"}` | `{"seq":12001}` |
| POST | `/api/v1/write/batch` | `{"count":100}` | `{"seqs":[..],"durable":..}` |
| POST | `/api/v1/sync` | — | `{"durableSeq":12345}` |
| GET | `/api/v1/log` | query: `from,limit` | `{"records":[{seq,offset,data}]}` |
| GET | `/api/v1/metrics` | — | 指标快照 |
| POST | `/api/v1/crash` | — | 模拟崩溃（仅回环） |
| POST | `/api/v1/corrupt` | query: `seq` | 损坏注入结果 |
| POST | `/api/v1/recover` | — | `{"replayed":12345,"truncated":true}` |
| GET | `/` | — | 前端页面（内嵌资源） |

---

## 8. 测试规划（不计入行数约束）

| 测试文件 | 覆盖内容 |
|----------|----------|
| `wal/record_test.go` | 编解码往返、分片/重组、边界长度、CRC 篡改检测 |
| `wal/segment_test.go` | 滚动、文件名序号、清理、损坏段打开 |
| `wal/wal_test.go` | 追加→读取往返、AppendSync 语义、批量水位 |
| `wal/replay_test.go` | 干净日志重放、尾部残片截断、未闭合分片丢弃 |
| `wal/recovery_test.go` | 随机字节尾部注入后恢复；写一半后 kill 模拟（子进程） |
| `wal/fsync_test.go` | 组提交并发正确性（-race）、Sync 阻塞语义 |
| `wal/checksum_test.go` | 单字节翻转必失败、损坏点定位 |
| `store/store_test.go` | Set/Get/Delete、重放幂等 |
| `store/snapshot_test.go` | 快照写坏回退全量重放、快照+增量恢复 |
| `api/server_test.go` | 端点冒烟、JSON 协议、损坏注入端点权限 |

**崩溃一致性专项**：使用 `os/exec` 启动子进程执行写入任务，父进程在随机时刻 `Process.Kill()`，子进程重启后校验"已确认写入的记录全部可读且一致"，循环 ≥ 100 次。

---

## 9. 目录结构（最终形态）

```
code-23/
├── go.mod                     # module wal-demo（Go 1.21+）
├── cmd/
│   └── wald/
│       └── main.go            # 60 行
├── internal/
│   ├── wal/                   # 核心库（14 个文件，1380 行）
│   │   ├── options.go  record.go  checksum.go  segment.go
│   │   ├── buffer.go  wal.go  reader.go  replay.go
│   │   ├── recovery.go  truncate.go  fsync.go  index.go
│   │   ├── metrics.go  errors.go
│   │   └── （*_test.go 测试文件，不计数）
│   ├── store/                 # 演示存储层（3 个文件，280 行）
│   │   ├── store.go  state.go  snapshot.go
│   ├── api/                   # HTTP 服务层（3 个文件，280 行）
│   │   ├── server.go  handlers.go  router.go
│   └── ...
├── web/                       # 前端（embed.go 30 行 + 静态资源）
│   ├── embed.go  index.html  style.css  app.js
├── docs/
│   └── PROJECT_DOC.md         # 本文档
└── README.md                  # 使用说明（构建/运行/演示步骤）
```

---

## 10. 开发里程碑

| 阶段 | 内容 | 产出 |
|------|------|------|
| M1（第 1~2 天） | 搭建模块骨架；实现 record/checksum/errors/options | 可编解码与校验的日志帧 |
| M2（第 3~5 天） | segment/buffer/wal 主路径：追加、滚动、批量刷盘 | 可追加可读取的 WAL |
| M3（第 6~7 天） | reader/replay/recovery/truncate/fsync/index | 崩溃恢复链路打通 |
| M4（第 8 天） | store 层：状态机、快照、双写顺序 | 演示存储可用 |
| M5（第 9 天） | api 层 + web 前端 + embed | 单二进制可视化演示 |
| M6（第 10 天） | 测试补齐（含崩溃注入专项）、行数/文件数校准、README | 满足全部验收标准 |

---

## 11. 验收标准

1. **功能**：追加写、checksum 校验、批量刷盘、启动重放全部可用；崩溃注入恢复后数据零丢失零损坏（专项测试 ≥ 100 轮通过）。
2. **代码量**：`wc -l` 统计非测试 `.go` 文件，总行数 **2000 < N < 2200**；文件数 **20 < M < 25**（本设计为 22 文件 / 2030 行）。
3. **前端**：单二进制启动后访问 `http://127.0.0.1:8888/` 即可完成"写入→刷盘→崩溃→恢复→校验"完整演示。
4. **质量**：`go vet ./...` 与 `go test -race ./...` 全部通过；仅依赖标准库。
5. **文档**：本文档与 README 覆盖构建、运行、演示与设计决策。

---

## 12. 风险与应对

| 风险 | 影响 | 应对 |
|------|------|------|
| 行数超限（>2200 或 <2000） | 不满足硬性约束 | 第 5.3 节调控策略 + 每周 `wc -l` 检查 |
| fsync 语义误用导致假一致 | 崩溃数据丢失 | 刷盘路径仅依赖 `File.Sync()`；专项测试验证 |
| 分片记录跨段滚动 | 重放逻辑复杂 | 分片记录禁止跨段：滚动前强制闭合当前记录 |
| 演示崩溃端点误伤生产 | 数据丢失风险 | 仅回环绑定 + 明确警告 + 默认可配置关闭 |
| 尾部撕裂与损坏叠加 | 恢复结果不确定 | 损坏点与尾部截断分别处理，恢复结果显式返回 `truncated` 标志 |
