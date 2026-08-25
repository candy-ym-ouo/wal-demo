# wal-demo

一个只依赖 Go 标准库的分段预写日志（WAL）与崩溃恢复演示项目。它提供
CRC32 校验、逻辑记录分片、批量刷盘、日志段滚动、尾部撕裂修复、启动重放、
内存 KV、校验快照、HTTP API 和内嵌 Web 控制台。

## 构建与运行

需要 Go 1.21 或更高版本。

```bash
go test -race ./...
go vet ./...
go build -o dist/wald ./cmd/wald
./dist/wald -addr 127.0.0.1:8888 -data ./data
```

浏览器访问 `http://127.0.0.1:8888/`。数据默认写入 `./data`。停止进程后使用
相同的 `-data` 参数重启，程序会扫描日志、校验 CRC、修复尾部残片并恢复 KV。

崩溃演示会直接终止进程，因此默认关闭；仅在本地实验时增加 `-allow-crash`。
服务仍会校验请求来自回环地址。

## 常用 API

```bash
curl -X POST http://127.0.0.1:8888/api/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"world"}'
curl http://127.0.0.1:8888/api/v1/kv/hello
curl -X POST http://127.0.0.1:8888/api/v1/snapshot
curl -X POST http://127.0.0.1:8888/api/v1/snapshots/release-2026-08
curl http://127.0.0.1:8888/api/v1/snapshots
curl http://127.0.0.1:8888/api/v1/metrics
curl -X POST http://127.0.0.1:8888/api/v1/verify
curl http://127.0.0.1:8888/api/v1/segments
```

## 打包

项目无运行时资源依赖，HTML、CSS 和 JavaScript 已嵌入可执行文件。发行压缩包
可用下面的命令产生：

```bash
mkdir -p dist/package
cp dist/wald README.md dist/package/
tar -czf dist/wal-demo-darwin-arm64.tar.gz -C dist/package .
```

## 持久化语义

- `AppendSync` 在返回前执行 `write + fsync`。
- `AppendBatch` 将记录加入组提交缓冲，`Sync` 一次持久化整批数据。
- KV 层始终先持久化 WAL，再更新内存状态。
- 记录分片不会跨日志段；启动扫描只接受完整、连续、CRC 正确的逻辑记录。
- 最后一个段中的短头、短负载或未闭合分片会截断到最后有效边界。
- 中间损坏不会静默跳过，而是返回含段号和偏移的 `CorruptionError`。

原始设计与验收约束见项目内的 `docs/PROJECT_DOC.md`。
