基于 Go 实现的分段预写日志（WAL）与崩溃恢复 Web 项目，一款后端服务，提供持久化 KV、日志校验、快照恢复与可视化管理功能。

# wal-demo 评测说明

## 环境要求

- Go 1.21 或更高版本
- Docker（使用评测镜像时需要）

项目仅使用 Go 标准库，不依赖外部 Go 模块或前端构建工具。Web 页面通过 `go:embed` 编译进可执行文件。

## 标准验证命令

```bash
go mod tidy
go vet ./...
go test ./...
go build ./...
```

并发安全与竞态验证：

```bash
go test -race ./...
```

## 本地运行

```bash
go run ./cmd/wald -addr 127.0.0.1:8888 -data ./data
```

浏览器访问 `http://127.0.0.1:8888/`。使用 `Ctrl+C` 可触发优雅退出；再次使用相同的 `-data` 目录启动时，会从 WAL 和快照恢复数据。

## 本地构建

```bash
mkdir -p dist
go build -trimpath -o dist/wald ./cmd/wald
./dist/wald -addr 127.0.0.1:8888 -data ./data
```

## Docker 多架构构建

```bash
chmod +x build_benzhi_docker.sh
./build_benzhi_docker.sh wal-demo linux/arm64
./build_benzhi_docker.sh wal-demo linux/amd64
```

进入镜像并重新验证：

```bash
docker run --rm -it wal-demo:latest
go test ./...
go build ./...
```

评测镜像保留完整 Go 工具链，默认进入 Bash，便于继续修改、构建和测试。
