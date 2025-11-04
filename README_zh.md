# Binlog-Sync

[![Go Version](https://img.shields.io/badge/Go-1.18+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

[English](./README.md) | 简体中文

**Binlog-Sync** 是一个高性能、灵活且可靠的工具，用于将MySQL `binlog`事件实时同步到各种目标系统。它使用Go语言构建，具有出色的并发性能和低内存占用。

## 功能特性

- **多目标支持**: 支持将数据同步到 **MySQL**、**PostgreSQL** 和 **Elasticsearch**。
- **实时同步**: 在毫秒级别从MySQL `binlog`中捕获数据变更 (INSERT, UPDATE, DELETE)。
- **高性能**:
    - **并发处理**: 利用Go的并发模型高效处理事件。
    - **批量处理**: 支持对目标系统进行批量写入，以提高吞吐量。
- **可靠与弹性**:
    - **至少一次投递**: 保证每个数据变更至少被成功投递一次。
    - **自动位点管理**: 将`binlog`位点持久化到Redis或本地文件，实现从中断处无缝恢复。

## 工作原理

![架构图](https://your-image-hosting.com/architecture.png)  <!-- 您需要将此URL替换为实际的图片链接 -->

1.  **Binlog捕获**: 工具作为从库连接到源MySQL数据库，并使用 `go-mysql` 库捕获基于 `ROW` 的`binlog`事件。
2.  **事件处理**: `EventHandler` 处理`binlog`事件流。对于每个 `RowsEvent`（代表数据变更），它会根据表配置过滤事件。
3.  **异步处理**: 经过滤的事件被推送到一个channel中，由工作goroutine进行异步处理。这将事件捕获与数据写入解耦，并提供了背压机制。
4.  **批量与写入 (Sink)**: `Sink`工作协程从channel中消费事件，根据大小和时间进行批处理，构建相应的SQL/ES请求，并将其写入目标数据存储（MySQL、PostgreSQL或Elasticsearch）。
5.  **位点管理**: 数据成功写入目标后，`binlog`位点 (`pos`) 会被定期、安全地持久化。如果发生重启，进程将从最后保存的位点恢复，确保数据完整性。

## 快速开始

### 前提条件

- **Go**: 1.18或更高版本。
- **MySQL**: 5.7或更高版本，并启用`binlog`。

### 安装

1.  **克隆仓库:**
    ```bash
    git clone https://github.com/your-username/binlog-sync.git
    cd binlog-sync
    ```

2.  **安装依赖:**
    ```bash
    go mod tidy
    ```

3.  **构建二进制文件:**
    ```bash
    go build -o ./bin/binlog-sync ./cmd/binlog-sync/main.go
    ```

## 配置

配置在 `config/config.yaml` 文件中管理。

## 使用方法

### 独立运行


1.  **修改 `config/config.yaml`** 以匹配您的环境。
2.  **运行应用程序:**
    ```bash
    ./bin/binlog-sync --config=./config/config.yaml
    ```

## 项目结构

```
/
├── cmd/                  # 主程序入口
├── config/               # 配置文件 (应用、docker等)
├── docker-compose.yaml   # 用于完整环境设置的Docker Compose文件
├── global/               # 全局应用配置对象
├── go.mod                # Go模块依赖
├── internal/             # 核心应用逻辑
│   ├── binlog/           # Binlog事件捕获和处理
│   ├── cache/            # 位点和schema缓存管理
│   ├── db/               # 数据库连接管理 (MySQL, PG, Redis, ES)
│   └── sink/             # 将数据写入目标系统的逻辑
└── README.md
```

## 许可证

本项目采用MIT许可证 - 详情请见 [LICENSE](LICENSE) 文件。