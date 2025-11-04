# Binlog-Sync

[![Go Version](https://img.shields.io/badge/Go-1.18+-blue.svg)](https://golang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

English | [简体中文](./README_zh.md)

**Binlog-Sync** is a high-performance, flexible, and reliable tool for synchronizing MySQL binlog events to various target systems in real-time. Built with Go, it offers excellent concurrency and a low memory footprint.

## Features

- **Multiple Sinks**: Supports synchronizing data to **MySQL**, **PostgreSQL**, and **Elasticsearch**.
- **Real-time Synchronization**: Captures data changes (INSERT, UPDATE, DELETE) from MySQL binlog at the millisecond level.
- **High Performance**:
    - **Concurrent Processing**: Utilizes Go's concurrency model for efficient event handling.
    - **Batch Processing**: Supports batch writing to target systems to improve throughput.
- **Reliable & Resilient**:
    - **At-Least-Once Delivery**: Guarantees that every data change is delivered at least once.
    - **Automatic Position Management**: Persists binlog position to Redis or a local file, enabling seamless recovery from interruptions.

## How It Works

![Architecture Diagram](https://your-image-hosting.com/architecture.png)  <!-- You should replace this with an actual image URL -->

1.  **Binlog Capture**: The tool connects to the source MySQL database as a slave and captures `ROW` based binlog events using the `go-mysql` library.
2.  **Event Handling**: An `EventHandler` processes the stream of binlog events. For each `RowsEvent` (representing data changes), it filters the event based on the table configuration.
3.  **Asynchronous Processing**: Filtered events are pushed into a channel for asynchronous processing by worker goroutines. This decouples event capture from data writing and provides backpressure.
4.  **Batch & Sink**: `Sink` workers consume events from the channel, batch them based on size and time, build the appropriate SQL/ES requests, and write them to the target data store (MySQL, PostgreSQL, or Elasticsearch).
5.  **Position Management**: After the data is successfully written to the target, the binlog position (`pos`) is periodically and safely persisted. In case of a restart, the process resumes from the last saved position, ensuring data integrity.

## Getting Started

### Prerequisites

- **Go**: Version 1.18 or higher.
- **MySQL**: Version 5.7 or higher, with binlog enabled.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/binlog-sync.git
    cd binlog-sync
    ```

2.  **Install dependencies:**
    ```bash
    go mod tidy
    ```

3.  **Build the binary:**
    ```bash
    go build -o ./bin/binlog-sync ./cmd/binlog-sync/main.go
    ```

## Configuration

The configuration is managed in `config/config.yaml`. Below is an example with explanations.

## Usage

### Running Standalone

1.  **Modify `config/config.yaml`** to match your environment.
2.  **Run the application:**
    ```bash
    ./bin/binlog-sync --config=./config/config.yaml
    ```

## Project Structure

```
/
├── cmd/                  # Main application entrypoint
├── config/               # Configuration files (app, docker, etc.)
├── docker-compose.yaml   # Docker Compose for full environment setup
├── global/               # Global application configuration object
├── go.mod                # Go module dependencies
├── internal/             # Core application logic
│   ├── binlog/           # Binlog event capture and handling
│   ├── cache/            # Position and schema cache management
│   ├── db/               # Database connection management (MySQL, PG, Redis, ES)
│   └── sink/             # Logic for writing data to target systems
└── README.md
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.