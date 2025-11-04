/**
* @File    :   es_sink.go
* @Date    :   2025/10/20 14:15:47
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   Elasticsearch数据写入实现
**/

package sink

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/cache"
	"github.com/changhuang7/binlog-sync/internal/db"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
)

// ESSink Elasticsearch数据写入接口
type ESSink struct {
	*BaseSink
	esClient     *db.ESClient
	DatabaseName string
	batch        []db.ESEvent    // 批处理缓冲
}

func (s *ESSink) FlushBatch() error {
	if len(s.batch) == 0 {
		return nil
	}

	err := s.esClient.Bulk(s.batch)
	if err != nil {
		log.Printf("❌ ES bulk 写入失败: %v", err)
		// 可以 retry（建议）
	}

	s.batch = s.batch[:0] // 清空
	return nil
}

// NewESSink 创建Elasticsearch写入实例
func NewESSink(config *config.Config, eventHandler interface{}, posStore cache.PositionStore, position mysql.Position) (*ESSink, error) {
	if config == nil || config.Sink.Elasticsearch.Address == "" {
		return nil, fmt.Errorf("invalid Elasticsearch sink configuration")
	}

	esConfig := db.ESConfig{
		Address:  config.Sink.Elasticsearch.Address,
		Index:    config.Sink.Elasticsearch.Index,
		Username: config.Sink.Elasticsearch.Username,
		Password: config.Sink.Elasticsearch.Password,
	}

	esClient, err := db.GetES(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	baseSink := &BaseSink{
		Config:        config,
		BatchSize:     config.Batch.Size,
		BatchTimeout:  time.Duration(config.Batch.Timeout) * time.Second,
		batchBuffer:   make([]string, 0, config.Batch.Size),
		PositionStore: posStore,
		Position:      position,
		DatabaseName:  "Elasticsearch",
	}

	return &ESSink{
		BaseSink:     baseSink,
		esClient:     esClient,
		DatabaseName: "Elasticsearch",
	}, nil
}

// OnRow 把 binlog 事件转成 ES 批处理事件，不直接写 ES
func (s *ESSink) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	targetTable := s.getTargetTable(table.Name)
	switch e.Action {

	case canal.InsertAction:
		for _, row := range e.Rows {
			doc := s.rowToDocument(table, row)
			id := s.getPrimaryKeyValue(table, row)

			s.batch = append(s.batch, db.ESEvent{
				Index: targetTable,
				Action: "index",
				ID:     id,
				Doc:    doc,
			})
		}

	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]

			oldID := s.getPrimaryKeyValue(table, oldRow)
			newID := s.getPrimaryKeyValue(table, newRow)
			doc := s.rowToDocument(table, newRow)

			// 主键改变 = delete old + index new
			if oldID != newID {
				s.batch = append(s.batch, db.ESEvent{
					Index: targetTable,
					Action: "delete",
					ID:     oldID,
			})
			}

			// 主键没变 = index 覆盖即可（等价于 upsert）
			s.batch = append(s.batch, db.ESEvent{
				Index: targetTable,
				Action: "index",
				ID:     newID,
				Doc:    doc,
			})
		}

	case canal.DeleteAction:
		for _, row := range e.Rows {
			id := s.getPrimaryKeyValue(table, row)
			s.batch = append(s.batch, db.ESEvent{
				Index: targetTable,
				Action: "delete",
				ID:     id,
			})
		}

	default:
		return fmt.Errorf("unsupported action: %s", e.Action)
	}
	if len(s.batch) >= s.BatchSize {
		if err := s.FlushBatch(); err != nil {
			return fmt.Errorf("failed to flush ES batch: %w", err)
		}
	}

	// log.Printf("✅ ES 队列接收 %s 操作: %s.%s", e.Action, e.Table.Schema, targetTable)
	return nil
}

// rowToDocument 将行数据转换为文档
func (s *ESSink) rowToDocument(table *schema.Table, row []interface{}) map[string]interface{} {
	doc := make(map[string]interface{})

	for i, value := range row {
		if i < len(table.Columns) {
			columnName := table.Columns[i].Name
			doc[columnName] = value
		}
	}

	return doc
}

// getPrimaryKeyValue 获取主键值作为文档ID
func (s *ESSink) getPrimaryKeyValue(table *schema.Table, row []interface{}) string {
	// 存在主键（可以是多个）
	if len(table.PKColumns) > 0 {
		parts := make([]string, 0, len(table.PKColumns))

		for _, idx := range table.PKColumns {
			if idx >= len(row) {
				parts = append(parts, "NULL")
				continue
			}

			val := row[idx]
			switch v := val.(type) {
			case int64:
				parts = append(parts, strconv.FormatInt(v, 10))
			case []uint8: // MySQL 的 VARCHAR 可能是 []uint8
				parts = append(parts, string(v))
			case string:
				parts = append(parts, v)
			default:
				parts = append(parts, fmt.Sprintf("%v", v))
			}
		}

		// 主键拼接成一个稳定字符串，例如 "1001|8899|A12"
		keyStr := strings.Join(parts, "|")

		// 做 SHA-1，长度适中且碰撞率非常低
		h := sha1.Sum([]byte(keyStr))
		// log.Printf("转化id from %s -> %s", keyStr, hex.EncodeToString(h[:]))
		return hex.EncodeToString(h[:])

	}

	// 没主键，用时间戳兜底
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// getTargetTable 获取目标表名
func (s *ESSink) getTargetTable(sourceTable string) string {
	for _, tableConfig := range s.Config.Tables {
		if tableConfig.SourceTable == sourceTable && tableConfig.TargetTable != "" {
			return tableConfig.TargetTable
		}
	}
	return sourceTable
}

// Close 关闭连接
func (s *ESSink) Close() error {
	// if s.esClient != nil {
	// 	return s.esClient.Close()
	// }
	return nil
}
