/**
* @File    :   base_sink.go
* @Date    :   2025/10/20 14:15:07
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package sink

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/cache"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
)

// SQLBuilder 接口定义了构建SQL语句的方法
type SQLBuilder interface {
	EscapeValue(value interface{}) string
	BuildWhereCondition(column string, value interface{}) string
	IsPrimaryKey(table *schema.Table, index int) bool
	BuildInsertSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string
	BuildUpdateSQL(e *canal.RowsEvent, oldRow []interface{}, row []interface{}, targetTable string) string
	BuildDeleteSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string
}

// Database 接口定义了数据库连接的基本操作
type Database interface {
	BeginTx() (interface{}, error)
	Commit(tx interface{}) error
	Rollback(tx interface{}) error
	Exec(query string, args ...interface{}) (interface{}, error)
	ExecInTx(tx interface{}, query string, args ...interface{}) (interface{}, error)
	Close() error
}

// BaseSink 包含了所有数据库sink的共同功能
type BaseSink struct {
	DB            Database
	BatchSize     int
	BatchTimeout  time.Duration
	batchBuffer   []string // 用来存sql
	Config        *config.Config
	EventHandler  interface{} // 这里应该是EventHandler的接口，但为了简化暂时使用interface{}
	PositionStore cache.PositionStore
	Position      mysql.Position
	SQLBuilder    SQLBuilder
	DatabaseName  string     // 用于日志中区分数据库类型
	mu            sync.Mutex // 🔒 用于保护 batchBuffer
}

// NewBaseSink 创建一个新的BaseSink实例
func NewBaseSink(db Database, cfg *config.Config, batchSize int, batchTimeout time.Duration,
	eventHandler interface{}, posStore cache.PositionStore, position mysql.Position, sqlBuilder SQLBuilder, dbName string) *BaseSink {
	return &BaseSink{
		DB:            db,
		BatchSize:     batchSize,
		BatchTimeout:  batchTimeout,
		batchBuffer:   make([]string, 0),
		Config:        cfg,
		EventHandler:  eventHandler,
		PositionStore: posStore,
		Position:      position,
		SQLBuilder:    sqlBuilder,
		DatabaseName:  dbName,
	}
}

// execOrBatch 根据配置执行 SQL 或加入批处理
func (s *BaseSink) execOrBatch(action string, sql string) error {
	if s.Config.Batch.Enabled {
		if err := s.AddToBatch(sql); err != nil {
			return fmt.Errorf("failed to add %s SQL to batch for %s: %v, SQL: %s", action, s.DatabaseName, err, sql)
		}
	} else {
		if _, err := s.DB.Exec(sql); err != nil {
			return fmt.Errorf("failed to execute %s SQL for %s: %v, SQL: %s", action, s.DatabaseName, err, sql)
		}
	}
	return nil
}

// Write 处理行事件并构建相应的SQL语句
func (s *BaseSink) Write(e *canal.RowsEvent) error {
	targetTable := s.getTargetTable(e.Table.Schema, e.Table.Name)

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			sql := s.SQLBuilder.BuildInsertSQL(e, row, targetTable)
			if err := s.execOrBatch("insert", sql); err != nil {
				return err
			}
		}
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			if i+1 >= len(e.Rows) {
				break
			}
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]
			sql := s.SQLBuilder.BuildUpdateSQL(e, oldRow, newRow, targetTable)
			if err := s.execOrBatch("update", sql); err != nil {
				return err
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			sql := s.SQLBuilder.BuildDeleteSQL(e, row, targetTable)
			if err := s.execOrBatch("delete", sql); err != nil {
				return err
			}
		}
	default:
		log.Printf("Unknown action: %s", e.Action)
	}

	return nil
}

// AddToBatch 添加 SQL 到缓冲区，必要时触发批量写入
func (s *BaseSink) AddToBatch(sql string) error {
	s.mu.Lock()

	s.batchBuffer = append(s.batchBuffer, sql)
	needFlush := len(s.batchBuffer) >= s.BatchSize

	var batch []string
	if needFlush {
		// swap 技巧：减少锁持有时间，避免 I/O 阻塞 AddToBatch
		batch = s.batchBuffer
		s.batchBuffer = nil
	}

	s.mu.Unlock()

	// 批量执行放到锁外
	if needFlush {
		return s.writeBatch(batch)
	}

	return nil
}

// FlushBatch 执行所有未满批次的 SQL
func (s *BaseSink) FlushBatch() error {
	s.mu.Lock()

	if len(s.batchBuffer) == 0 {
		s.mu.Unlock()
		return nil
	}

	// 同样 swap 出当前 batch
	batch := s.batchBuffer
	s.batchBuffer = nil

	s.mu.Unlock()

	return s.writeBatch(batch)
}

// WriteBatch（外部调用时）执行批处理
// 在优化后的模型内它只是 FlushBatch 的别名
func (s *BaseSink) WriteBatch() error {
	return s.FlushBatch()
}

// writeBatch 执行 SQL 批量写入，不上锁
func (s *BaseSink) writeBatch(batch []string) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := s.DB.BeginTx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction for %s: %v", s.DatabaseName, err)
	}

	for _, sql := range batch {
		if _, err := s.DB.ExecInTx(tx, sql); err != nil {
			_ = s.DB.Rollback(tx)
			return fmt.Errorf("batch execution failed for %s: %v, SQL: %s",
				s.DatabaseName, err, sql)
		}
	}

	if err := s.DB.Commit(tx); err != nil {
		return fmt.Errorf("failed to commit transaction for %s: %v", s.DatabaseName, err)
	}

	log.Printf("Successfully executed %d SQL statements for %s",
		len(batch), s.DatabaseName)

	return nil
}

// getTargetTable 获取目标表名
func (s *BaseSink) getTargetTable(schema string, table string) string {
	for _, tableConfig := range s.Config.Tables {
		// fmt.Printf("配置的表名")
		if tableConfig.TargetTable == table {
			switch s.Config.Sink.Type {
			case "mysql":
				return tableConfig.TargetTable
			case "postgres":
				return tableConfig.TargetTable
			case "elasticsearch":
				panic("elasticsearch is not completed")
			}
		}
	}
	// 默认使用原表名
	return fmt.Sprintf("%s.%s", schema, table)
}

// Close 关闭数据库连接
func (s *BaseSink) Close() error {
	// 刷新批处理缓冲区
	if err := s.FlushBatch(); err != nil {
		return fmt.Errorf("failed to flush batch for %s: %v", s.DatabaseName, err)
	}
	return s.DB.Close()
}

