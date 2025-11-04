/**
* @File    :   pgsql_sink.go
* @Date    :   2025/10/20 14:15:31
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   PostgreSQL数据写入实现
**/

package sink

import (
	"errors"
	"fmt"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/cache"
	"github.com/changhuang7/binlog-sync/internal/db"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// PgSQLSink PostgreSQL数据写入接口
type PgSQLSink struct {
	*BaseSink
	database   *PgSQLDatabase
	sqlBuilder *PgSQLSQLBuilder
}

// NewPgSQLSink 创建PostgreSQL写入实例
func NewPgSQLSink(config *config.Config, eventHandler interface{}, posStore cache.PositionStore, position mysql.Position) (*PgSQLSink, error) {
	if config == nil || config.Sink.PgSQL.Host == "" {
		return nil, errors.New("invalid PostgreSQL sink configuration")
	}

	// 构建连接字符串
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Sink.PgSQL.Host,
		config.Sink.PgSQL.Port,
		config.Sink.PgSQL.User,
		config.Sink.PgSQL.Password,
		config.Sink.PgSQL.Database,
	)

	// 创建数据库配置
	dbConfig := db.DBConfig{
		Host:         config.Sink.PgSQL.Host,
		Port:         config.Sink.PgSQL.Port,
		User:         config.Sink.PgSQL.User,
		Password:     config.Sink.PgSQL.Password,
		Database:     config.Sink.PgSQL.Database,
		MaxOpenConns: config.ConnectionPool.MaxOpen,
		MaxIdleConns: config.ConnectionPool.MaxIdle,
		MaxLifetime:  time.Duration(config.ConnectionPool.MaxLifetime) * time.Second,
	}

	// 获取数据库连接
	pgsqlDB, err := db.GetPgSQL(connStr, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL sink: %v", err)
	}

	// 创建数据库操作实例
	database := NewPgSQLDatabase(pgsqlDB)

	// 创建SQL构建实例
	sqlBuilder := NewPgSQLSQLBuilder()

	// 创建基础Sink
	baseSink := NewBaseSink(
		database,
		config,
		config.Batch.Size,
		time.Duration(config.Batch.Timeout)*time.Second,
		eventHandler,
		posStore,
		position,
		sqlBuilder,
		"PostgreSQL",
	)

	return &PgSQLSink{
		BaseSink:   baseSink,
		database:   database,
		sqlBuilder: sqlBuilder,
	}, nil
}
