/**
* @file    :   mysql_sink.go
* @date    :   2025/10/20 14:15:07
* @author  :   seestars
* @version :   2.0
* @desc    :   mysql数据写入实现
**/

package sink

import (
	"fmt"
	"log"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/cache"
	"github.com/changhuang7/binlog-sync/internal/db"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// MySQLSink MySQL数据写入接口
type MySQLSink struct {
	*BaseSink
}

// NewMySQLSink 创建MySQL写入实例
func NewMySQLSink(config *config.Config, eventHandler interface{}, posStore cache.PositionStore, position mysql.Position) (*MySQLSink, error) {
	if config == nil || config.Sink.MySQL.Host == "" {
		return nil, fmt.Errorf("invalid MySQL sink configuration")
	}

	// 构建DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		config.Sink.MySQL.User,
		config.Sink.MySQL.Password,
		config.Sink.MySQL.Host,
		config.Sink.MySQL.Port,
		config.Sink.MySQL.Database,
		config.Sink.MySQL.Charset,
	)

	// 创建数据库配置
	dbConfig := db.DBConfig{
		Host:         config.Sink.MySQL.Host,
		Port:         config.Sink.MySQL.Port,
		User:         config.Sink.MySQL.User,
		Password:     config.Sink.MySQL.Password,
		Database:     config.Sink.MySQL.Database,
		Charset:      config.Sink.MySQL.Charset,
		MaxOpenConns: config.ConnectionPool.MaxOpen,
		MaxIdleConns: config.ConnectionPool.MaxIdle,
		MaxLifetime:  time.Duration(config.ConnectionPool.MaxLifetime) * time.Second,
	}

	// 获取数据库连接
	db, err := db.GetMySQL(dsn, dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL sink: %v", err)
	}

	// 创建MySQL数据库适配器
	mysqlDB := NewMySQLDatabase(db)

	// 创建MySQL SQL构建器
	sqlBuilder := NewMySQLSQLBuilder()

	// 创建基础Sink
	baseSink := NewBaseSink(
		mysqlDB,
		config,
		config.Batch.Size,
		time.Duration(config.Batch.Timeout)*time.Second,
		eventHandler,
		posStore,
		position,
		sqlBuilder,
		"MySQL",
	)

	return &MySQLSink{
		BaseSink: baseSink,
	}, nil
}

// Close 关闭数据库连接
func (s *MySQLSink) Close() error {
	log.Println("closing MySQL sink connection")
	return s.BaseSink.Close()
}
