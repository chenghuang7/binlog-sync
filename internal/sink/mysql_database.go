/**
* @File    :   mysql_database.go
* @Date    :   2025/10/20 14:15:07
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package sink

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/changhuang7/binlog-sync/internal/db"
)

// MySQLDatabase 实现Database接口，提供MySQL特定的数据库操作
type MySQLDatabase struct {
	db *db.MySQLDB
}

// NewMySQLDatabase 创建一个新的MySQLDatabase实例
func NewMySQLDatabase(mysqlDB *db.MySQLDB) *MySQLDatabase {
	return &MySQLDatabase{db: mysqlDB}
}

// BeginTx 开始一个事务
func (m *MySQLDatabase) BeginTx() (interface{}, error) {
	ctx := context.Background()
	return m.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// Commit 提交事务
func (m *MySQLDatabase) Commit(tx interface{}) error {
	if mysqlTx, ok := tx.(*sql.Tx); ok {
		return mysqlTx.Commit()
	}
	return fmt.Errorf("invalid transaction type")
}

// Rollback 回滚事务
func (m *MySQLDatabase) Rollback(tx interface{}) error {
	if mysqlTx, ok := tx.(*sql.Tx); ok {
		return mysqlTx.Rollback()
	}
	return fmt.Errorf("invalid transaction type")
}

// Exec 执行SQL语句
func (m *MySQLDatabase) Exec(query string, args ...interface{}) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.db.ExecContext(ctx, query, args...)
}

// ExecInTx 在事务中执行SQL语句
func (m *MySQLDatabase) ExecInTx(tx interface{}, query string, args ...interface{}) (interface{}, error) {
	if mysqlTx, ok := tx.(*sql.Tx); ok {
		return mysqlTx.ExecContext(context.Background(), query, args...)
	}
	return nil, fmt.Errorf("invalid transaction type")
}

// Close 关闭数据库连接
func (m *MySQLDatabase) Close() error {
	return m.db.Conn.Close()
}
