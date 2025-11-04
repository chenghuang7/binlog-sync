/**
* @File    :   pgsql_database.go
* @Date    :   2025/10/20 14:15:31
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   PostgreSQL数据库操作实现
**/

package sink

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/changhuang7/binlog-sync/internal/db"
)

// PgSQLDatabase PostgreSQL数据库操作实现
type PgSQLDatabase struct {
	db *db.PgSQLDB
}

// NewPgSQLDatabase 创建PostgreSQL数据库操作实例
func NewPgSQLDatabase(pgsqlDB *db.PgSQLDB) *PgSQLDatabase {
	return &PgSQLDatabase{
		db: pgsqlDB,
	}
}

// BeginTx 开始事务
func (d *PgSQLDatabase) BeginTx() (interface{}, error) {
	ctx := context.Background()
	return d.db.BeginTx(ctx, nil)
}

// Commit 提交事务
func (d *PgSQLDatabase) Commit(tx interface{}) error {
	if pgTx, ok := tx.(*sql.Tx); ok {
		return pgTx.Commit()
	}
	return fmt.Errorf("invalid transaction type")
}

// Rollback 回滚事务
func (d *PgSQLDatabase) Rollback(tx interface{}) error {
	if pgTx, ok := tx.(*sql.Tx); ok {
		return pgTx.Rollback()
	}
	return fmt.Errorf("invalid transaction type")
}

// Exec 执行SQL
func (d *PgSQLDatabase) Exec(query string, args ...interface{}) (interface{}, error) {
	ctx := context.Background()
	return d.db.ExecContext(ctx, query, args...)
}

// ExecInTx 在事务中执行SQL
func (d *PgSQLDatabase) ExecInTx(tx interface{}, query string, args ...interface{}) (interface{}, error) {
	if pgTx, ok := tx.(*sql.Tx); ok {
		return pgTx.ExecContext(context.Background(), query, args...)
	}
	return nil, fmt.Errorf("invalid transaction type")
}

// Close 关闭数据库连接
func (d *PgSQLDatabase) Close() error {
	return d.db.Conn.Close()
}
