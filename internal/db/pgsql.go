/**
* @File    :   pgsql.go
* @Date    :   2025/10/20 14:17:04
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   PostgreSQL数据库连接池实现
**/

package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// PgSQLDB PostgreSQL数据库连接封装
type PgSQLDB struct {
	Conn   *sql.DB
	config DBConfig
	mu     sync.RWMutex
}

// PostgreSQL连接池管理
type PgSQLConnectionPool struct {
	pools map[string]*PgSQLDB
	mu    sync.RWMutex
}

// PostgreSQL全局连接池管理器
var (
	pgsqlGlobalPool *PgSQLConnectionPool
	oncePgSQLPoolInit sync.Once
)

// 初始化PostgreSQL全局连接池
func init() {
	oncePgSQLPoolInit.Do(func() {
		pgsqlGlobalPool = &PgSQLConnectionPool{
			pools: make(map[string]*PgSQLDB),
		}
	})
}

// GetPgSQLConnectionPool 获取PostgreSQL全局连接池实例
func GetPgSQLConnectionPool() *PgSQLConnectionPool {
	return pgsqlGlobalPool
}

// GetPgSQL 获取PostgreSQL连接（工厂方法）
func GetPgSQL(dsn string, config DBConfig) (*PgSQLDB, error) {
	pool := GetPgSQLConnectionPool()
	return pool.GetOrCreateDB(dsn, config)
}

// GetOrCreateDB 获取或创建PostgreSQL数据库连接
func (p *PgSQLConnectionPool) GetOrCreateDB(dsn string, config DBConfig) (*PgSQLDB, error) {
	p.mu.RLock()
	db, exists := p.pools[dsn]
	p.mu.RUnlock()

	if exists {
		return db, nil
	}

	// 不存在则创建新连接
	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if db, exists = p.pools[dsn]; exists {
		return db, nil
	}

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	// 设置连接池参数
	maxOpen := config.MaxOpenConns
	maxIdle := config.MaxIdleConns
	maxLifetime := config.MaxLifetime

	if maxOpen <= 0 {
		maxOpen = 10
	}
	if maxIdle <= 0 {
		maxIdle = 5
	}
	if maxLifetime <= 0 {
		maxLifetime = 5 * time.Minute
	}

	conn.SetMaxOpenConns(maxOpen)
	conn.SetMaxIdleConns(maxIdle)
	conn.SetConnMaxLifetime(maxLifetime)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping PostgreSQL: %v", err)
	}

	db = &PgSQLDB{
		Conn:   conn,
		config: config,
	}
	p.pools[dsn] = db

	log.Printf("PostgreSQL连接池创建成功: %s/%s", config.Host, config.Database)
	return db, nil
}

// Close 关闭指定连接
func (p *PgSQLConnectionPool) Close(dsn string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if db, exists := p.pools[dsn]; exists {
		err := db.Conn.Close()
		delete(p.pools, dsn)
		return err
	}
	return nil
}

// CloseAll 关闭所有连接
func (p *PgSQLConnectionPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for dsn, db := range p.pools {
		if err := db.Conn.Close(); err != nil {
			log.Printf("Error closing PostgreSQL connection %s: %v", dsn, err)
		}
		delete(p.pools, dsn)
	}
}

// Query 执行查询
func (m *PgSQLDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Query(query, args...)
}

// QueryContext 带上下文的查询
func (m *PgSQLDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.QueryContext(ctx, query, args...)
}

// Exec 执行命令
func (m *PgSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Exec(query, args...)
}

// ExecContext 带上下文的执行命令
func (m *PgSQLDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (m *PgSQLDB) Begin() (*sql.Tx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Begin()
}

// BeginTx 带上下文的开始事务
func (m *PgSQLDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.BeginTx(ctx, opts)
}

// Stats 获取连接池状态
func (m *PgSQLDB) Stats() sql.DBStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Stats()
}