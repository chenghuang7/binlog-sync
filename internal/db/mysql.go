/**
* @File    :   mysql.go
* @Date    :   2025/10/20 14:16:45
* @Author  :   SeeStars
* @Version :   2.0
* @Desc    :   MySQL数据库连接池实现
**/

// internal/db/mysql.go
package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// DBConfig 数据库配置
type DBConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	Database     string
	Charset      string
	MaxOpenConns int
	MaxIdleConns int
	MaxLifetime  time.Duration
}

// MySQLDB MySQL数据库连接封装
type MySQLDB struct {
	Conn   *sql.DB
	config DBConfig
	mu     sync.RWMutex
}

// 连接池管理
type ConnectionPool struct {
	pools map[string]*MySQLDB
	mu    sync.RWMutex
}

// 全局连接池管理器
var (
	globalPool     *ConnectionPool
	oncePoolInit   sync.Once
)

// 初始化全局连接池
func init() {
	oncePoolInit.Do(func() {
		globalPool = &ConnectionPool{
			pools: make(map[string]*MySQLDB),
		}
	})
}

// GetConnectionPool 获取全局连接池实例
func GetConnectionPool() *ConnectionPool {
	return globalPool
}

// GetMySQL 获取MySQL连接（工厂方法）
func GetMySQL(dsn string, config DBConfig) (*MySQLDB, error) {
	pool := GetConnectionPool()
	return pool.GetOrCreateDB(dsn, config)
}

// GetOrCreateDB 获取或创建数据库连接
func (p *ConnectionPool) GetOrCreateDB(dsn string, config DBConfig) (*MySQLDB, error) {
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

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %v", err)
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
		return nil, fmt.Errorf("failed to ping MySQL: %v", err)
	}

	db = &MySQLDB{
		Conn:   conn,
		config: config,
	}
	p.pools[dsn] = db

	log.Printf("MySQL连接池创建成功: %s/%s", config.Host, config.Database)
	return db, nil
}

// Close 关闭指定连接
func (p *ConnectionPool) Close(dsn string) error {
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
func (p *ConnectionPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for dsn, db := range p.pools {
		if err := db.Conn.Close(); err != nil {
			log.Printf("Error closing MySQL connection %s: %v", dsn, err)
		}
		delete(p.pools, dsn)
	}
}

// Query 执行查询
func (m *MySQLDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Query(query, args...)
}

// QueryContext 带上下文的查询
func (m *MySQLDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.QueryContext(ctx, query, args...)
}

// Exec 执行命令
func (m *MySQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Exec(query, args...)
}

// ExecContext 带上下文的执行命令
func (m *MySQLDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.ExecContext(ctx, query, args...)
}

// Begin 开始事务
func (m *MySQLDB) Begin() (*sql.Tx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Begin()
}

// BeginTx 带上下文的开始事务
func (m *MySQLDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.BeginTx(ctx, opts)
}

// Stats 获取连接池状态
func (m *MySQLDB) Stats() sql.DBStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Conn.Stats()
}
