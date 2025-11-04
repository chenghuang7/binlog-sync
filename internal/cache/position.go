/**
* @file    :   position.go
* @date    :   2025/10/20 14:14:15
* @author  :   seestars
* @version :   1.0
* @desc    :   none
**/

package cache

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/db"
	"github.com/go-mysql-org/go-mysql/mysql"
)

type PositionStore interface {
	Load() (mysql.Position, error)
	Save(mysql.Position) error
	Close() error
}

////////////////////////
// 文件实现
////////////////////////

type FilePositionStore struct {
	Path string
}

func (f *FilePositionStore) Load() (mysql.Position, error) {
	var pos mysql.Position
	if _, err := os.Stat(f.Path); os.IsNotExist(err) {
		return pos, err
	}
	data, err := os.ReadFile(f.Path)
	if err != nil {
		return pos, err
	}
	err = json.Unmarshal(data, &pos)
	return pos, err
}

func (f *FilePositionStore) Save(pos mysql.Position) error {
	data, err := json.Marshal(pos)
	if err != nil {
		return err
	}
	return os.WriteFile(f.Path, data, 0644)
}

func (f *FilePositionStore) Close() error {
	return nil
}

////////////////////////
// Redis 实现
////////////////////////

type RedisPositionStore struct {
	Key    string
	Client *db.RedisClient
}

// NewRedisPositionStore 创建一个新的Redis位置存储
func NewRedisPositionStore(cfg *config.Config, key string) (*RedisPositionStore, error) {
	client, err := db.GetRedisInstanceByDB(cfg, cfg.PositionStore.Redis.DB)
	if err != nil {
		return nil, err
	}
	return &RedisPositionStore{
		Key:    key,
		Client: client,
	}, nil
}

func (r *RedisPositionStore) Load() (mysql.Position, error) {
	var pos mysql.Position
	val, err := r.Client.Get(r.Key)
	if err != nil {
		return pos, nil // 不存在则返回空位点
	}
	err = json.Unmarshal([]byte(val), &pos)
	return pos, err
}

func (r *RedisPositionStore) Save(pos mysql.Position) error {
	fmt.Println(pos)
	return r.Client.SetJSON(r.Key, pos, 0)
}

// Close 关闭Redis连接
func (r *RedisPositionStore) Close() error {
	return r.Client.Close()
}
