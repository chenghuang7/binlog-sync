/**
* @File    :   cache_sql.go
* @Date    :   2025/10/24 14:54:01
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package cache

import (
	"encoding/json"
	"fmt"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/db"
)

type CacheSqlStore interface {
	Load(dbType string) ([]string, error)
	Save(dbType string, sqls []string) error
}

////////////////////////
// 文件实现
////////////////////////

// type FileCacheSqlStore struct {
// 	Path string
// }

// func (f *FileCacheSqlStore) Load() (mysql.Position, error) {
// 	var pos mysql.Position
// 	if _, err := os.Stat(f.Path); os.IsNotExist(err) {
// 		return pos, err
// 	}
// 	data, err := os.ReadFile(f.Path)
// 	if err != nil {
// 		return pos, err
// 	}
// 	err = json.Unmarshal(data, &pos)
// 	return pos, err
// }

// func (f *FileCacheSqlStore) Save(pos mysql.Position) error {
// 	data, err := json.Marshal(pos)
// 	if err != nil {
// 		return err
// 	}
// 	return os.WriteFile(f.Path, data, 0644)
// }

////////////////////////
// Redis 实现
////////////////////////

type RedisCacheSqlStore struct {
	Key    string
	Client *db.RedisClient
}

// NewRedisCacheSqlStore 创建一个新的Redis缓存SQL存储
func NewRedisCacheSqlStore(cfg *config.Config, key string) (*RedisCacheSqlStore, error) {
	client, err := db.GetRedisInstanceByDB(cfg, (cfg.PositionStore.Redis.DB+1)%6)
	if err != nil {
		return nil, err
	}
	return &RedisCacheSqlStore{
		Key:    key,
		Client: client,
	}, nil
}

func (r *RedisCacheSqlStore) Load(dbType string) ([]string, error) {
	var sqls []string
	key := fmt.Sprintf("%s:%s", r.Key, dbType)
	val, err := r.Client.Get(key)
	if err != nil {
		return sqls, nil // 不存在则返回空列表
	}
	err = json.Unmarshal([]byte(val), &sqls)
	return sqls, err
}

func (r *RedisCacheSqlStore) Save(dbType string, sqls []string) error {
	key := fmt.Sprintf("%s:%s", r.Key, dbType)
	fmt.Printf("保存 %d 条SQL语句到缓存 (key: %s)\n", len(sqls), key)
	return r.Client.SetJSON(key, sqls, 0)
}

// Close 关闭Redis连接
func (r *RedisCacheSqlStore) Close() error {
	return r.Client.Close()
}
