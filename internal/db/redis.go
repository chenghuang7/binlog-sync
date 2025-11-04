/**
* @File    :   redis.go
* @Date    :   2025/10/24 14:41:56
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   Redis单例和缓存操作
**/

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/go-redis/redis/v8"
)

// RedisClient Redis客户端单例
type RedisClient struct {
	client *redis.Client
	ctx    context.Context
}

// 全局 map 管理不同 DB 的 RedisClient
var redisInstances = make(map[int]*RedisClient)
var mu sync.Mutex

// 获取指定 DB 的 Redis 实例
func GetRedisInstanceByDB(cfg *config.Config, db int) (*RedisClient, error) {
    mu.Lock()
    defer mu.Unlock()

    // 如果已经初始化过，直接返回
    if instance, ok := redisInstances[db]; ok {
        return instance, nil
    }

    // 创建新实例
    client := &RedisClient{
        ctx: context.Background(),
    }
    client.client = redis.NewClient(&redis.Options{
        Addr:     fmt.Sprintf("%s:%d", cfg.PositionStore.Redis.Host, cfg.PositionStore.Redis.Port),
        Password: cfg.PositionStore.Redis.Password,
        DB:       db,
    })

    // 测试连接
    if _, err := client.client.Ping(client.ctx).Result(); err != nil {
        return nil, fmt.Errorf("连接Redis DB %d 失败: %v", db, err)
    }

    redisInstances[db] = client
    return client, nil
}

// Set 存储键值对
func (r *RedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(r.ctx, key, value, expiration).Err()
}

// Get 获取键值
func (r *RedisClient) Get(key string) (string, error) {
	return r.client.Get(r.ctx, key).Result()
}

// GetBytes 获取键值并返回字节数组
func (r *RedisClient) GetBytes(key string) ([]byte, error) {
	val, err := r.client.Get(r.ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	return val, nil
}

// SetJSON 存储JSON格式的数据
func (r *RedisClient) SetJSON(key string, value interface{}, expiration time.Duration) error {
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("JSON序列化失败: %v", err)
	}
	return r.client.Set(r.ctx, key, jsonBytes, expiration).Err()
}

// GetJSON 获取JSON格式的数据
func (r *RedisClient) GetJSON(key string, dest interface{}) error {
	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(val), dest)
}

// Del 删除键
func (r *RedisClient) Del(keys ...string) error {
	return r.client.Del(r.ctx, keys...).Err()
}

// Exists 检查键是否存在
func (r *RedisClient) Exists(keys ...string) (int64, error) {
	return r.client.Exists(r.ctx, keys...).Result()
}

// Expire 设置键的过期时间
func (r *RedisClient) Expire(key string, expiration time.Duration) error {
	return r.client.Expire(r.ctx, key, expiration).Err()
}

// TTL 获取键的剩余过期时间
func (r *RedisClient) TTL(key string) (time.Duration, error) {
	return r.client.TTL(r.ctx, key).Result()
}

// HSet 设置哈希表字段
func (r *RedisClient) HSet(key string, values ...interface{}) error {
	return r.client.HSet(r.ctx, key, values...).Err()
}

// HGet 获取哈希表字段值
func (r *RedisClient) HGet(key, field string) (string, error) {
	return r.client.HGet(r.ctx, key, field).Result()
}

// HGetAll 获取哈希表所有字段和值
func (r *RedisClient) HGetAll(key string) (map[string]string, error) {
	return r.client.HGetAll(r.ctx, key).Result()
}

// HDel 删除哈希表字段
func (r *RedisClient) HDel(key string, fields ...string) error {
	return r.client.HDel(r.ctx, key, fields...).Err()
}

// LPush 将值插入列表头部
func (r *RedisClient) LPush(key string, values ...interface{}) error {
	return r.client.LPush(r.ctx, key, values...).Err()
}

// RPush 将值插入列表尾部
func (r *RedisClient) RPush(key string, values ...interface{}) error {
	return r.client.RPush(r.ctx, key, values...).Err()
}

// LPop 移出并获取列表的第一个元素
func (r *RedisClient) LPop(key string) (string, error) {
	return r.client.LPop(r.ctx, key).Result()
}

// RPop 移出并获取列表的最后一个元素
func (r *RedisClient) RPop(key string) (string, error) {
	return r.client.RPop(r.ctx, key).Result()
}

// LLen 获取列表长度
func (r *RedisClient) LLen(key string) (int64, error) {
	return r.client.LLen(r.ctx, key).Result()
}

// LRange 获取列表指定范围内的元素
func (r *RedisClient) LRange(key string, start, stop int64) ([]string, error) {
	return r.client.LRange(r.ctx, key, start, stop).Result()
}

// SAdd 向集合添加成员
func (r *RedisClient) SAdd(key string, members ...interface{}) error {
	return r.client.SAdd(r.ctx, key, members...).Err()
}

// SMembers 获取集合所有成员
func (r *RedisClient) SMembers(key string) ([]string, error) {
	return r.client.SMembers(r.ctx, key).Result()
}

// SRem 移除集合成员
func (r *RedisClient) SRem(key string, members ...interface{}) error {
	return r.client.SRem(r.ctx, key, members...).Err()
}

// SIsMember 判断成员是否在集合中
func (r *RedisClient) SIsMember(key string, member interface{}) (bool, error) {
	return r.client.SIsMember(r.ctx, key, member).Result()
}

// Close 关闭Redis连接
func (r *RedisClient) Close() error {
	return r.client.Close()
}

// Ping 测试Redis连接
func (r *RedisClient) Ping() error {
	_, err := r.client.Ping(r.ctx).Result()
	return err
}

