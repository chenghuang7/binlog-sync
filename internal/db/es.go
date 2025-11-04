/**
* @File    :   es.go
* @Date    :   2025/10/20 14:17:17
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   Elasticsearch数据库连接和操作
**/

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type ESEvent struct {
	Index  string                 // 索引名称
	Action string                 // index/update/delete
	ID     string                 // 主键
	Doc    map[string]interface{} // 文档内容，可为空（delete）
}

// ESConfig 配置
type ESConfig struct {
	Address    string
	Username   string
	Password   string
	Index      string
	MaxRetries int
}

// ESClient ES连接封装
type ESClient struct {
	Client *elasticsearch.Client
	config ESConfig
	mu     sync.RWMutex
}

// ESPool 全局ES连接池
type ESPool struct {
	pools map[string]*ESClient
	mu    sync.RWMutex
}

var (
	globalESPool   *ESPool
	onceESPoolInit sync.Once
)

// 初始化全局池
func init() {
	onceESPoolInit.Do(func() {
		globalESPool = &ESPool{
			pools: make(map[string]*ESClient),
		}
	})
}

func GetESPool() *ESPool {
	return globalESPool
}

// GetES 获取/创建 ESClient
func GetES(cfg ESConfig) (*ESClient, error) {
	return GetESPool().GetOrCreate(cfg)
}

// GetOrCreate 获取或创建连接
func (p *ESPool) GetOrCreate(cfg ESConfig) (*ESClient, error) {
	p.mu.RLock()
	client, exists := p.pools[cfg.Address]
	p.mu.RUnlock()

	if exists {
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 双重检查
	if client, exists = p.pools[cfg.Address]; exists {
		return client, nil
	}

	esCfg := elasticsearch.Config{
		Addresses: []string{cfg.Address},
		Username:  cfg.Username,
		Password:  cfg.Password,
		MaxRetries: func() int {
			if cfg.MaxRetries > 0 {
				return cfg.MaxRetries
			}
			return 3
		}(),
	}

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %v", err)
	}

	// Ping: ES 使用 Info()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	info, err := es.Info(es.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to ping ES: %v", err)
	}
	defer info.Body.Close()

	if info.IsError() {
		return nil, fmt.Errorf("elasticsearch error: %s", info.String())
	}

	client = &ESClient{
		Client: es,
		config: cfg,
	}
	p.pools[cfg.Address] = client

	log.Printf("Elasticsearch连接成功: %s", cfg.Address)

	return client, nil
}

// Close 单个实例关闭
func (p *ESPool) Close(address string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pools, address)
}

// CloseAll 清空池
func (p *ESPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr := range p.pools {
		delete(p.pools, addr)
	}
}

func (e *ESClient) Bulk(events []ESEvent) error {
	if len(events) == 0 {
		return nil
	}

	var buf bytes.Buffer

	for _, evt := range events {

		// 写 action 行
		meta := map[string]map[string]string{
			evt.Action: {
				"_index": e.config.Index + "." + evt.Index,
				"_id":    evt.ID,
			},
		}

		metaLine, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("marshal meta failed: %w", err)
		}
		buf.Write(metaLine)
		buf.WriteByte('\n')

		// 写文档行（delete 无文档）
		if evt.Action != "delete" {
			sourceLine, err := json.Marshal(evt.Doc)
			if err != nil {
				return fmt.Errorf("marshal doc failed: %w", err)
			}
			buf.Write(sourceLine)
			buf.WriteByte('\n')
		}
	}

	// --- 2. 发请求 ---
	req := esapi.BulkRequest{
		Body:    bytes.NewReader(buf.Bytes()),
		Refresh: "true",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := req.Do(ctx, e.Client)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	// --- 3. 处理 ES 错误（逐条） ---
	if res.IsError() {
		return fmt.Errorf("bulk response error: %s", res.String())
	}

	var resp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int                    `json:"status"`
			Error  map[string]interface{} `json:"error"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		return fmt.Errorf("decode bulk response failed: %w", err)
	}

	if resp.Errors {
		for i, item := range resp.Items {
			for action, info := range item {
				if info.Status >= 300 {
					log.Printf("❌ ES Bulk error: action=%s index=%s id=%s status=%d error=%v (event index %d)",
						action, e.config.Index+"."+events[i].Index, events[i].ID, info.Status, info.Error, i)
				}
			}
		}
		return fmt.Errorf("bulk contains failed items")
	}

	log.Printf("✅ Bulk 批量写入成功，数量：%d", len(events))
	return nil
}
