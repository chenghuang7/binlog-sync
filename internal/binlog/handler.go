/**
* @File    :   handler.go
* @Date    :   2025/10/20 12:02:37
* @Author  :   SeeStars
* @Version :   2.0
* @Desc    :   Binlog事件处理器
**/

package binlog

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/global"
	"github.com/changhuang7/binlog-sync/internal/cache"
	"github.com/changhuang7/binlog-sync/internal/sink"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// SinkType 定义数据输出类型
type SinkType string

const (
	MySQLSink         SinkType = "mysql"
	PostgresSink      SinkType = "postgres"
	ElasticsearchSink SinkType = "elasticsearch"
)

// EventHandler 事件处理器
type EventHandler struct {
	canal.DummyEventHandler
	Config       *config.Config
	ListenInsert bool
	ListenUpdate bool
	ListenDelete bool
	ListenDDL    bool

	// 数据输出接口
	mysqlSink *sink.MySQLSink
	pgsqlSink *sink.PgSQLSink
	esSink    *sink.ESSink

	// 位点管理
	position      mysql.Position
	positionStore cache.PositionStore

	// 批处理相关
	batchMode bool
	batchSize int
	stopChan  chan struct{}

	rowsCh      chan *canal.RowsEvent
	posCommitCh chan mysql.Position
	workersWg   sync.WaitGroup
	commitWg    sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc

	// 原子计数：尚未确认写入目标系统的行数
	pendingEvents int64

	// 统计信息使用原子操作
	insertCount int64
	updateCount int64
	deleteCount int64
	errorCount  int64
}

// NewEventHandler 创建事件处理器
func NewEventHandler() *EventHandler {
	h := &EventHandler{
		Config:       global.AppConfig,
		ListenInsert: true,
		ListenUpdate: true,
		ListenDelete: true,
		ListenDDL:    false,
		batchMode:    global.AppConfig.Batch.Enabled,
		batchSize:    global.AppConfig.Batch.Size,
		stopChan:     make(chan struct{}),

		// 初始化队列：队列大小可由配置决定（反压阈值）
		// 队列，设置大小，
		rowsCh:      make(chan *canal.RowsEvent, global.AppConfig.QueueSize),
		posCommitCh: make(chan mysql.Position, 128),
	}
	h.ctx, h.cancel = context.WithCancel(context.Background())

	// 初始化位置存储 && Sql语句
	switch h.Config.PositionStore.Type {
	case "redis":
		var err error
		h.positionStore, err = cache.NewRedisPositionStore(h.Config, h.Config.PositionStore.Redis.Key)
		if err != nil {
			log.Fatalf("初始化Redis位置存储失败: %v", err)
		}
		log.Println("✅ Redis位置存储初始化成功")
	default:
		// 默认使用文件位置存储
		h.positionStore = &cache.FilePositionStore{
			Path: h.Config.PositionStore.Path,
		}
		log.Println("✅ 文件位置存储初始化成功")
	}

	// 初始化position字段
	h.position = mysql.Position{
		Name: "",
		Pos:  0,
	}

	// 初始化数据输出
	h.initSinks()

	numWorkers := 1
	if numWorkers < 1 {
		numWorkers = 1
	}
	// 协程来处理数据写入
	for i := 0; i < numWorkers; i++ {
		h.workersWg.Add(1)
		go h.sinkWorker(i)
	}

	// 协程来处理持久化位点
	h.commitWg.Add(1)
	go h.posCommitLoop()

	return h
}

// sinkWorker 从 rowsCh 读取事件并按批写入 sink，成功后减少 pendingEvents
func (h *EventHandler) sinkWorker(id int) {
	defer h.workersWg.Done()
	// 本地缓存
	batch := make([]*canal.RowsEvent, 0, h.batchSize)
	timer := time.NewTimer(time.Duration(h.Config.Batch.Timeout) * time.Second)
	defer timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		// 写入 sink（按配置的 sink 类型）
		var err error
		switch SinkType(h.Config.Sink.Type) {
		case MySQLSink:
			if h.mysqlSink == nil {
				err = fmt.Errorf("mysql sink nil")
			} else {
				for _, e := range batch {
					if werr := h.mysqlSink.Write(e); werr != nil {
						err = werr
						break
					}
				}
				if err == nil {
					err = h.mysqlSink.FlushBatch()
				}
			}
		case PostgresSink:
			if h.pgsqlSink == nil {
				err = fmt.Errorf("pgsql sink nil")
			} else {
				for _, e := range batch {
					if werr := h.pgsqlSink.Write(e); werr != nil {
						err = werr
						break
					}
				}
				if err == nil {
					err = h.pgsqlSink.FlushBatch()
				}
			}
		case ElasticsearchSink:
			if h.esSink == nil {
				err = fmt.Errorf("es sink nil")
			} else {
				for _, e := range batch {
					if werr := h.esSink.OnRow(e); werr != nil {
						err = werr
						break
					}
				}
				if err == nil {
					err = h.esSink.FlushBatch()
				}
			}
		default:
			err = fmt.Errorf("unsupported sink")
		}
		// 在这个地方我们保持乐观的态度，也就是执行基本都是成功的，即使失败也是因为程序重启的时候重复执行
		// 保持幂等性
		var removed int64
		for _, ev := range batch {
			removed += int64(len(ev.Rows))
		}
		if err != nil {
			log.Printf("worker[%d] flush error: %v", id, err)
			atomic.AddInt64(&h.errorCount, 1)
			// TODO: 可以考虑重试机制,暂时先不写了，目前还没遇到

		}
		// sinkworker，也就是负责搬运的搬运完成多少就往pendingEvents减少多少
		atomic.AddInt64(&h.pendingEvents, -removed)
		// 清空 batch
		batch = batch[:0]
	}

	for {
		select {
		case e, ok := <-h.rowsCh:
			if !ok {
				// rowsCh 已关闭：flush 并退出
				flush()
				return
			}
			// 收集
			batch = append(batch, e)
			if len(batch) >= h.batchSize {
				flush()
				// reset timer
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(time.Duration(h.Config.Batch.Timeout) * time.Second)
			}
		case <-timer.C:
			// 超时触发 flush
			flush()
			timer.Reset(time.Duration(h.Config.Batch.Timeout) * time.Second)
		case <-h.ctx.Done():
			// 上下文取消：flush 并退出
			flush()
			return
		}
	}
}

// posCommitLoop：负责将位点在数据已落库后持久化
func (h *EventHandler) posCommitLoop() {
	defer h.commitWg.Done()
	for {
		select {
		// 不保证每一个完成的事件都及时的存储下来，因为没必要啊，只要程序不停止就不会出错，
		// h.pendingEvents == 0的时候才会存盘，否则就先不存，线往前跑，下边是一个可能的数值变化
		// pendingEvents: 2000 → 1500 → 900 → 0 → 800 → 1200 → 600 → 0 → ...
		// 但是pendingEvents 为0的时候并不一定是没有任务了，仅表示在为0的时候已经放到sink的已经全部放到目标系统了
		case pos, ok := <-h.posCommitCh:
			if !ok {
				// 通道关闭：退出前确保没有 pending
				for {
					if atomic.LoadInt64(&h.pendingEvents) == 0 {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
				// 最后保存（可忽略若无新 pos）
				if err := h.positionStore.Save(pos); err != nil {
					log.Printf("最后保存位点失败: %v", err)
				}
				return
			}

			// 等待 pendingEvents 归零（数据已持久化）
			for {
				if atomic.LoadInt64(&h.pendingEvents) == 0 {
					break
				}
				// 轻睡眠避免忙等
				time.Sleep(50 * time.Millisecond)
				fmt.Println("等待 pendingEvents 归零，当前值:", atomic.LoadInt64(&h.pendingEvents))
			}

			// 此时可以安全地保存位点
			if err := h.positionStore.Save(pos); err != nil {
				log.Printf("保存位点失败: %v", err)
			} else {
				log.Printf("位点已保存: %s %d", pos.Name, pos.Pos)
			}
		case <-h.ctx.Done():
			return
		}
	}
}

// 初始化数据输出接口
func (h *EventHandler) initSinks() {
	var err error

	// 根据配置初始化对应的Sink
	switch SinkType(h.Config.Sink.Type) {
	case MySQLSink:
		h.mysqlSink, err = sink.NewMySQLSink(h.Config, h, h.positionStore, h.position)
		if err != nil {
			log.Fatalf("初始化MySQL Sink失败: %v", err)
		}
		log.Println("✅ MySQL Sink初始化成功")

	case PostgresSink:
		h.pgsqlSink, err = sink.NewPgSQLSink(h.Config, h, h.positionStore, h.position)
		if err != nil {
			log.Fatalf("初始化PostgreSQL Sink失败: %v", err)
		}
		log.Println("✅ PostgreSQL Sink初始化成功")

	case ElasticsearchSink:
		h.esSink, err = sink.NewESSink(h.Config, h, h.positionStore, h.position)
		if err != nil {
			log.Fatalf("初始化Elasticsearch Sink失败: %v", err)
		}
		log.Println("✅ Elasticsearch Sink初始化成功")

	default:
		log.Fatalf("❌ 不支持的Sink类型: %s", h.Config.Sink.Type)
	}
}

// Close 优雅关闭，等待 worker & posCommit 完成
func (h *EventHandler) Close() error {
	// 取消上下文，先告诉 worker 结束
	fmt.Println("关闭事件处理器")
	h.cancel()

	// 关闭 rowsCh 让 worker 读到 EOF 并退出
	close(h.rowsCh)
	// 等待 worker 完成（会 flush）
	h.workersWg.Wait()

	waitstart := time.Now()
	timeout := 10 * time.Second

	for {
		if atomic.LoadInt64(&h.pendingEvents) == 0 {
			break
		}
		if time.Since(waitstart) > timeout {
			log.Printf("等待 pendingEvents 归零超时，当前值: %d", atomic.LoadInt64(&h.pendingEvents))
			break
		}
		log.Printf("等待 pendingEvents 归零，当前值: %d", atomic.LoadInt64(&h.pendingEvents))
		time.Sleep(100 * time.Millisecond)
	}

	// 关闭 posCommitCh，让 posCommitLoop 处理最后一条并退出
	close(h.posCommitCh)
	h.commitWg.Wait()

	// 现在所有 pendingEvents 应为 0（理论上）
	if atomic.LoadInt64(&h.pendingEvents) != 0 {
		log.Printf("警告：关闭时仍有 pendingEvents=%d", atomic.LoadInt64(&h.pendingEvents))
	}

	// 关闭 sinks & positionStore （沿用你原来的 Close）
	if h.mysqlSink != nil {
		if err := h.mysqlSink.Close(); err != nil {
			log.Printf("关闭MySQL sink失败: %v", err)
		}
	}
	if h.pgsqlSink != nil {
		if err := h.pgsqlSink.Close(); err != nil {
			log.Printf("关闭PgSQL sink失败: %v", err)
		}
	}
	if h.esSink != nil {
		if err := h.esSink.Close(); err != nil {
			log.Printf("关闭ES sink失败: %v", err)
		}
	}
	if h.positionStore != nil {
		if err := h.positionStore.Close(); err != nil {
			log.Printf("关闭位置存储失败: %v", err)
		}
	}

	log.Println("✅ 事件处理器优雅关闭完成")
	return nil
}

// 判断该事件是否属于配置的目标表
func (h *EventHandler) getTableConfig(schema string, table string) *config.TableConfig {
	for _, tbl := range h.Config.Tables {
		if tbl.SourceTable == table && h.Config.MySQL.Database == schema {
			return &tbl
		}
	}
	return nil
}

// OnRow 变更：把事件发入 rowsCh，负责反压和计数
func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
	// 过滤表（保留 schema & table）
	tcfg := h.getTableConfig(e.Table.Schema, e.Table.Name)
	if tcfg == nil {
		log.Printf("忽略非目标表: %s.%s", e.Table.Schema, e.Table.Name)

		return nil
	}

	// 增加待确认计数（按行数） —— 保守做法：len(e.Rows) 代表“行条目数”
	n := int64(len(e.Rows))
	atomic.AddInt64(&h.pendingEvents, n)
	// 这里为了防止事件很多一直在阻塞着，就先判断一下h.ctx.Done()是不是来了，不然一直执行不到这里，就没办法正常的停止
	// 先判断一下，起码在后边还有很多的事件的时候可以及时的放弃，嗯，没错，差不多就是这个样子了
	// 因为select如果都准备好了的话，是随机选择一个执行的，所以这里先判断一下ctx.Done()，如果已经关闭了，就直接返回错误
	// 这样就不会阻塞在后边的select里了
	select {
	case <-h.ctx.Done():
		atomic.AddInt64(&h.pendingEvents, -n)
		return fmt.Errorf("handler closing")
	default:
	}

	// 发送事件到队列：此处会阻塞，当队列满时形成反压
	h.rowsCh <- e
	// 成功送入，主要是统计信息，不过目前基本没有到，之后可能会用吧
	switch e.Action {
	case canal.InsertAction:
		atomic.AddInt64(&h.insertCount, n)
	case canal.UpdateAction:
		atomic.AddInt64(&h.updateCount, n)
	case canal.DeleteAction:
		atomic.AddInt64(&h.deleteCount, n)
	}
	return nil
}

// OnPosSynced 收到 canal 请求保存位点时，把位点丢到 posCommitCh 异步处理
func (h *EventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// 如果 force 为 true，你可能想要立即保存，不等待 pending==0（可配置）
	if force {
		// 等待短时间确保数据落盘，或者直接强制 flush sinks（慎用）
		// 我们此处给短等待窗口
		timeout := time.After(5 * time.Second)
		forceflag := false
		for !forceflag {
			if atomic.LoadInt64(&h.pendingEvents) == 0 {
				break
			}
			select {
			case <-timeout:
				log.Println("force 保存但 pending 未归零，仍保存位点（可能导致数据丢失）")
				forceflag = true
			default:
				time.Sleep(20 * time.Millisecond)
			}
			// 如果超时跳出并进行保存
		}
		if err := h.positionStore.Save(pos); err != nil {
			log.Printf("❌ 强制保存位点失败: %v", err)
			return err
		}
		log.Printf("✅ 强制位点已保存: %s %d", pos.Name, pos.Pos)
		return nil
	}

	// 非强制场景：异步放入 posCommitCh，由 posCommitLoop 等待 pending==0 后持久化
	select {
	case h.posCommitCh <- pos:
		// 已入队
	case <-h.ctx.Done():
		return fmt.Errorf("handler closing")
	}

	return nil
}

// String 返回处理器名称
func (h *EventHandler) String() string {
	return "BinlogSyncEventHandler"
}

// GetStats 获取统计信息
func (h *EventHandler) GetStats() map[string]int64 {
	return map[string]int64{
		"insert": h.insertCount,
		"update": h.updateCount,
		"delete": h.deleteCount,
		"error":  h.errorCount,
	}
}
