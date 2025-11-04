// package binlog

// import (
//     "context"
//     "fmt"
//     "log"
//     "sync"
//     "sync/atomic"
//     "time"

//     "github.com/go-mysql-org/go-mysql/canal"
//     "github.com/go-mysql-org/go-mysql/mysql"
//     "github.com/go-mysql-org/go-mysql/replication"
// )

// // --- 在 EventHandler struct 中新增字段 ---
// type EventHandler struct {
//     canal.DummyEventHandler
//     // ... 你原来的字段 ...

//     // 事件队列与控制
//     rowsCh       chan *canal.RowsEvent
//     posCommitCh  chan mysql.Position
//     workersWg    sync.WaitGroup
//     commitWg     sync.WaitGroup
//     ctx          context.Context
//     cancel       context.CancelFunc

//     // 原子计数：尚未确认写入目标系统的行数
//     pendingEvents int64

//     // 统计信息使用原子操作
//     insertCount int64
//     updateCount int64
//     deleteCount int64
//     errorCount  int64
// }

// // NewEventHandler 的关键初始化（摘取并替换主要部分）
// func NewEventHandler() *EventHandler {
//     h := &EventHandler{
//         Config:       global.AppConfig,
//         ListenInsert: true,
//         ListenUpdate: true,
//         ListenDelete: true,
//         ListenDDL:    false,
//         batchMode:    global.AppConfig.Batch.Enabled,
//         batchSize:    global.AppConfig.Batch.Size,
//         stopChan:     make(chan struct{}),

//         // 初始化队列：队列大小可由配置决定（反压阈值）
//         rowsCh:      make(chan *canal.RowsEvent, global.AppConfig.QueueSize),
//         posCommitCh: make(chan mysql.Position, 128),
//     }

//     h.ctx, h.cancel = context.WithCancel(context.Background())

//     // 初始化 positionStore、sinks 等（沿用你的代码）
//     // h.initSinks()
//     // h.loadCacheSql()

//     // 启动 sink worker（单 worker 或多个按需求）
//     numWorkers := global.AppConfig.Workers
//     if numWorkers < 1 {
//         numWorkers = 1
//     }
//     for i := 0; i < numWorkers; i++ {
//         h.workersWg.Add(1)
//         go h.sinkWorker(i)
//     }

//     // 启动位点提交协程
//     h.commitWg.Add(1)
//     go h.posCommitLoop()

//     return h
// }

// // OnRow 变更：把事件发入 rowsCh，负责反压和计数
// func (h *EventHandler) OnRow(e *canal.RowsEvent) error {
//     // 过滤表（保留 schema & table）
//     tcfg := h.getTableConfig(e.Table.Schema, e.Table.Name)
//     if tcfg == nil {
//         return nil
//     }

//     // 增加待确认计数（按行数） —— 保守做法：len(e.Rows) 代表“行条目数”
//     n := int64(len(e.Rows))
//     atomic.AddInt64(&h.pendingEvents, n)

//     // 发送事件到队列：此处会阻塞，当队列满时形成反压
//     select {
//     case h.rowsCh <- e:
//         // 成功送入
//         switch e.Action {
//         case canal.InsertAction:
//             atomic.AddInt64(&h.insertCount, n)
//         case canal.UpdateAction:
//             atomic.AddInt64(&h.updateCount, n)
//         case canal.DeleteAction:
//             atomic.AddInt64(&h.deleteCount, n)
//         }
//         return nil
//     case <-h.ctx.Done():
//         // 已关闭上下文，拒绝接受新事件
//         atomic.AddInt64(&h.errorCount, 1)
//         // 回退 pendingEvents，因为没有被消费者处理
//         atomic.AddInt64(&h.pendingEvents, -n)
//         return fmt.Errorf("handler closing")
//     }
// }

// // sinkWorker 从 rowsCh 读取事件并按批写入 sink，成功后减少 pendingEvents
// func (h *EventHandler) sinkWorker(id int) {
//     defer h.workersWg.Done()
//     // 本地缓存
//     batch := make([]*canal.RowsEvent, 0, h.batchSize)
//     timer := time.NewTimer(time.Duration(h.Config.Batch.Timeout) * time.Second)
//     defer timer.Stop()

//     flush := func() {
//         if len(batch) == 0 {
//             return
//         }
//         // 写入 sink（按配置的 sink 类型）
//         var err error
//         switch SinkType(h.Config.Sink.Type) {
//         case MySQLSink:
//             if h.mysqlSink == nil {
//                 err = fmt.Errorf("mysql sink nil")
//             } else {
//                 for _, e := range batch {
//                     if werr := h.mysqlSink.Write(e); werr != nil {
//                         err = werr
//                         break
//                     }
//                 }
//                 if err == nil {
//                     err = h.mysqlSink.FlushBatch()
//                 }
//             }
//         case PostgresSink:
//             if h.pgsqlSink == nil {
//                 err = fmt.Errorf("pgsql sink nil")
//             } else {
//                 for _, e := range batch {
//                     if werr := h.pgsqlSink.Write(e); werr != nil {
//                         err = werr
//                         break
//                     }
//                 }
//                 if err == nil {
//                     err = h.pgsqlSink.FlushBatch()
//                 }
//             }
//         case ElasticsearchSink:
//             if h.esSink == nil {
//                 err = fmt.Errorf("es sink nil")
//             } else {
//                 for _, e := range batch {
//                     if werr := h.esSink.OnRow(e); werr != nil {
//                         err = werr
//                         break
//                     }
//                 }
//                 if err == nil {
//                     err = h.esSink.FlushBatch()
//                 }
//             }
//         default:
//             err = fmt.Errorf("unsupported sink")
//         }

//         if err != nil {
//             log.Printf("worker[%d] flush error: %v", id, err)
//             atomic.AddInt64(&h.errorCount, 1)
//             // 失败策略：你可以重试、写入本地文件或中断。这里采取简单的重试（可改）
//             // 为避免无限循环，这里不自动重试；生产可设计重试队列或持久化到磁盘。
//         } else {
//             // 减少 pendingEvents
//             var removed int64
//             for _, ev := range batch {
//                 removed += int64(len(ev.Rows))
//             }
//             atomic.AddInt64(&h.pendingEvents, -removed)
//         }
//         // 清空 batch
//         batch = batch[:0]
//     }

//     for {
//         select {
//         case e, ok := <-h.rowsCh:
//             if !ok {
//                 // rowsCh 已关闭：flush 并退出
//                 flush()
//                 return
//             }
//             // 收集
//             batch = append(batch, e)
//             if len(batch) >= h.batchSize {
//                 flush()
//                 // reset timer
//                 if !timer.Stop() {
//                     select {
//                     case <-timer.C:
//                     default:
//                     }
//                 }
//                 timer.Reset(time.Duration(h.Config.Batch.Timeout) * time.Second)
//             }
//         case <-timer.C:
//             // 超时触发 flush
//             flush()
//             timer.Reset(time.Duration(h.Config.Batch.Timeout) * time.Second)
//         case <-h.ctx.Done():
//             // 上下文取消：flush 并退出
//             flush()
//             return
//         }
//     }
// }

// // posCommitLoop：负责将位点在数据已落库后持久化
// func (h *EventHandler) posCommitLoop() {
//     defer h.commitWg.Done()
//     for {
//         select {
//         case pos, ok := <-h.posCommitCh:
//             if !ok {
//                 // 通道关闭：退出前确保没有 pending
//                 for {
//                     if atomic.LoadInt64(&h.pendingEvents) == 0 {
//                         break
//                     }
//                     time.Sleep(100 * time.Millisecond)
//                 }
//                 // 最后保存（可忽略若无新 pos）
//                 if err := h.positionStore.Save(pos); err != nil {
//                     log.Printf("最后保存位点失败: %v", err)
//                 }
//                 return
//             }

//             // 等待 pendingEvents 归零（数据已持久化）
//             for {
//                 if atomic.LoadInt64(&h.pendingEvents) == 0 {
//                     break
//                 }
//                 // 轻睡眠避免忙等；也可改成更复杂的条件变量
//                 time.Sleep(50 * time.Millisecond)
//             }

//             // 此时可以安全地保存位点
//             if err := h.positionStore.Save(pos); err != nil {
//                 log.Printf("保存位点失败: %v", err)
//             } else {
//                 log.Printf("位点已保存: %s %d", pos.Name, pos.Pos)
//             }

//         case <-h.ctx.Done():
//             return
//         }
//     }
// }

// // OnPosSynced 收到 canal 请求保存位点时，把位点丢到 posCommitCh 异步处理
// func (h *EventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
//     // 如果 force 为 true，你可能想要立即保存，不等待 pending==0（可配置）
//     if force {
//         // 等待短时间确保数据落盘，或者直接强制 flush sinks（慎用）
//         // 我们此处给短等待窗口
//         timeout := time.After(5 * time.Second)
//         for {
//             if atomic.LoadInt64(&h.pendingEvents) == 0 {
//                 break
//             }
//             select {
//             case <-timeout:
//                 log.Println("force 保存但 pending 未归零，仍保存位点（可能导致数据丢失）")
//                 break
//             default:
//                 time.Sleep(20 * time.Millisecond)
//             }
//             // 如果超时跳出并进行保存
//         }
//         if err := h.positionStore.Save(pos); err != nil {
//             log.Printf("❌ 强制保存位点失败: %v", err)
//             return err
//         }
//         log.Printf("✅ 强制位点已保存: %s %d", pos.Name, pos.Pos)
//         return nil
//     }

//     // 非强制场景：异步放入 posCommitCh，由 posCommitLoop 等待 pending==0 后持久化
//     select {
//     case h.posCommitCh <- pos:
//         // 已入队
//     case <-h.ctx.Done():
//         return fmt.Errorf("handler closing")
//     }

//     return nil
// }

// // Close 优雅关闭，等待 worker & posCommit 完成
// func (h *EventHandler) Close() error {
//     // 取消上下文，先告诉 worker 结束
//     h.cancel()

//     // 关闭 rowsCh 让 worker 读到 EOF 并退出
//     close(h.rowsCh)
//     // 等待 worker 完成（会 flush）
//     h.workersWg.Wait()

//     // 关闭 posCommitCh，让 posCommitLoop 处理最后一条并退出
//     close(h.posCommitCh)
//     h.commitWg.Wait()

//     // 现在所有 pendingEvents 应为 0（理论上）
//     if atomic.LoadInt64(&h.pendingEvents) != 0 {
//         log.Printf("警告：关闭时仍有 pendingEvents=%d", atomic.LoadInt64(&h.pendingEvents))
//     }

//     // 关闭 sinks & positionStore （沿用你原来的 Close）
//     if h.mysqlSink != nil {
//         if err := h.mysqlSink.Close(); err != nil {
//             log.Printf("关闭MySQL sink失败: %v", err)
//         }
//     }
//     if h.pgsqlSink != nil {
//         if err := h.pgsqlSink.Close(); err != nil {
//             log.Printf("关闭PgSQL sink失败: %v", err)
//         }
//     }
//     if h.esSink != nil {
//         if err := h.esSink.Close(); err != nil {
//             log.Printf("关闭ES sink失败: %v", err)
//         }
//     }
//     if h.positionStore != nil {
//         if err := h.positionStore.Close(); err != nil {
//             log.Printf("关闭位置存储失败: %v", err)
//         }
//     }

//     log.Println("✅ 事件处理器优雅关闭完成")
//     return nil
// }

package binlog