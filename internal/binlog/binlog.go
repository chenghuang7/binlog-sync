/**
* @File    :   binlog.go
* @Date    :   2025/10/20 12:03:12
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/
package binlog

import (
	"fmt"
	"log"
	"os"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
)

func getFirstBinlogPos(c *canal.Canal, cfg *config.Config) (mysql.Position, error) {
	query := "SHOW MASTER LOGS"
	rows, err := c.Execute(query)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("执行 %s 失败: %w", query, err)
	}

	if len(rows.Values) == 0 {
		return mysql.Position{}, fmt.Errorf("未找到任何 binlog 文件")
	}

	firstLog := string(rows.Values[0][0].AsString())
	return mysql.Position{Name: firstLog, Pos: uint32(cfg.Binlog.StartPosition)}, nil // binlog 从配置的起始位置开始
}

// StartBinlogSync 启动 binlog 监听
func StartBinlogSync(cfg *config.Config) error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = cfg.MySQL.Host + ":" + fmt.Sprintf("%d", cfg.MySQL.Port)
	canalCfg.User = cfg.MySQL.User
	canalCfg.Password = cfg.MySQL.Password
	canalCfg.Flavor = "mysql"
	canalCfg.ServerID = uint32(cfg.MySQL.ServerID)

	// 禁用 mysqldump，只同步 binlog
	canalCfg.Dump.ExecutionPath = ""

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return err
	}

	// 注册事件处理器
	h := NewEventHandler()
	c.SetEventHandler(h)

	pos, err := h.positionStore.Load()
	fmt.Println(pos, err)
	if os.IsNotExist(err) {
		pos, err = getFirstBinlogPos(c, cfg)
	} else if err != nil {
		pos, err = c.GetMasterPos()
	}
	if err != nil {
		return err
	}

	log.Printf("✅ Binlog 同步开始，文件: %s, 位置: %d", pos.Name, pos.Pos)
	c.RunFrom(pos)
	return nil
}
