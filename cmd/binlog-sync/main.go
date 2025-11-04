/**
* @File    :   main.go
* @Date    :   2025/10/19 21:00:43
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package main

import (
	"log"

	"github.com/changhuang7/binlog-sync/global"
	"github.com/changhuang7/binlog-sync/internal/binlog"
)

func main() {
	if err := binlog.StartBinlogSync(global.AppConfig); err != nil {
		log.Fatalf("❌ Binlog 同步失败: %v", err)
	}
}
