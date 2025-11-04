/**
* @File    :   global.go
* @Date    :   2025/10/20 15:37:30
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package global

import (
	"fmt"
	"log"

	"github.com/changhuang7/binlog-sync/config"
	"github.com/changhuang7/binlog-sync/internal/db"
)

var (
	MysqlMaster *db.MySQLDB
	MysqlSlave  *db.MySQLDB
	// esClient    *db.ESClient
	// pgClient    *db.PGSQLClient
	AppConfig *config.Config
)

func init() {
	var err error
	AppConfig, err = config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}
	AppConfig.Print()

	// 2️⃣ 初始化主数据库
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		AppConfig.MySQL.User,
		AppConfig.MySQL.Password,
		AppConfig.MySQL.Host,
		AppConfig.MySQL.Port,
		AppConfig.MySQL.Database,
		AppConfig.MySQL.Charset,
	)
	// 创建数据库配置
	dbConfig := db.DBConfig{
		Host:         AppConfig.MySQL.Host,
		Port:         AppConfig.MySQL.Port,
		User:         AppConfig.MySQL.User,
		Password:     AppConfig.MySQL.Password,
		Database:     AppConfig.MySQL.Database,
		Charset:      AppConfig.MySQL.Charset,
		MaxOpenConns: 20,
		MaxIdleConns: 10,
		MaxLifetime:  300, // 5分钟
	}
	fmt.Printf("从数据库的dsn：%s\n", dsn)
	MysqlMaster, err = db.GetMySQL(dsn, dbConfig)
	if err != nil {
		log.Fatalf("failed to connect to MySQL master: %v", err)
	}

	fmt.Println("MySQL 主库初始化成功:", MysqlMaster)
	// 初始化从数据库
	dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		AppConfig.Sink.MySQL.User,
		AppConfig.Sink.MySQL.Password,
		AppConfig.Sink.MySQL.Host,
		AppConfig.Sink.MySQL.Port,
		AppConfig.Sink.MySQL.Database,
		AppConfig.Sink.MySQL.Charset,
	)
	fmt.Printf("从数据库的dsn：%s\n", dsn)
	// 复用相同的数据库配置
	MysqlSlave, err = db.GetMySQL(dsn, dbConfig)
	if err != nil {
		log.Fatalf("failed to connect to MySQL master: %v", err)
	}

	fmt.Println("MySQL 从库初始化成功:", MysqlSlave)
}
