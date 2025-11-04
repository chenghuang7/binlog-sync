/**
* @File    :   config.go
* @Date    :   2025/10/19 21:08:44
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package config

import (
	"fmt"
	"log"
	"reflect"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type TableConfig struct {
	SourceTable string            // 源表名，如 "users"
	TargetTable string            // 目标表名，如 "users_copy"；可留空则默认同名
	Fields      []string          // 要同步的字段；为空则表示全字段
	FieldMap    map[string]string // 源字段 -> 目标字段 的映射，可选
	Where       string            // 同步时可选过滤条件（例如 "status=1"）
}

// TimeoutConfig 超时配置
type TimeoutConfig struct {
	Context int // 秒
}

// BinlogConfig Binlog配置
type BinlogConfig struct {
	StartPosition uint32 `yaml:"startPosition"`
}

type Config struct {
	MySQL struct {
		Host     string
		Port     int
		User     string
		Password string
		Database string
		ServerID int
		Charset  string
	}
	Sink struct {
		Type          string
		Elasticsearch struct {
			Address  string
			Index    string
			Username string
			Password string
		}
		Postgres struct {
			DSN string
		}
		PgSQL struct {
			Host     string
			Port     int
			User     string
			Password string
			Database string
		}
		MySQL struct {
			Host     string
			Port     int
			User     string
			Password string
			Database string
			ServerID int
			Charset  string
		}
	}
	Tables        []TableConfig
	PositionStore struct {
		Type  string // 文件类型: file, redis
		Path  string // 文件路径，当Type为file时使用
		Redis struct {
			Host     string
			Port     int
			Password string
			DB       int
			Key      string // 存储位置的键名
		}
	}
	Batch struct {
		Enabled bool
		Size    int
		Timeout int // 秒
	}
	ConnectionPool struct {
		MaxOpen     int
		MaxIdle     int
		MaxLifetime int // 秒
	}
	Timeout TimeoutConfig
	Binlog  BinlogConfig
	QueueSize int
}

func Load() (*Config, error) {
	// 1. 定义命令行参数 --config
	configPath := pflag.String("config", "config/config.yaml", "配置文件路径")
	fmt.Println("加载配置文件:", *configPath)
	pflag.Parse()

	// 2. 初始化 viper
	viper.SetConfigFile(*configPath)
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("读取配置失败: %v", err)
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("解析配置失败: %v", err)
		return nil, err
	}

	log.Println("✅ 配置加载成功:", *configPath)
	return &cfg, nil
}

func (cfg *Config) Print() {
	fmt.Println("配置信息:")
	printStruct(reflect.ValueOf(cfg).Elem(), 0)
}

// 递归打印结构体字段
func printStruct(v reflect.Value, indent int) {
	t := v.Type()
	indentStr := ""
	for i := 0; i < indent; i++ {
		indentStr += "  " // 缩进
	}

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)
		fieldName := fieldType.Name

		switch field.Kind() {
		case reflect.Struct:
			fmt.Printf("%s%s:\n", indentStr, fieldName)
			printStruct(field, indent+1) // 递归打印子结构体
		default:
			fmt.Printf("%s%-20s: %v\n", indentStr, fieldName, field.Interface())
		}
	}
}
