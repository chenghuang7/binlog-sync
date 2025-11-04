/**
* @File    :   mysql_sqlbuilder.go
* @Date    :   2025/10/20 14:15:07
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   None
**/

package sink

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

// MySQLSQLBuilder 实现SQLBuilder接口，提供MySQL特定的SQL构建功能
type MySQLSQLBuilder struct{}

// NewMySQLSQLBuilder 创建一个新的MySQLSQLBuilder实例
func NewMySQLSQLBuilder() *MySQLSQLBuilder {
	return &MySQLSQLBuilder{}
}

// EscapeValue 转义并格式化值，防止注入攻击
func (m *MySQLSQLBuilder) EscapeValue(v interface{}) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

// BuildWhereCondition WHERE 条件安全构造（处理 NULL）
func (m *MySQLSQLBuilder) BuildWhereCondition(column string, value interface{}) string {
	if value == nil {
		return fmt.Sprintf("`%s` is null", column)
	}
	switch val := value.(type) {
	case string:
		return fmt.Sprintf("`%s` = '%s'", column, strings.ReplaceAll(val, "'", "''"))
	default:
		return fmt.Sprintf("`%s` = '%v'", column, val)
	}
}

// IsPrimaryKey 判断是否主键列
func (m *MySQLSQLBuilder) IsPrimaryKey(table *schema.Table, index int) bool {
	for _, pkIdx := range table.PKColumns {
		if pkIdx == index {
			return true
		}
	}
	return false
}

// BuildInsertSQL 构建插入SQL
func (m *MySQLSQLBuilder) BuildInsertSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	colNames := make([]string, len(columns))
	values := make([]string, len(columns))

	for i, col := range columns {
		colNames[i] = fmt.Sprintf("`%s`", col.Name)
		values[i] = m.EscapeValue(row[i])
	}

	sql := fmt.Sprintf("insert ignore into `%s` (%s) values (%s)",
		targetTable,
		strings.Join(colNames, ", "),
		strings.Join(values, ", "),
	)
	return sql
}

// BuildUpdateSQL 构建更新SQL
func (m *MySQLSQLBuilder) BuildUpdateSQL(e *canal.RowsEvent, oldRow []interface{}, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	// 构建 SET 子句
	setParts := make([]string, 0, len(columns))
	for i, col := range columns {
		if i >= len(row) {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("`%s` = %s", col.Name, m.EscapeValue(row[i])))
	}

	// 构建 WHERE 子句（优先用主键）
	whereParts := make([]string, 0)
	for i, col := range columns {
		if i >= len(oldRow) {
			continue
		}
		if m.IsPrimaryKey(table, i) {
			whereParts = append(whereParts, m.BuildWhereCondition(col.Name, oldRow[i]))
		}
	}

	// 如果没有主键，退化为用全部列匹配
	if len(whereParts) == 0 {
		for i, col := range columns {
			if i >= len(oldRow) {
				continue
			}
			whereParts = append(whereParts, m.BuildWhereCondition(col.Name, oldRow[i]))
		}
	}

	sql := fmt.Sprintf("update `%s` set %s where %s",
		strings.ReplaceAll(targetTable, "`", ""), // 防止注入
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " and "),
	)
	return sql
}

// BuildDeleteSQL 构建删除SQL
func (m *MySQLSQLBuilder) BuildDeleteSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	// 构建WHERE部分（使用主键或所有列）
	whereParts := make([]string, 0)
	for i, col := range columns {
		if m.IsPrimaryKey(table, i) {
			whereParts = append(whereParts, m.BuildWhereCondition(col.Name, row[i]))
		}
	}
	// 如果没有主键就使用所有的列
	if len(whereParts) == 0 {
		for i, col := range columns {
			whereParts = append(whereParts, m.BuildWhereCondition(col.Name, row[i]))
		}
	}

	sql := fmt.Sprintf("delete from `%s` where %s",
		targetTable,
		strings.Join(whereParts, " and "),
	)
	return sql
}
