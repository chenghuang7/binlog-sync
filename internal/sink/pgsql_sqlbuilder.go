/**
* @File    :   pgsql_sqlbuilder.go
* @Date    :   2025/10/20 14:15:31
* @Author  :   SeeStars
* @Version :   1.0
* @Desc    :   PostgreSQL SQL构建实现
**/

package sink

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

// PgSQLSQLBuilder PostgreSQL SQL构建实现
type PgSQLSQLBuilder struct{}

// NewPgSQLSQLBuilder 创建PostgreSQL SQL构建实例
func NewPgSQLSQLBuilder() *PgSQLSQLBuilder {
	return &PgSQLSQLBuilder{}
}

// EscapeValue 转义并格式化值，防止注入攻击
func (b *PgSQLSQLBuilder) EscapeValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
	default:
		return fmt.Sprintf("'%v'", val)
	}
}

// BuildWhereCondition WHERE 条件安全构造（处理 NULL）
func (b *PgSQLSQLBuilder) BuildWhereCondition(col string, v interface{}) string {
	if v == nil {
		return fmt.Sprintf("\"%s\" IS NULL", col)
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("\"%s\" = '%s'", col, strings.ReplaceAll(val, "'", "''"))
	default:
		return fmt.Sprintf("\"%s\" = '%v'", col, val)
	}
}

// IsPrimaryKey 判断是否主键列
func (b *PgSQLSQLBuilder) IsPrimaryKey(table *schema.Table, idx int) bool {
	for _, pkIdx := range table.PKColumns {
		if pkIdx == idx {
			return true
		}
	}
	return false
}

// BuildInsertSQL 构建插入SQL
// INSERT INTO "%s" (%s) VALUES (%s)
func (b *PgSQLSQLBuilder) BuildInsertSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	colNames := make([]string, len(columns))
	values := make([]string, len(columns))

	for i, col := range columns {
		colNames[i] = fmt.Sprintf("\"%s\"", col.Name)
		switch v := row[i].(type) {
		case nil:
			values[i] = "NULL"
		case string:
			values[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		default:
			values[i] = fmt.Sprintf("'%v'", v)
		}
	}

	sql := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		targetTable,
		strings.Join(colNames, ", "),
		strings.Join(values, ", "),
	)
	// log.Println(sql)
	return sql
}

// BuildUpdateSQL 构建更新SQL
// UPDATE "%s" SET %s WHERE %s
func (b *PgSQLSQLBuilder) BuildUpdateSQL(e *canal.RowsEvent, oldRow []interface{}, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	// 构建 SET 子句
	setParts := make([]string, 0, len(columns))
	for i, col := range columns {
		if i >= len(row) {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("\"%s\" = %s", col.Name, b.EscapeValue(row[i])))
	}

	// 构建 WHERE 子句（优先用主键）
	whereParts := make([]string, 0)
	for i, col := range columns {
		if i >= len(oldRow) {
			continue
		}
		if b.IsPrimaryKey(table, i) {
			whereParts = append(whereParts, b.BuildWhereCondition(col.Name, oldRow[i]))
		}
	}

	// 如果没有主键，退化为用全部列匹配
	if len(whereParts) == 0 {
		for i, col := range columns {
			if i >= len(oldRow) {
				continue
			}
			whereParts = append(whereParts, b.BuildWhereCondition(col.Name, oldRow[i]))
		}
	}

	sql := fmt.Sprintf("UPDATE \"%s\" SET %s WHERE %s",
		targetTable,
		strings.Join(setParts, ", "),
		strings.Join(whereParts, " AND "),
	)
	// log.Println("update:", sql)
	return sql
}

// BuildDeleteSQL 构建删除SQL
// DELETE FROM "%s" WHERE %s
func (b *PgSQLSQLBuilder) BuildDeleteSQL(e *canal.RowsEvent, row []interface{}, targetTable string) string {
	table := e.Table
	columns := table.Columns

	// 构建WHERE部分（使用主键或所有列）
	whereParts := make([]string, 0)
	// log.Println(row...)
	for i, col := range columns {
		if b.IsPrimaryKey(table, i) {
			whereParts = append(whereParts, b.BuildWhereCondition(col.Name, row[i]))
		}
	}
	//  如果没有主键就使用所有的列
	if len(whereParts) == 0 {
		for i, col := range columns {
			whereParts = append(whereParts, b.BuildWhereCondition(col.Name, row[i]))
		}
	}

	sql := fmt.Sprintf("DELETE FROM \"%s\" WHERE %s",
		targetTable,
		strings.Join(whereParts, " AND "),
	)
	// log.Println("pgsql: ", sql)
	return sql
}
