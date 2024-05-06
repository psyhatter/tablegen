// Code generated by tablegen. DO NOT EDIT.
package tables

import (
	"encoding/json"
	"reflect"
	"sync"

	"github.com/pkg/errors"
)

func Get() AllTables                   { return AllTables{} }
func WithAlias(alias string) AllTables { return AllTables{alias: alias} }

type AllTables struct{ alias string }

type Index struct {
	tableAlias, tableName, indexName string
	columns                          []string
}

func (i Index) Name() string             { return i.indexName }
func (i Index) Table() string            { return i.tableName }
func (i Index) AllColumnNames() []string { return i.columns }

func tableName(table, alias string) string {
	if alias != "" {
		table += " AS " + alias
	}
	return table
}

func colName(alias, col string) string {
	if alias == "" {
		return col
	}
	return alias + "." + col
}

var nulls sync.Map

func ForceDefault[T any]() *T {
	var v T
	p, _ := nulls.LoadOrStore(reflect.TypeOf(&v).String(), &v)
	return p.(*T)
}

type jsonEncoder struct{ v any }

func (j jsonEncoder) MarshalJSON() ([]byte, error) {
	v, err := json.Marshal(j.v)
	return v, errors.WithStack(err)
}