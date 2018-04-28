package sync

import "github.com/siddontang/go-mysql/canal"

type Sink interface {
	Parse(e *canal.RowsEvent) ([]interface{}, error)
	Publish([]interface{}) error
}