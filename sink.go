package sync

import "github.com/siddontang/go-mysql/canal"

// Sink the interface to depict sync target
type Sink interface {
	Parse(e *canal.RowsEvent) ([]interface{}, error)
	Publish([]interface{}) error
}
