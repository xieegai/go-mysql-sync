package sync

import "github.com/siddontang/go-mysql/canal"

// RowMapper the mapper to process some row transformation
type RowMapper interface {
	Transform(e *canal.RowsEvent) *canal.RowsEvent
}

// DefaultRowMapper the default mapper do nothing
type DefaultRowMapper struct{}

// Transform the default row mapper func doing nothing on rows
func (m *DefaultRowMapper) Transform(e *canal.RowsEvent) *canal.RowsEvent {
	return e
}
