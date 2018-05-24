package sync

import "github.com/siddontang/go-mysql/canal"

type SyncMapper interface {
	Transform(e *canal.RowsEvent) *canal.RowsEvent
}

type DefaultSyncMapper struct {}

func (m *DefaultSyncMapper) Transform(e *canal.RowsEvent) *canal.RowsEvent {
	return e
}
