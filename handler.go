package sync

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
)

// Handler the handler to process all MySQL binlog events
type Handler struct {
	sm *Manager
}

// OnRotate the function to handle binlog position rotation
func (h *Handler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		Name: string(e.NextLogName),
		Pos:  uint32(e.Position),
	}

	h.sm.syncCh <- posSaver{pos, true}

	return h.sm.ctx.Err()
}

// OnTableChanged the function to handle table changed
func (h *Handler) OnTableChanged(schema, table string) error {
	return nil
}

// OnDDL the function to handle DDL event
func (h *Handler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.sm.syncCh <- posSaver{nextPos, true}
	return h.sm.ctx.Err()
}

// OnXID the function to handle XID event
func (h *Handler) OnXID(nextPos mysql.Position) error {
	h.sm.syncCh <- posSaver{nextPos, false}
	return h.sm.ctx.Err()
}

// OnRow the function to handle row changed
func (h *Handler) OnRow(e *canal.RowsEvent) error {
	var reqs []interface{}
	var err error
	var matchFlag bool = true

	if h.sm.rowMapper != nil {
		e = h.sm.rowMapper.Transform(e)
	}

	if len(h.sm.c.PublishTables) > 0 {
		matchFlag = false
		for _, table := range h.sm.c.PublishTables {
			if table == (e.Table.Schema + "." + e.Table.Name) {
				matchFlag = true
				break
			}
		}
	}

	if matchFlag {
		reqs, err = h.sm.sink.Parse(e)
		if err != nil {
			h.sm.cancel()
			return errors.Errorf("make %s ES request err %v, close sync", e.Action, err)
		}

		if len(reqs) > 0 {
			h.sm.syncCh <- reqs
		}
	}
	return h.sm.ctx.Err()
}

// OnGTID the function to handle GTID event
func (h *Handler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (h *Handler) OnPosSynced(pos mysql.Position, gtidSet mysql.GTIDSet, force bool) error {
	return nil
}

func (h *Handler) String() string {
	return "SyncHandler"
}
