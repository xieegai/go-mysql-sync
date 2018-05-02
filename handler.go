package sync

import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/canal"
	"github.com/juju/errors"
)

type SyncHandler struct {
	sm *SyncManager
}

func (h *SyncHandler) OnRotate(e *replication.RotateEvent) error {
	pos := mysql.Position{
		string(e.NextLogName),
		uint32(e.Position),
	}

	h.sm.syncCh <- posSaver{pos, true}

	return h.sm.ctx.Err()
}

func (h *SyncHandler) OnTableChanged(schema, table string) error {
	return nil
}

func (h *SyncHandler) OnDDL(nextPos mysql.Position, _ *replication.QueryEvent) error {
	h.sm.syncCh <- posSaver{nextPos, true}
	return h.sm.ctx.Err()
}

func (h *SyncHandler) OnXID(nextPos mysql.Position) error {
	h.sm.syncCh <- posSaver{nextPos, false}
	return h.sm.ctx.Err()
}

func (h *SyncHandler) OnRow(e *canal.RowsEvent) error {
	var reqs []interface{}
	var err error
	var matchFlag bool = true

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

func (h *SyncHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

// OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
func (h *SyncHandler) OnPosSynced(pos mysql.Position, force bool) error {
	return nil
}

func (h *SyncHandler) String() string {
	return "SyncHandler"
}
