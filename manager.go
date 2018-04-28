package sync

import (
	"github.com/siddontang/go-mysql/canal"
	"context"
	"sync"
	"github.com/juju/errors"
	"time"
	"github.com/siddontang/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

type SyncManager struct {
	c *SyncConfig

	canal *canal.Canal

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	sink Sink

	st *stat

	master *masterInfo

	posHoler PositionHoler

	syncCh chan interface{}
}

func NewSyncManager(c *SyncConfig, holder PositionHoler, sink Sink) (*SyncManager, error) {
	sm := new(SyncManager)

	sm.c = c
	sm.syncCh = make(chan interface{}, 4096)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())

	sm.posHoler = holder
	sm.sink = sink

	var err error
	if err = sm.newMaster(); err != nil {
		return nil, errors.Trace(err)
	}
	if err = sm.prepareMaster(); err != nil {
		return nil, errors.Trace(err)
	}
	if err = sm.newCanal(); err != nil {
		return nil, errors.Trace(err)
	}
	if err = sm.prepareCanal(); err != nil {
		return nil, errors.Trace(err)
	}
	// We must use binlog full row image
	if err = sm.canal.CheckBinlogRowImage("FULL"); err != nil {
		return nil, errors.Trace(err)
	}

	sm.st = &stat{r: sm}
	go sm.st.Run(sm.c.StatAddr)

	return sm, nil
}

func (r *SyncManager) newMaster() error {
	r.master = &masterInfo{}
	return nil
}

func (r *SyncManager) prepareMaster() error {
	if r.posHoler == nil {
		r.posHoler = &FilePositionHolder{dataDir: r.c.DataDir}
	}
	r.master.holder = r.posHoler
	return r.master.loadPos()
}

func (r *SyncManager) newCanal() error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = r.c.MyAddr
	cfg.User = r.c.MyUser
	cfg.Password = r.c.MyPassword
	cfg.Charset = r.c.MyCharset
	cfg.Flavor = r.c.Flavor

	cfg.ServerID = r.c.ServerID
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false
	cfg.Dump.SkipMasterData = false

	//TODO
	cfg.IncludeTableRegex = []string{"xiaomai\\.institution"}

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *SyncManager) prepareCanal() error {
	r.canal.SetEventHandler(&SyncHandler{r})

	return nil
}

// Run syncs the data from MySQL and inserts to ES.
func (r *SyncManager) Run() error {
	r.wg.Add(1)
	go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		return errors.Trace(err)
	}

	return nil
}

func (r *SyncManager) Ctx() context.Context {
	return r.ctx
}

func (r *SyncManager) Close() {
	log.Infof("closing manager")

	r.cancel()

	r.canal.Close()

	r.master.Close()

	r.wg.Wait()
}

type posSaver struct {
	pos   mysql.Position
	force bool
}

func (r *SyncManager) syncLoop() {
	bulkSize := r.c.BulkSize
	if bulkSize == 0 {
		bulkSize = 128
	}

	interval := r.c.FlushBulkTime
	if interval == 0 {
		interval = 200 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer r.wg.Done()

	lastSavedTime := time.Now()
	reqs := make([]interface{}, 0, 1024)

	var pos mysql.Position

	for {
		needFlush := false
		needSavePos := false

		select {
		case v := <-r.syncCh:
			switch v := v.(type) {
			case posSaver:
				now := time.Now()
				if v.force || now.Sub(lastSavedTime) > 3*time.Second {
					lastSavedTime = now
					needFlush = true
					needSavePos = true
					pos = v.pos
				}
			case []interface{}:
				reqs = append(reqs, v...)
				needFlush = len(reqs) >= bulkSize
			}
		case <-ticker.C:
			needFlush = true
		case <-r.ctx.Done():
			return
		}

		if needFlush {
			// TODO: retry some times?
			if err := r.sink.Publish(reqs); err != nil {
				log.Errorf("do ES bulk err %v, close sync", err)
				r.cancel()
				return
			}
			reqs = reqs[0:0]
		}

		if needSavePos {
			if err := r.master.Save(pos); err != nil {
				log.Errorf("save sync position %s err %v, close sync", pos, err)
				r.cancel()
				return
			}
		}
	}
}
