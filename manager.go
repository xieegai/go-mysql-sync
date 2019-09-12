package sync

import (
	"context"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

// Manager the contral manager to schedule mysql sync
type Manager struct {
	c *Config

	canal *canal.Canal

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	sink Sink

	master *masterInfo

	posHolder PositionHolder

	rowMapper RowMapper

	syncCh chan interface{}
}

// NewManager the constructor of mysql sync manager
func NewManager(c *Config, holder PositionHolder, rowMapper RowMapper, sink Sink) (*Manager, error) {
	sm := new(Manager)

	sm.c = c
	sm.syncCh = make(chan interface{}, 4096)
	sm.ctx, sm.cancel = context.WithCancel(context.Background())

	sm.posHolder = holder
	sm.rowMapper = rowMapper
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

	return sm, nil
}

func (r *Manager) newMaster() error {
	r.master = &masterInfo{}
	return nil
}

func (r *Manager) prepareMaster() error {
	if r.posHolder == nil {
		r.posHolder = &FilePositionHolder{dataDir: r.c.DataDir}
	}
	r.master.holder = r.posHolder
	return r.master.loadPos()
}

func (r *Manager) newCanal() error {
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

	cfg.IncludeTableRegex = r.c.SubscribeTableRegex

	var err error
	r.canal, err = canal.NewCanal(cfg)
	return errors.Trace(err)
}

func (r *Manager) prepareCanal() error {
	r.canal.SetEventHandler(&Handler{r})

	return nil
}

// Run syncs the data from MySQL and inserts to ES.
func (r *Manager) Run() error {
	r.wg.Add(1)
	go r.syncLoop()

	pos := r.master.Position()
	if err := r.canal.RunFrom(pos); err != nil {
		log.Errorf("start canal err %v", err)
		return errors.Trace(err)
	}

	return nil
}

// Ctx get the manager context
func (r *Manager) Ctx() context.Context {
	return r.ctx
}

// Close close the manager
func (r *Manager) Close() {
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

func (r *Manager) syncLoop() {
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
				for retry := 0; retry < 3 && err != nil; retry++ {
					log.Errorf("Batch flush failed %v, retry %v ...", err, retry+1)
					time.Sleep(time.Second * time.Duration(retry+1))
					err = r.sink.Publish(reqs)
				}
				if err != nil {
					log.Errorln("Batch flush failed over 3 times, cancelled")
					r.cancel()
					return
				}
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
