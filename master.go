package sync

import (
	"os"
	"path"
	"sync"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	"fmt"
	"io/ioutil"
	"strings"
	"strconv"
)

type PositionHoler interface {
	Load() (*mysql.Position, error)
	Save(pos *mysql.Position) error
}

type FilePositionHolder struct {
	dataDir string
}

func (h *FilePositionHolder) Save(pos *mysql.Position) error {
	if len(h.dataDir) == 0 {
		return nil
	}

	filePath := path.Join(h.dataDir, "master.info")

	posContent := fmt.Sprintf("%s:%v", pos.Name, pos.Pos)

	var err error
	if err = ioutil2.WriteFileAtomic(filePath, []byte(posContent), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", filePath, err)
	}
	return err
}

func (h *FilePositionHolder) Load() (*mysql.Position, error) {
	var pos mysql.Position

	if err := os.MkdirAll(h.dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	filePath := path.Join(h.dataDir, "master.info")
	f, err := os.Open(filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return nil, nil
	}
	defer f.Close()

	bytes, err := ioutil.ReadFile(filePath)
	if (err != nil) {
		return nil, err
	}

	toks := strings.Split(string(bytes), ":")
	if len(toks) == 2 {
		pos.Name = toks[0]

		rawPos, err := strconv.Atoi(toks[1])

		if err != nil {
			return nil, err
		}
		pos.Pos = uint32(rawPos)
		return &pos, errors.Trace(err)
	}
	return nil, errors.New("Cannot parse mysql position")
}

type masterInfo struct {
	sync.RWMutex

	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`

	lastSaveTime time.Time

	holder PositionHoler
}

func (m *masterInfo) loadPos() error {
	m.lastSaveTime = time.Now()

	pos, err := m.holder.Load()

	if err != nil {
		return errors.Trace(err)
	}

	if pos != nil {
		m.Name = pos.Name
		m.Pos = pos.Pos
	}

	return nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}
	m.lastSaveTime = n

	err := m.holder.Save(&pos)

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		m.Name,
		m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}

