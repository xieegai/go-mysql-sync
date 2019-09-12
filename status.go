package sync

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/siddontang/go/sync2"
	log "github.com/sirupsen/logrus"
)

// Stat the struct to hold some stats while MySQL sync
type Stat struct {
	Sms []*Manager

	l net.Listener

	InsertNum sync2.AtomicInt64
	UpdateNum sync2.AtomicInt64
	DeleteNum sync2.AtomicInt64
}

func (s *Stat) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var buf bytes.Buffer

	for _, sm := range s.Sms {
		rr, err := sm.canal.Execute("SHOW MASTER STATUS")
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(fmt.Sprintf("execute sql error %v", err)))
			return
		}

		binName, _ := rr.GetString(0, 0)
		binPos, _ := rr.GetUint(0, 1)

		pos := sm.canal.SyncedPosition()

		buf.WriteString(fmt.Sprintf("sync info of %s\n", sm.c.Label))
		buf.WriteString(fmt.Sprintf("-------------------------------------------------------------------------------\n"))
		buf.WriteString(fmt.Sprintf("server_current_binlog:(%s, %d)\n", binName, binPos))
		buf.WriteString(fmt.Sprintf("read_binlog:%s\n", pos))

		buf.WriteString(fmt.Sprintf("insert_num:%d\n", s.InsertNum.Get()))
		buf.WriteString(fmt.Sprintf("update_num:%d\n", s.UpdateNum.Get()))
		buf.WriteString(fmt.Sprintf("delete_num:%d\n", s.DeleteNum.Get()))
		buf.WriteString(fmt.Sprintf("-------------------------------------------------------------------------------\n\n"))
	}

	w.Write(buf.Bytes())
}

// Run the start function of Stat server
func (s *Stat) Run(addr string) {
	if len(addr) == 0 {
		return
	}
	log.Infof("run status http server %s", addr)
	var err error
	s.l, err = net.Listen("tcp", addr)
	if err != nil {
		log.Errorf("listen stat addr %s err %v", addr, err)
		return
	}

	srv := http.Server{}
	mux := http.NewServeMux()
	mux.Handle("/stat", s)
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	srv.Handler = mux

	srv.Serve(s.l)
}

// Close the close function of Stat
func (s *Stat) Close() {
	if s.l != nil {
		s.l.Close()
	}
}
