package sync

import (
	"time"
)

// Config sync configuration typedef
type Config struct {
	// the label of MySQL configuration
	Label string `toml:"label"`

	// the MySQL address
	MyAddr string `toml:"my_addr"`
	// the MySQL user
	MyUser string `toml:"my_user"`
	// the MySQL password
	MyPassword string `toml:"my_pass"`
	// the MySQL charset
	MyCharset string `toml:"my_charset"`

	// the service id to mimic a slave server
	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime time.Duration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`

	SubscribeTableRegex []string
	PublishTables       []string
}
