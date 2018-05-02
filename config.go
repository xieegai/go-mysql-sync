package sync

import (
	"time"
)

type SyncConfig struct {
	MyAddr     string `toml:"my_addr"`
	MyUser     string `toml:"my_user"`
	MyPassword string `toml:"my_pass"`
	MyCharset  string `toml:"my_charset"`

	StatAddr string `toml:"stat_addr"`

	ServerID uint32 `toml:"server_id"`
	Flavor   string `toml:"flavor"`
	DataDir  string `toml:"data_dir"`

	//SkipMasterData bool   `toml:"skip_master_data"`

	//Sources []SourceConfig `toml:"source"`

	BulkSize int `toml:"bulk_size"`

	FlushBulkTime time.Duration `toml:"flush_bulk_time"`

	SkipNoPkTable bool `toml:"skip_no_pk_table"`

	SubscribeTableRegex []string
	PublishTables []string
}

