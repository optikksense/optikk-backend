package config

import "fmt"

type ClickHouseConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Secure   bool   `yaml:"secure"`

	// AutoMigrate, when true, runs chmigrate.Migrator.Up at server boot after
	// opening the ClickHouse connection. Off by default — production deploys
	// should run `./migrate up` explicitly before rolling the binary. Turn on
	// in dev for one-shot bootstrap of a fresh cluster.
	AutoMigrate bool `yaml:"auto_migrate"`
}

func (c Config) ClickHouseDSN() string {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		c.ClickHouse.User,
		c.ClickHouse.Password,
		c.ClickHouse.Host,
		c.ClickHouse.Port,
		c.ClickHouse.Database,
	)
	if c.ClickHouse.Secure {
		dsn += "?secure=true"
	}
	return dsn
}
