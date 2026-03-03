package main

import (
	"github.com/rassulmurat/lsm-kv-db/internal/config"
	"github.com/rassulmurat/lsm-kv-db/internal/database"
	"github.com/rassulmurat/lsm-kv-db/internal/server"
)

func main() {
    var cfg = config.NewConfig()
	var dbEngine = database.NewEngine(cfg)
	var server = server.NewServer(&cfg.HttpConfig, dbEngine)
	server.Start()
}