package server

import (
	"log"
	"net/http"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
	"github.com/rassulmurat/lsm-kv-db/internal/database"
)

type Server struct {
	mux *http.ServeMux
	httpConfig *config.HttpConfig
	dbEngine *database.Engine
}

func NewServer(httpConfig *config.HttpConfig, dbEngine *database.Engine) *Server {
	mux := http.NewServeMux()

	server := &Server{
		mux: mux,
		httpConfig: httpConfig,
		dbEngine: dbEngine,
	}
	server.registerHandlers()

	return server
}

func (s *Server) Start() {
	http.ListenAndServe(":" + s.httpConfig.Port, s.mux)
	log.Println("Listening on :" + s.httpConfig.Port)
}