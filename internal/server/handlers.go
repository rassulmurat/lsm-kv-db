package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) registerHandlers() {
	s.mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	s.mux.HandleFunc("POST /api/v1/put", s.handlePut)
}

type putRequest struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	if req.Key == "" || req.Value == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Key and value are required"))
		return
	}

	err = s.dbEngine.Put(req.Key, req.Value)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}