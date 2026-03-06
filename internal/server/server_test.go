package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
	"github.com/rassulmurat/lsm-kv-db/internal/database"
)

func setupTestServer(t *testing.T) *Server {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, ".wal/")

	cfg := &config.Config{
		HttpConfig: config.HttpConfig{
			Port: "8080",
		},
		WalConfig: config.WalConfig{
			DirPath:          walPath,
			MaxBatchBytes: 1024 * 1024,
			MaxBatchDelay: 100 * time.Millisecond,
		},
	}

	dbEngine := database.NewEngine(cfg)
	httpConfig := &config.HttpConfig{Port: "8080"}
	server := NewServer(httpConfig, dbEngine)

	return server
}

func TestNewServer(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := &config.Config{
		HttpConfig: config.HttpConfig{
			Port: "8080",
		},
		WalConfig: config.WalConfig{
			DirPath:          walPath,
			MaxBatchBytes: 1024 * 1024,
			MaxBatchDelay: 100 * time.Millisecond,
		},
	}

	dbEngine := database.NewEngine(cfg)
	httpConfig := &config.HttpConfig{Port: "8080"}
	server := NewServer(httpConfig, dbEngine)

	if server == nil {
		t.Fatal("NewServer() returned nil")
	}
	if server.mux == nil {
		t.Fatal("Server mux is nil")
	}
	if server.httpConfig == nil {
		t.Fatal("Server httpConfig is nil")
	}
	if server.dbEngine == nil {
		t.Fatal("Server dbEngine is nil")
	}
}

func TestHealthEndpoint(t *testing.T) {
	server := setupTestServer(t)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	body := w.Body.String()
	if body != "OK" {
		t.Errorf("Expected body 'OK', got %q", body)
	}
}

func TestHandlePut_Success(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "test-key",
		Value: "test-value",
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	body := w.Body.String()
	if body != "OK" {
		t.Errorf("Expected body 'OK', got %q", body)
	}
}

func TestHandlePut_MissingKey(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "",
		Value: "test-value",
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}

	body := w.Body.String()
	expected := "Key and value are required"
	if body != expected {
		t.Errorf("Expected body %q, got %q", expected, body)
	}
}

func TestHandlePut_MissingValue(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "test-key",
		Value: "",
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}

	body := w.Body.String()
	expected := "Key and value are required"
	if body != expected {
		t.Errorf("Expected body %q, got %q", expected, body)
	}
}

func TestHandlePut_EmptyKeyAndValue(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "",
		Value: "",
	}
	jsonBody, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}

	body := w.Body.String()
	expected := "Key and value are required"
	if body != expected {
		t.Errorf("Expected body %q, got %q", expected, body)
	}
}

func TestHandlePut_InvalidJSON(t *testing.T) {
	server := setupTestServer(t)

	invalidJSON := `{"key": "test-key", "value": invalid}`
	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBufferString(invalidJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}

	body := w.Body.String()
	if body == "" {
		t.Error("Expected error message in body, got empty string")
	}
}

func TestHandlePut_MalformedJSON(t *testing.T) {
	server := setupTestServer(t)

	malformedJSON := `{"key": "test-key", "value": "test-value"`
	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBufferString(malformedJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandlePut_EmptyBody(t *testing.T) {
	server := setupTestServer(t)

	req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBufferString(""))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestHandlePut_MultipleEntries(t *testing.T) {
	server := setupTestServer(t)

	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, entry := range entries {
		reqBody := putRequest{
			Key:   entry.key,
			Value: entry.value,
		}
		jsonBody, _ := json.Marshal(reqBody)

		req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		server.mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d for key %q, got %d", http.StatusOK, entry.key, w.Code)
		}
	}
}

func TestHandlePut_SpecialCharacters(t *testing.T) {
	server := setupTestServer(t)

	testCases := []struct {
		name  string
		key   string
		value string
	}{
		{"unicode", "ключ", "значение"},
		{"special chars", "key:with:colons", "value with spaces"},
		{"json-like", `{"key":"value"}`, `{"nested":"object"}`},
		{"newlines", "key\nwith\nnewlines", "value\nwith\nnewlines"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reqBody := putRequest{
				Key:   tc.key,
				Value: tc.value,
			}
			jsonBody, _ := json.Marshal(reqBody)

			req := httptest.NewRequest("POST", "/api/v1/put", bytes.NewBuffer(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			server.mux.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
			}
		})
	}
}

func TestHandlePut_WrongMethod(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "test-key",
		Value: "test-value",
	}
	jsonBody, _ := json.Marshal(reqBody)

	// Try GET instead of POST
	req := httptest.NewRequest("GET", "/api/v1/put", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	// Should return 405 Method Not Allowed or 404 Not Found
	if w.Code != http.StatusMethodNotAllowed && w.Code != http.StatusNotFound {
		t.Errorf("Expected status code 405 or 404, got %d", w.Code)
	}
}

func TestHandlePut_WrongPath(t *testing.T) {
	server := setupTestServer(t)

	reqBody := putRequest{
		Key:   "test-key",
		Value: "test-value",
	}
	jsonBody, _ := json.Marshal(reqBody)

	// Try wrong path
	req := httptest.NewRequest("POST", "/api/v1/wrong", bytes.NewBuffer(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	server.mux.ServeHTTP(w, req)

	// Should return 404 Not Found
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status code %d, got %d", http.StatusNotFound, w.Code)
	}
}
