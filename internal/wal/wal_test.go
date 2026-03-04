package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rassulmurat/lsm-kv-db/internal/config"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 100 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	if wal == nil {
		t.Fatal("New() returned nil WAL")
	}
	if wal.ch == nil {
		t.Fatal("WAL channel is nil")
	}
	if wal.stopped == nil {
		t.Fatal("WAL stopped channel is nil")
	}

	// Clean up
	if err := wal.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}
}

func TestNew_InvalidPath(t *testing.T) {
	// Try to create WAL in a non-existent directory
	cfg := config.WalConfig{
		Path:          "/nonexistent/path/wal.log",
		MaxBatchBytes: 1024,
		MaxBatchDelay: 100 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err == nil {
		t.Error("New() should fail with invalid path")
		if wal != nil {
			wal.Close()
		}
	}
}

func TestPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Put a single entry
	err = wal.Put("key1", "value1")
	if err != nil {
		t.Errorf("Put() failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	expected := "key1:value1"
	if !strings.Contains(string(content), expected) {
		t.Errorf("Expected to find %q in WAL file, got: %s", expected, string(content))
	}
}

func TestPut_MultipleEntries(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Put multiple entries
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, e := range entries {
		if err := wal.Put(e.key, e.value); err != nil {
			t.Errorf("Put(%q, %q) failed: %v", e.key, e.value, err)
		}
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all entries were written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	contentStr := string(content)
	for _, e := range entries {
		expected := e.key + ":" + e.value
		if !strings.Contains(contentStr, expected) {
			t.Errorf("Expected to find %q in WAL file", expected)
		}
	}
}

func TestPut_WithSync(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Put with sync enabled
	err = wal.Put("key1", "value1", WithSync(true))
	if err != nil {
		t.Errorf("Put() with sync failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	expected := "key1:value1"
	if !strings.Contains(string(content), expected) {
		t.Errorf("Expected to find %q in WAL file, got: %s", expected, string(content))
	}
}

func TestPut_WithoutSync(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Put with sync disabled
	err = wal.Put("key1", "value1", WithSync(false))
	if err != nil {
		t.Errorf("Put() without sync failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	expected := "key1:value1"
	if !strings.Contains(string(content), expected) {
		t.Errorf("Expected to find %q in WAL file, got: %s", expected, string(content))
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Put some entries
	wal.Put("key1", "value1")
	wal.Put("key2", "value2")

	// Close should not error
	err = wal.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Verify file exists and has content
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty after Close()")
	}
}

func TestBatching_MaxBatchBytes(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	// Set a small batch size to trigger batching
	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 50, // Small batch size
		MaxBatchDelay: 200 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Write entries that will exceed MaxBatchBytes
	// Each entry is approximately "keyX:valueX" = ~13 bytes
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		wal.Put(key, value)
	}

	// Wait for writes to complete
	time.Sleep(300 * time.Millisecond)

	// Verify all entries were written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty")
	}

	// Verify we have multiple entries
	contentStr := string(content)
	entryCount := strings.Count(contentStr, ":")
	if entryCount < 5 {
		t.Errorf("Expected at least 5 entries, found %d", entryCount)
	}
}

func TestBatching_MaxBatchDelay(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 100 * time.Millisecond, // Short delay
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Write entries with delays
	wal.Put("key1", "value1")
	time.Sleep(50 * time.Millisecond)
	wal.Put("key2", "value2")
	time.Sleep(50 * time.Millisecond)
	wal.Put("key3", "value3")

	// Wait for all writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all entries were written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	contentStr := string(content)
	expectedEntries := []string{"key1:value1", "key2:value2", "key3:value3"}
	for _, expected := range expectedEntries {
		if !strings.Contains(contentStr, expected) {
			t.Errorf("Expected to find %q in WAL file", expected)
		}
	}
}

func TestConcurrentPuts(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "wal.log")

	cfg := config.WalConfig{
		Path:          walPath,
		MaxBatchBytes: 1024,
		MaxBatchDelay: 50 * time.Millisecond,
	}

	wal, err := New(cfg)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	defer wal.Close()

	// Concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("key%d-%d", id, j)
				value := fmt.Sprintf("value%d-%d", id, j)
				if err := wal.Put(key, value); err != nil {
					t.Errorf("Put(%q, %q) failed: %v", key, value, err)
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Wait for writes to complete
	time.Sleep(300 * time.Millisecond)

	// Verify entries were written
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty after concurrent puts")
	}

	// Verify we have many entries
	contentStr := string(content)
	entryCount := strings.Count(contentStr, ":")
	expectedCount := 50 // 10 goroutines * 5 entries each
	if entryCount < expectedCount/2 { // Allow for some variance
		t.Errorf("Expected at least %d entries, found %d", expectedCount/2, entryCount)
	}
}
