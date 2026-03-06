package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)


func TestNewWALWriter(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	if writer == nil {
		t.Fatal("NewWALWriter() returned nil WALWriter")
	}
	if writer.ch == nil {
		t.Fatal("WALWriter channel is nil")
	}
	if writer.stopped == nil {
		t.Fatal("WALWriter stopped channel is nil")
	}

	// Clean up
	if err := writer.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Verify file was created
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file was not created")
	}
}

func TestNewWALWriter_InvalidPath(t *testing.T) {
	// Try to create WAL in a non-existent directory
	_, err := NewWALWriter("/nonexistent/path", 1)
	if err == nil {
		t.Error("NewWALWriter() should fail with invalid path")
	}
}
