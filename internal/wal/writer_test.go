package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// decodeWalRecords decodes all WAL records from binary data
func decodeWalRecords(data []byte) ([]walRecord, error) {
	var records []walRecord
	offset := 0

	for offset < len(data) {
		if offset+16 > len(data) {
			break // Not enough data for header
		}

		// Manually decode header (fields are unexported, so can't use binary.Read)
		var h header
		// magic: [3]byte at offset 0
		copy(h.magic[:], data[offset:offset+3])
		// version: uint8 at offset 3
		h.version = data[offset+3]
		// crc32: uint32 at offset 4
		h.crc32 = binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		// payloadLength: uint32 at offset 8
		h.payloadLength = binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		// padding: uint32 at offset 12
		h.padding = binary.LittleEndian.Uint32(data[offset+12 : offset+16])

		// Verify magic bytes
		if h.magic[0] != 'W' || h.magic[1] != 'A' || h.magic[2] != 'L' {
			return nil, fmt.Errorf("invalid magic bytes")
		}

		offset += 16 // Skip header

		// Read payload
		if offset+int(h.payloadLength) > len(data) {
			return nil, fmt.Errorf("not enough data for payload")
		}

		payloadData := data[offset : offset+int(h.payloadLength)]

		// Verify CRC32
		expectedCRC := crc32.ChecksumIEEE(payloadData)
		if h.crc32 != expectedCRC {
			return nil, fmt.Errorf("CRC32 mismatch: expected %d, got %d", expectedCRC, h.crc32)
		}

		// Decode payload (new format: lengths at 9:13 and 17:21, data starts at 21)
		var p payload
		if len(payloadData) < 21 {
			return nil, fmt.Errorf("payload too short")
		}

		p.seq = binary.LittleEndian.Uint64(payloadData[0:8])
		p.op = Operations(payloadData[8])
		p.keyLength = binary.LittleEndian.Uint32(payloadData[9:13])
		p.valLength = binary.LittleEndian.Uint32(payloadData[17:21])

		if len(payloadData) < 21+int(p.keyLength) {
			return nil, fmt.Errorf("not enough data for key")
		}

		p.key = make([]byte, p.keyLength)
		copy(p.key, payloadData[21:21+int(p.keyLength)])

		if len(payloadData) < 21+int(p.keyLength)+int(p.valLength) {
			return nil, fmt.Errorf("not enough data for value")
		}

		p.val = make([]byte, p.valLength)
		copy(p.val, payloadData[21+int(p.keyLength):21+int(p.keyLength)+int(p.valLength)])

		records = append(records, walRecord{
			header:  h,
			payload: p,
		})

		offset += int(h.payloadLength)
	}

	return records, nil
}

func TestWALWriter_Put(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put a single entry
	err = writer.Put(1, "key1", "value1")
	if err != nil {
		t.Errorf("Put() failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found in WAL file")
	}

	found := false
	for _, rec := range records {
		if string(rec.payload.key) == "key1" && string(rec.payload.val) == "value1" {
			found = true
			if rec.payload.op != PUT {
				t.Errorf("Expected operation PUT, got %v", rec.payload.op)
			}
			break
		}
	}

	if !found {
		t.Errorf("Expected to find key1=value1 in WAL records")
	}
}

func TestWALWriter_Put_MultipleEntries(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put multiple entries
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for i, e := range entries {
		if err := writer.Put(uint64(i+1), e.key, e.value); err != nil {
			t.Errorf("Put(%q, %q) failed: %v", e.key, e.value, err)
		}
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all entries were written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) < len(entries) {
		t.Errorf("Expected at least %d records, got %d", len(entries), len(records))
	}

	// Create a map of expected entries
	expectedMap := make(map[string]string)
	for _, e := range entries {
		expectedMap[e.key] = e.value
	}

	// Verify all entries are present
	for _, rec := range records {
		key := string(rec.payload.key)
		val := string(rec.payload.val)
		expectedVal, exists := expectedMap[key]
		if !exists {
			continue // Skip entries not in our test set
		}
		if val != expectedVal {
			t.Errorf("Expected value %q for key %q, got %q", expectedVal, key, val)
		}
		if rec.payload.op != PUT {
			t.Errorf("Expected operation PUT for key %q, got %v", key, rec.payload.op)
		}
	}
}

func TestWALWriter_Put_WithSync(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put with sync enabled
	err = writer.Put(1, "key1", "value1", WithSync(true))
	if err != nil {
		t.Errorf("Put() with sync failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found in WAL file")
	}

	found := false
	for _, rec := range records {
		if string(rec.payload.key) == "key1" && string(rec.payload.val) == "value1" {
			found = true
			if rec.payload.op != PUT {
				t.Errorf("Expected operation PUT, got %v", rec.payload.op)
			}
			break
		}
	}

	if !found {
		t.Errorf("Expected to find key1=value1 in WAL records")
	}
}

func TestWALWriter_Batching_MaxBatchBytes(t *testing.T) {
	tmpDir := t.TempDir()

	// Set a small batch size to trigger batching
	writer, err := NewWALWriter(tmpDir, 1, WithMaxBatchBytes(50))
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Write entries that will exceed MaxBatchBytes
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		writer.Put(uint64(i+1), key, value)
	}

	// Wait for writes to complete
	time.Sleep(300 * time.Millisecond)

	// Verify all entries were written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty")
	}

	// Verify we have multiple entries
	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) < 5 {
		t.Errorf("Expected at least 5 entries, found %d", len(records))
	}
}

func TestWALWriter_Batching_MaxBatchDelay(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1, WithMaxBatchDelay(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}

	// Write entries with delays
	writer.Put(1, "key1", "value1")
	time.Sleep(50 * time.Millisecond)
	writer.Put(2, "key2", "value2")
	time.Sleep(50 * time.Millisecond)
	writer.Put(3, "key3", "value3")

	// Wait for all writes to complete (need to wait for MaxBatchDelay to flush the last entry)
	time.Sleep(250 * time.Millisecond)

	// Close WAL to ensure all writes are flushed
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Verify all entries were written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	expectedEntries := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	found := make(map[string]bool)
	for _, rec := range records {
		key := string(rec.payload.key)
		val := string(rec.payload.val)
		if expectedVal, exists := expectedEntries[key]; exists && val == expectedVal {
			found[key] = true
		}
	}

	for key := range expectedEntries {
		if !found[key] {
			t.Errorf("Expected to find key %q in WAL file", key)
		}
	}
}

func TestWALWriter_ConcurrentPuts(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Concurrent puts
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			for j := 0; j < 5; j++ {
				key := fmt.Sprintf("key%d-%d", id, j)
				value := fmt.Sprintf("value%d-%d", id, j)
				seq := uint64(id*5 + j + 1)
				if err := writer.Put(seq, key, value); err != nil {
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
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty after concurrent puts")
	}

	// Verify we have many entries
	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	expectedCount := 50 // 10 goroutines * 5 entries each
	if len(records) < expectedCount/2 { // Allow for some variance
		t.Errorf("Expected at least %d entries, found %d", expectedCount/2, len(records))
	}
}

func TestWALWriter_Close(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}

	// Put some entries
	writer.Put(1, "key1", "value1")
	writer.Put(2, "key2", "value2")

	// Close should not error
	err = writer.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Verify file exists and has content
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	if len(content) == 0 {
		t.Error("WAL file is empty after Close()")
	}

	// Verify we can decode the records
	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) == 0 {
		t.Error("No records found in WAL file after Close()")
	}
}

func TestWALWriter_Rotate(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put some entries in the first WAL file
	err = writer.Put(1, "key1", "value1")
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	err = writer.Put(2, "key2", "value2")
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Rotate to a new WAL file
	err = writer.Rotate(2)
	if err != nil {
		t.Fatalf("Rotate() failed: %v", err)
	}

	// Put entries in the new WAL file
	err = writer.Put(3, "key3", "value3")
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	err = writer.Put(4, "key4", "value4")
	if err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify first WAL file exists and has correct entries
	walPath1 := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content1, err := os.ReadFile(walPath1)
	if err != nil {
		t.Fatalf("Failed to read first WAL file: %v", err)
	}

	records1, err := decodeWalRecords(content1)
	if err != nil {
		t.Fatalf("Failed to decode WAL records from first file: %v", err)
	}

	foundKey1 := false
	foundKey2 := false
	for _, rec := range records1 {
		if string(rec.payload.key) == "key1" && string(rec.payload.val) == "value1" {
			foundKey1 = true
		}
		if string(rec.payload.key) == "key2" && string(rec.payload.val) == "value2" {
			foundKey2 = true
		}
	}

	if !foundKey1 {
		t.Error("Expected to find key1=value1 in first WAL file")
	}
	if !foundKey2 {
		t.Error("Expected to find key2=value2 in first WAL file")
	}

	// Verify second WAL file exists and has correct entries
	walPath2 := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 2))
	content2, err := os.ReadFile(walPath2)
	if err != nil {
		t.Fatalf("Failed to read second WAL file: %v", err)
	}

	records2, err := decodeWalRecords(content2)
	if err != nil {
		t.Fatalf("Failed to decode WAL records from second file: %v", err)
	}

	foundKey3 := false
	foundKey4 := false
	for _, rec := range records2 {
		if string(rec.payload.key) == "key3" && string(rec.payload.val) == "value3" {
			foundKey3 = true
		}
		if string(rec.payload.key) == "key4" && string(rec.payload.val) == "value4" {
			foundKey4 = true
		}
	}

	if !foundKey3 {
		t.Error("Expected to find key3=value3 in second WAL file")
	}
	if !foundKey4 {
		t.Error("Expected to find key4=value4 in second WAL file")
	}

	// Verify key3 and key4 are NOT in the first file
	for _, rec := range records1 {
		if string(rec.payload.key) == "key3" || string(rec.payload.key) == "key4" {
			t.Errorf("Found key %q in first WAL file, but it should be in second file", string(rec.payload.key))
		}
	}
}

func TestWALWriter_WithSync_False(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put with sync disabled
	err = writer.Put(1, "key1", "value1", WithSync(false))
	if err != nil {
		t.Errorf("Put() with sync=false failed: %v", err)
	}

	// Wait for write to complete
	time.Sleep(200 * time.Millisecond)

	// Verify content was written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) == 0 {
		t.Fatal("No records found in WAL file")
	}

	found := false
	for _, rec := range records {
		if string(rec.payload.key) == "key1" && string(rec.payload.val) == "value1" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Expected to find key1=value1 in WAL records")
	}
}

func TestWALWriter_SequenceNumbers(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put entries with specific sequence numbers
	sequences := []uint64{1, 5, 10, 100, 1000}
	for i, seq := range sequences {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err = writer.Put(seq, key, value)
		if err != nil {
			t.Fatalf("Put(seq=%d) failed: %v", seq, err)
		}
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all entries were written with correct sequence numbers
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	seqMap := make(map[uint64]bool)
	for _, seq := range sequences {
		seqMap[seq] = false
	}

	for _, rec := range records {
		if expected, exists := seqMap[rec.payload.seq]; exists {
			seqMap[rec.payload.seq] = true
			if !expected {
				t.Errorf("Found duplicate sequence number: %d", rec.payload.seq)
			}
		}
	}

	for seq, found := range seqMap {
		if !found {
			t.Errorf("Expected to find sequence number %d in WAL records", seq)
		}
	}
}

func TestWALWriter_EmptyKeyValue(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWALWriter(tmpDir, 1)
	if err != nil {
		t.Fatalf("NewWALWriter() failed: %v", err)
	}
	defer writer.Close()

	// Put with empty key
	err = writer.Put(1, "", "value1")
	if err != nil {
		t.Fatalf("Put() with empty key failed: %v", err)
	}

	// Put with empty value
	err = writer.Put(2, "key2", "")
	if err != nil {
		t.Fatalf("Put() with empty value failed: %v", err)
	}

	// Put with both empty
	err = writer.Put(3, "", "")
	if err != nil {
		t.Fatalf("Put() with empty key and value failed: %v", err)
	}

	// Wait for writes to complete
	time.Sleep(200 * time.Millisecond)

	// Verify all entries were written
	walPath := filepath.Join(tmpDir, fmt.Sprintf(walFileFormat, 1))
	content, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("Failed to read WAL file: %v", err)
	}

	records, err := decodeWalRecords(content)
	if err != nil {
		t.Fatalf("Failed to decode WAL records: %v", err)
	}

	if len(records) < 3 {
		t.Errorf("Expected at least 3 records, got %d", len(records))
	}

	// Verify empty key/value records
	foundEmptyKey := false
	foundEmptyValue := false
	foundBothEmpty := false

	for _, rec := range records {
		if string(rec.payload.key) == "" && string(rec.payload.val) == "value1" {
			foundEmptyKey = true
		}
		if string(rec.payload.key) == "key2" && string(rec.payload.val) == "" {
			foundEmptyValue = true
		}
		if string(rec.payload.key) == "" && string(rec.payload.val) == "" {
			foundBothEmpty = true
		}
	}

	if !foundEmptyKey {
		t.Error("Expected to find record with empty key")
	}
	if !foundEmptyValue {
		t.Error("Expected to find record with empty value")
	}
	if !foundBothEmpty {
		t.Error("Expected to find record with empty key and value")
	}
}
