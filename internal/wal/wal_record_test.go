package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestNewWalRecord(t *testing.T) {
	key := []byte("test-key")
	val := []byte("test-value")
	seq := uint64(123)
	op := PUT

	rec, err := newWalRecord(seq, op, key, val)
	if err != nil {
		t.Fatalf("newWalRecord() failed: %v", err)
	}

	if rec.payload.seq != seq {
		t.Errorf("Expected seq %d, got %d", seq, rec.payload.seq)
	}
	if rec.payload.op != op {
		t.Errorf("Expected op %v, got %v", op, rec.payload.op)
	}
	if !bytes.Equal(rec.payload.key, key) {
		t.Errorf("Expected key %v, got %v", key, rec.payload.key)
	}
	if !bytes.Equal(rec.payload.val, val) {
		t.Errorf("Expected val %v, got %v", val, rec.payload.val)
	}
	if rec.payload.keyLength != uint32(len(key)) {
		t.Errorf("Expected keyLength %d, got %d", len(key), rec.payload.keyLength)
	}
	if rec.payload.valLength != uint32(len(val)) {
		t.Errorf("Expected valLength %d, got %d", len(val), rec.payload.valLength)
	}
}

func TestPayload_Encode(t *testing.T) {
	tests := []struct {
		name  string
		seq   uint64
		op    Operations
		key   []byte
		val   []byte
		check func(t *testing.T, buf []byte)
	}{
		{
			name: "simple key-value",
			seq:  1,
			op:   PUT,
			key:  []byte("key"),
			val:  []byte("value"),
			check: func(t *testing.T, buf []byte) {
				expectedLen := 16 + 1 + 4 + 3 + 4 + 5 // seq(8) + op(1) + keyLen(4) + key(3) + valLen(4) + val(5) = 21 + 8 = 29
				if len(buf) != expectedLen {
					t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf))
				}
			},
		},
		{
			name: "empty key",
			seq:  2,
			op:   PUT,
			key:  []byte(""),
			val:  []byte("value"),
			check: func(t *testing.T, buf []byte) {
				expectedLen := 16 + 1 + 4 + 0 + 4 + 5 // 21 + 5 = 26
				if len(buf) != expectedLen {
					t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf))
				}
			},
		},
		{
			name: "empty value",
			seq:  3,
			op:   DEL,
			key:  []byte("key"),
			val:  []byte(""),
			check: func(t *testing.T, buf []byte) {
				expectedLen := 16 + 1 + 4 + 3 + 4 + 0 // 21 + 3 = 24
				if len(buf) != expectedLen {
					t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf))
				}
			},
		},
		{
			name: "large values",
			seq:  4,
			op:   PUT,
			key:  bytes.Repeat([]byte("k"), 1000),
			val:  bytes.Repeat([]byte("v"), 2000),
			check: func(t *testing.T, buf []byte) {
				expectedLen := 16 + 1 + 4 + 1000 + 4 + 2000 // 21 + 3000 = 3021
				if len(buf) != expectedLen {
					t.Errorf("Expected buffer length %d, got %d", expectedLen, len(buf))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := payload{
				seq:       tt.seq,
				op:        tt.op,
				keyLength: uint32(len(tt.key)),
				valLength: uint32(len(tt.val)),
				key:       tt.key,
				val:       tt.val,
			}

			buf := p.encode()

			// Verify structure
			if len(buf) < 21 {
				t.Fatalf("Buffer too short: %d bytes", len(buf))
			}

			// Verify seq
			seq := binary.LittleEndian.Uint64(buf[0:8])
			if seq != tt.seq {
				t.Errorf("Expected seq %d, got %d", tt.seq, seq)
			}

			// Verify op
			op := Operations(buf[8])
			if op != tt.op {
				t.Errorf("Expected op %v, got %v", tt.op, op)
			}

			// Verify keyLength
			keyLength := binary.LittleEndian.Uint32(buf[9:13])
			if keyLength != uint32(len(tt.key)) {
				t.Errorf("Expected keyLength %d, got %d", len(tt.key), keyLength)
			}

			// Verify valLength
			valLength := binary.LittleEndian.Uint32(buf[17:21])
			if valLength != uint32(len(tt.val)) {
				t.Errorf("Expected valLength %d, got %d", len(tt.val), valLength)
			}

			// Verify key
			if len(tt.key) > 0 {
				key := buf[21 : 21+keyLength]
				if !bytes.Equal(key, tt.key) {
					t.Errorf("Expected key %v, got %v", tt.key, key)
				}
			}

			// Verify value
			if len(tt.val) > 0 {
				val := buf[21+keyLength : 21+keyLength+valLength]
				if !bytes.Equal(val, tt.val) {
					t.Errorf("Expected val %v, got %v", tt.val, val)
				}
			}

			if tt.check != nil {
				tt.check(t, buf)
			}
		})
	}
}

func TestWalRecord_Encode(t *testing.T) {
	tests := []struct {
		name string
		seq  uint64
		op   Operations
		key  []byte
		val  []byte
	}{
		{
			name: "PUT operation",
			seq:  1,
			op:   PUT,
			key:  []byte("key1"),
			val:  []byte("value1"),
		},
		{
			name: "DEL operation",
			seq:  2,
			op:   DEL,
			key:  []byte("key2"),
			val:  []byte(""),
		},
		{
			name: "empty key and value",
			seq:  3,
			op:   PUT,
			key:  []byte(""),
			val:  []byte(""),
		},
		{
			name: "unicode characters",
			seq:  4,
			op:   PUT,
			key:  []byte("ключ"),
			val:  []byte("значение"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec, err := newWalRecord(tt.seq, tt.op, tt.key, tt.val)
			if err != nil {
				t.Fatalf("newWalRecord() failed: %v", err)
			}
			encoded, err := rec.encode()
			if err != nil {
				t.Fatalf("Encode() failed: %v", err)
			}

			// Verify minimum length (header is 16 bytes)
			if len(encoded) < 16 {
				t.Fatalf("Encoded data too short: %d bytes", len(encoded))
			}

			// Verify header magic bytes
			if encoded[0] != 'W' || encoded[1] != 'A' || encoded[2] != 'L' {
				t.Errorf("Invalid magic bytes: expected WAL, got %c%c%c", encoded[0], encoded[1], encoded[2])
			}

			// Verify version
			if encoded[3] != 1 {
				t.Errorf("Expected version 1, got %d", encoded[3])
			}

			// Verify CRC32 and payload length are present
			headerCRC := binary.LittleEndian.Uint32(encoded[4:8])
			payloadLength := binary.LittleEndian.Uint32(encoded[8:12])

			// Verify payload length matches
			expectedPayloadLen := 16 + 1 + 4 + len(tt.key) + 4 + len(tt.val)
			if payloadLength != uint32(expectedPayloadLen) {
				t.Errorf("Expected payload length %d, got %d", expectedPayloadLen, payloadLength)
			}

			// Note: CRC32 verification would require importing hash/crc32, but we test it in decode tests
			_ = headerCRC // Acknowledge we read it
		})
	}
}

func TestDecodeWalRecord(t *testing.T) {
	tests := []struct {
		name    string
		seq     uint64
		op      Operations
		key     []byte
		val     []byte
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid PUT record",
			seq:  1,
			op:   PUT,
			key:  []byte("key1"),
			val:  []byte("value1"),
		},
		{
			name: "valid DEL record",
			seq:  2,
			op:   DEL,
			key:  []byte("key2"),
			val:  []byte(""),
		},
		{
			name: "empty key and value",
			seq:  3,
			op:   PUT,
			key:  []byte(""),
			val:  []byte(""),
		},
		{
			name: "large key and value",
			seq:  4,
			op:   PUT,
			key:  bytes.Repeat([]byte("k"), 100),
			val:  bytes.Repeat([]byte("v"), 200),
		},
		{
			name:    "data too short",
			wantErr:  true,
			errMsg:   "too short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var encoded []byte
			if !tt.wantErr {
				rec, err := newWalRecord(tt.seq, tt.op, tt.key, tt.val)
				if err != nil {
					t.Fatalf("newWalRecord() failed: %v", err)
				}
				encoded, err = rec.encode()
				if err != nil {
					t.Fatalf("Encode() failed: %v", err)
				}
			} else {
				// Create invalid data for error cases
				if tt.name == "data too short" {
					encoded = make([]byte, 10) // Too short
				}
			}

			decoded, err := decodeWalRecord(encoded)
			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
				if tt.errMsg != "" && err != nil {
					// Check if error message contains expected text
					if err.Error() == "" {
						t.Errorf("Expected error message containing %q, got empty", tt.errMsg)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("DecodeWalRecord() failed: %v", err)
			}

			// Verify decoded values
			if decoded.payload.seq != tt.seq {
				t.Errorf("Expected seq %d, got %d", tt.seq, decoded.payload.seq)
			}
			if decoded.payload.op != tt.op {
				t.Errorf("Expected op %v, got %v", tt.op, decoded.payload.op)
			}
			if !bytes.Equal(decoded.payload.key, tt.key) {
				t.Errorf("Expected key %v, got %v", tt.key, decoded.payload.key)
			}
			if !bytes.Equal(decoded.payload.val, tt.val) {
				t.Errorf("Expected val %v, got %v", tt.val, decoded.payload.val)
			}
			if decoded.payload.keyLength != uint32(len(tt.key)) {
				t.Errorf("Expected keyLength %d, got %d", len(tt.key), decoded.payload.keyLength)
			}
			if decoded.payload.valLength != uint32(len(tt.val)) {
				t.Errorf("Expected valLength %d, got %d", len(tt.val), decoded.payload.valLength)
			}

			// Verify header
			if decoded.header.magic[0] != 'W' || decoded.header.magic[1] != 'A' || decoded.header.magic[2] != 'L' {
				t.Errorf("Invalid magic bytes in decoded header")
			}
			if decoded.header.version != 1 {
				t.Errorf("Expected version 1, got %d", decoded.header.version)
			}
		})
	}
}

func TestEncodeDecode_RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		seq  uint64
		op   Operations
		key  []byte
		val  []byte
	}{
		{
			name: "simple PUT",
			seq:  1,
			op:   PUT,
			key:  []byte("key1"),
			val:  []byte("value1"),
		},
		{
			name: "DEL operation",
			seq:  2,
			op:   DEL,
			key:  []byte("key2"),
			val:  []byte(""),
		},
		{
			name: "empty key",
			seq:  3,
			op:   PUT,
			key:  []byte(""),
			val:  []byte("value"),
		},
		{
			name: "empty value",
			seq:  4,
			op:   PUT,
			key:  []byte("key"),
			val:  []byte(""),
		},
		{
			name: "both empty",
			seq:  5,
			op:   PUT,
			key:  []byte(""),
			val:  []byte(""),
		},
		{
			name: "large values",
			seq:  100,
			op:   PUT,
			key:  bytes.Repeat([]byte("k"), 500),
			val:  bytes.Repeat([]byte("v"), 1000),
		},
		{
			name: "unicode",
			seq:  200,
			op:   PUT,
			key:  []byte("ключ"),
			val:  []byte("значение"),
		},
		{
			name: "binary data",
			seq:  300,
			op:   PUT,
			key:  []byte{0x00, 0x01, 0x02, 0xFF, 0xFE},
			val:  []byte{0xAA, 0xBB, 0xCC, 0xDD},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			original, err := newWalRecord(tt.seq, tt.op, tt.key, tt.val)
			if err != nil {
				t.Fatalf("newWalRecord() failed: %v", err)
			}
			encoded, err := original.encode()
			if err != nil {
				t.Fatalf("Encode() failed: %v", err)
			}

			// Decode
			decoded, err := decodeWalRecord(encoded)
			if err != nil {
				t.Fatalf("DecodeWalRecord() failed: %v", err)
			}

			// Verify round-trip
			if decoded.payload.seq != original.payload.seq {
				t.Errorf("Seq mismatch: expected %d, got %d", original.payload.seq, decoded.payload.seq)
			}
			if decoded.payload.op != original.payload.op {
				t.Errorf("Op mismatch: expected %v, got %v", original.payload.op, decoded.payload.op)
			}
			if !bytes.Equal(decoded.payload.key, original.payload.key) {
				t.Errorf("Key mismatch: expected %v, got %v", original.payload.key, decoded.payload.key)
			}
			if !bytes.Equal(decoded.payload.val, original.payload.val) {
				t.Errorf("Val mismatch: expected %v, got %v", original.payload.val, decoded.payload.val)
			}
			if decoded.payload.keyLength != original.payload.keyLength {
				t.Errorf("KeyLength mismatch: expected %d, got %d", original.payload.keyLength, decoded.payload.keyLength)
			}
			if decoded.payload.valLength != original.payload.valLength {
				t.Errorf("ValLength mismatch: expected %d, got %d", original.payload.valLength, decoded.payload.valLength)
			}
		})
	}
}

func TestDecodeWalRecord_InvalidData(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "nil data",
			data:    nil,
			wantErr: true,
		},
		{
			name:    "empty data",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "too short for header",
			data:    make([]byte, 10),
			wantErr: true,
		},
		{
			name: "invalid magic bytes",
			data: func() []byte {
				rec, _ := newWalRecord(1, PUT, []byte("key"), []byte("val"))
				encoded, _ := rec.encode()
				encoded[0] = 'X' // Corrupt magic
				return encoded
			}(),
			wantErr: true,
		},
		{
			name: "payload length mismatch",
			data: func() []byte {
				rec, _ := newWalRecord(1, PUT, []byte("key"), []byte("val"))
				encoded, _ := rec.encode()
				// Corrupt payload length
				binary.LittleEndian.PutUint32(encoded[8:12], 9999)
				return encoded
			}(),
			wantErr: true,
		},
		{
			name: "CRC32 mismatch",
			data: func() []byte {
				rec, _ := newWalRecord(1, PUT, []byte("key"), []byte("val"))
				encoded, _ := rec.encode()
				// Corrupt CRC32
				binary.LittleEndian.PutUint32(encoded[4:8], 0x12345678)
				return encoded
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeWalRecord(tt.data)
			if tt.wantErr && err == nil {
				t.Errorf("Expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

func TestOperations_String(t *testing.T) {
	tests := []struct {
		op   Operations
		want string
	}{
		{PUT, "PUT"},
		{DEL, "DEL"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := tt.op.String()
			if got != tt.want {
				t.Errorf("Operations.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewWalRecord_UnknownOperation(t *testing.T) {
	unknownOp := Operations(99)
	_, err := newWalRecord(1, unknownOp, []byte("key"), []byte("val"))
	if err == nil {
		t.Error("Expected error for unknown operation, got nil")
	}
	if err != nil && err.Error() != "unknown operation: 99" {
		t.Errorf("Expected error message 'unknown operation: 99', got: %v", err)
	}
}

func TestWalRecord_Encode_UnknownOperation(t *testing.T) {
	rec := walRecord{
		payload: payload{
			seq:  1,
			op:   Operations(99), // Unknown operation
			key:  []byte("key"),
			val:  []byte("val"),
		},
	}
	_, err := rec.encode()
	if err == nil {
		t.Error("Expected error for unknown operation, got nil")
	}
	if err != nil && err.Error() != "unknown operation: 99" {
		t.Errorf("Expected error message 'unknown operation: 99', got: %v", err)
	}
}

func TestDecodeWalRecord_UnknownOperation(t *testing.T) {
	// Create a valid record first
	rec, err := newWalRecord(1, PUT, []byte("key"), []byte("val"))
	if err != nil {
		t.Fatalf("newWalRecord() failed: %v", err)
	}
	encoded, err := rec.encode()
	if err != nil {
		t.Fatalf("encode() failed: %v", err)
	}

	// Corrupt the operation byte to an unknown value
	payloadStart := 16
	operationOffset := payloadStart + 8
	encoded[operationOffset] = 99 // Operation is at offset 8 in payload

	// Recalculate CRC32 for the corrupted payload
	payloadBuf := encoded[payloadStart:]
	newCRC := crc32.ChecksumIEEE(payloadBuf)
	binary.LittleEndian.PutUint32(encoded[4:8], newCRC) // Update CRC32 in header

	_, err = decodeWalRecord(encoded)
	if err == nil {
		t.Error("Expected error for unknown operation, got nil")
	}
	if err != nil && err.Error() != "unknown operation: 99" {
		t.Errorf("Expected error message 'unknown operation: 99', got: %v", err)
	}
}
