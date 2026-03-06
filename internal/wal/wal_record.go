package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type walRecord struct {
	header header
	payload payload
}

func newWalRecord(seq uint64, op Operations, key []byte, val []byte) (walRecord, error) {
	if op != PUT && op != DEL {
		return walRecord{}, fmt.Errorf("unknown operation: %d", op)
	}
	return walRecord{
		payload: payload{
			seq: seq,
			op: op,
			keyLength: uint32(len(key)),
			key: key,
			valLength: uint32(len(val)),
			val: val,
		},
	}, nil
}

func (rec walRecord) encode() ([]byte, error) {
	if rec.payload.op != PUT && rec.payload.op != DEL {
		return nil, fmt.Errorf("unknown operation: %d", rec.payload.op)
	}
	payloadBuf := rec.payload.encode()

	header := header{
		magic: [3]byte{'W', 'A', 'L'},
		version: 1,
		crc32: crc32.ChecksumIEEE(payloadBuf),
		payloadLength: uint32(len(payloadBuf)),
	}
	var headerBuf bytes.Buffer
	binary.Write(&headerBuf, binary.LittleEndian, header)

	return append(headerBuf.Bytes(), payloadBuf...), nil
}

func decodeWalRecord(data []byte) (walRecord, error) {
	if len(data) < 16 {
		return walRecord{}, fmt.Errorf("data too short for header: got %d bytes, need at least 16", len(data))
	}

	header := header{}
	copy(header.magic[:], data[0:3])
	header.version = data[3]
	header.crc32 = binary.LittleEndian.Uint32(data[4:8])
	header.payloadLength = binary.LittleEndian.Uint32(data[8:12])
	header.padding = binary.LittleEndian.Uint32(data[12:16])

	// Verify magic bytes
	if header.magic[0] != 'W' || header.magic[1] != 'A' || header.magic[2] != 'L' {
		return walRecord{}, fmt.Errorf("invalid magic bytes: expected WAL, got %c%c%c", header.magic[0], header.magic[1], header.magic[2])
	}

	payloadBuf := data[16:]
	if len(payloadBuf) != int(header.payloadLength) {
		return walRecord{}, fmt.Errorf("payload length mismatch: expected %d, got %d", header.payloadLength, len(payloadBuf))
	}

	payloadChecksum := crc32.ChecksumIEEE(payloadBuf)
	if payloadChecksum != header.crc32 {
		return walRecord{}, fmt.Errorf("payload checksum mismatch: expected %d, got %d", header.crc32, payloadChecksum)
	}

	if len(payloadBuf) < 21 {
		return walRecord{}, fmt.Errorf("payload too short: got %d bytes, need at least 21", len(payloadBuf))
	}

	keyLength := binary.LittleEndian.Uint32(payloadBuf[9:13])
	valLength := binary.LittleEndian.Uint32(payloadBuf[17:21])

	if len(payloadBuf) < 21 + int(keyLength) {
		return walRecord{}, fmt.Errorf("payload too short for key: got %d bytes, need %d", len(payloadBuf), 21+int(keyLength))
	}

	if len(payloadBuf) < 21 + int(keyLength) + int(valLength) {
		return walRecord{}, fmt.Errorf("payload too short for value: got %d bytes, need %d", len(payloadBuf), 21+int(keyLength)+int(valLength))
	}

	op := Operations(payloadBuf[8])
	if op != PUT && op != DEL {
		return walRecord{}, fmt.Errorf("unknown operation: %d", op)
	}

	payload := payload{
		seq:       binary.LittleEndian.Uint64(payloadBuf[0:8]),
		op:        op,
		keyLength: keyLength,
		valLength: valLength,
		key:       make([]byte, keyLength),
		val:       make([]byte, valLength),
	}
	copy(payload.key, payloadBuf[21:21+keyLength])
	copy(payload.val, payloadBuf[21+keyLength:21+keyLength+valLength])

	return walRecord{
		header: header,
		payload: payload,
	}, nil
}

type header struct {
	magic [3]byte
	version uint8
	crc32 uint32
	payloadLength uint32
	padding uint32 // padding for CPU alignment
}

type payload struct {
	seq uint64
	op Operations
	keyLength uint32
	valLength uint32
	key []byte
	val []byte
}

func (p payload) encode() []byte {
	totalLength := 16 + 1 + 4 + len(p.key) + 4 + len(p.val)
	buf := make([]byte, totalLength)

	binary.LittleEndian.PutUint64(buf[0:8], p.seq)
	buf[8] = uint8(p.op)

	// Lengths fields
	binary.LittleEndian.PutUint32(buf[9:13], p.keyLength)
	binary.LittleEndian.PutUint32(buf[17:21], p.valLength)

	// Actual payload
	copy(buf[21:21 + p.keyLength], p.key)
	copy(buf[21 + p.keyLength:], p.val)

	return buf
}