// Copyright 2023-2025 The Oxia Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package database

import (
	"encoding/binary"
	"hash/crc64"
)

const (
	// ISO polynomial for CRC64 - commonly used for data integrity.
	crc64Polynomial = 0xD800000000000000
)

var crc64Table = crc64.MakeTable(crc64Polynomial)

// DBFingerprint represents a cumulative CRC64 checksum for database state.
type DBFingerprint uint64

// NewDBFingerprint creates a new DB fingerprint from data and previous fingerprint.
func NewDBFingerprint(prev DBFingerprint, data []byte) DBFingerprint {
	// XOR the previous fingerprint with the new data CRC as per proposal
	prevCRC := uint64(prev)
	dataCRC := crc64.Checksum(data, crc64Table)

	// XOR operation as specified: CRC_db(n) = CRC_db(n-1) XOR CRC64(batch.Repr)
	result := prevCRC ^ dataCRC

	return DBFingerprint(result)
}

// Bytes returns the 8-byte representation of the fingerprint.
func (f DBFingerprint) Bytes() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(f))
	return b
}

// DBFingerprintFromBytes creates a DB fingerprint from bytes.
func DBFingerprintFromBytes(b []byte) DBFingerprint {
	if len(b) < 8 {
		return 0
	}
	return DBFingerprint(binary.LittleEndian.Uint64(b))
}

// Uint64 returns the uint64 value of the fingerprint.
func (f DBFingerprint) Uint64() uint64 {
	return uint64(f)
}
