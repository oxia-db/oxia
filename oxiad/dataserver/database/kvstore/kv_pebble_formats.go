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

package kvstore

import (
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/pkg/errors"

	"github.com/oxia-db/oxia/common/proto"
	"github.com/oxia-db/oxia/oxiad/coordinator/model"

	"github.com/oxia-db/oxia/common/compare"
)

const (
	markerFileName                          = "oxia-key-encoding-format"
	keyEncodingFormatOldCompareHierarchical = "old-compare-hierarchical"
)

func makeDbBackupPath(path string) string {
	return path + "-backup"
}

func makeSwapTmpDbPath(path string, encoder compare.Encoder) string {
	return path + "-tmp-" + encoder.Name()
}

type pebbleDbConversion struct {
	dbPath string
	log    *slog.Logger
	kvTrap *KvTrap
}

func newPebbleDbConversion(log *slog.Logger, dbPath string, kvTrap *KvTrap) *pebbleDbConversion {
	return &pebbleDbConversion{
		dbPath: dbPath,
		log:    log,
		kvTrap: kvTrap,
	}
}

func pathExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func (p *pebbleDbConversion) configForOldCompareHierarchical() *pebble.Options {
	return &pebble.Options{
		Comparer:           OxiaSlashSpanComparer,
		DisableWAL:         true,
		Logger:             &pebbleLogger{p.log},
		FormatMajorVersion: pebble.FormatVirtualSSTables,
	}
}

func (p *pebbleDbConversion) configForNewerFormat() *pebble.Options {
	levelOptions := [7]pebble.LevelOptions{}
	levelOptions[0] = pebble.LevelOptions{
		BlockSize: 64 * 1024,
		Compression: func() *sstable.CompressionProfile {
			return sstable.NoCompression
		},
		FilterPolicy: bloom.FilterPolicy(10),
	}

	for i := 1; i < len(levelOptions); i++ {
		levelOptions[i] = pebble.LevelOptions{
			BlockSize: 64 * 1024,
			Compression: func() *sstable.CompressionProfile {
				return sstable.GoodCompression
			},
			FilterPolicy: bloom.FilterPolicy(10),
		}
	}
	return &pebble.Options{
		DisableWAL:         true,
		Logger:             &pebbleLogger{p.log},
		FormatMajorVersion: pebble.FormatVirtualSSTables,
		Levels:             levelOptions,
	}
}

func (p *pebbleDbConversion) checkConvertDB(desiredEncoding compare.Encoder) error {
	dbBackPath := makeDbBackupPath(p.dbPath)

	if !pathExists(p.dbPath) {
		// No db, check if we need to recover from backup
		if !pathExists(dbBackPath) {
			// no backup, no need to convert DB
			return nil
		}
		p.log.Info("Database backup found without primary database, indicating crash during conversion")
		// recover backup and keep going
		if err := os.Rename(dbBackPath, p.dbPath); err != nil {
			return err
		}
	}

	// DB already exists
	// Check if we need to clean up the backup, as previous backup cleanup may have failed.
	if pathExists(dbBackPath) {
		p.log.Info("Database backup found alongside primary database, indicating incomplete cleanup after conversion")
		if err := os.RemoveAll(dbBackPath); err != nil {
			return err
		}
	}

	var keyEncodingMarker string
	if markerData, err := os.ReadFile(filepath.Join(p.dbPath, markerFileName)); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		// Older versions were not setting the marker
		keyEncodingMarker = keyEncodingFormatOldCompareHierarchical
	} else {
		keyEncodingMarker = string(markerData)
	}

	if keyEncodingMarker == desiredEncoding.Name() {
		// Format is already correct, nothing to do
		return nil
	}

	switch keyEncodingMarker {
	case keyEncodingFormatOldCompareHierarchical:
		confOld := p.configForOldCompareHierarchical()
		confNew := p.configForNewerFormat()
		return p.convertDb(
			confOld, compare.EncoderNatural,
			confNew, desiredEncoding)

	case compare.EncoderNatural.Name(),
		compare.EncoderHierarchical.Name():
		confOld := p.configForNewerFormat()
		confNew := p.configForNewerFormat()
		oldEncoder, err := compare.GetEncoder(keyEncodingMarker)
		if err != nil {
			return err
		}
		return p.convertDb(
			confOld, oldEncoder,
			confNew, desiredEncoding)
	default:
		p.log.Warn("Found unknown encoding type. No conversion performed",
			slog.String("keyEncodingMarker", keyEncodingMarker))
		return nil
	}
}

func getKeyEncoder(dbPath string, keySorting proto.KeySortingType) (compare.Encoder, error) {
	switch keySorting {
	case proto.KeySortingType_NATURAL:
		return compare.EncoderNatural, nil
	case proto.KeySortingType_HIERARCHICAL:
		return compare.EncoderHierarchical, nil
	}

	// If the sorting is not specific, check if it was already
	// set in the db marker
	var keyEncodingMarker string
	markerData, err := os.ReadFile(filepath.Join(dbPath, markerFileName))
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		// Older versions were not setting the marker
		return compare.EncoderHierarchical, nil
	}

	keyEncodingMarker = string(markerData)

	if keyEncodingMarker == string(model.KeySortingNatural) {
		return compare.EncoderNatural, nil
	} else if keyEncodingMarker == string(model.KeySortingHierarchical) {
		return compare.EncoderHierarchical, nil
	}

	return nil, errors.Errorf("unknown encoding sort type: %s", keySorting)
}

func createMarker(dbPath string, newEncodingFormat string) error {
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(dbPath, markerFileName), []byte(newEncodingFormat), 0600)
}

func (p *pebbleDbConversion) convertDb(
	oldConfig *pebble.Options, oldEncoder compare.Encoder,
	newConfig *pebble.Options, newEncoder compare.Encoder) error {
	startTime := time.Now()

	oldDb, err := pebble.Open(p.dbPath, oldConfig)
	if err != nil {
		return errors.Wrap(err, "failed to open old database")
	}

	oldDbSizeMB := float64(oldDb.Metrics().DiskSpaceUsage()) / 1024 / 1024

	p.log.Info("Starting conversion of db",
		slog.String("path", p.dbPath),
		slog.String("oldEncodingFormat", oldEncoder.Name()),
		slog.String("newEncodingFormat", newEncoder.Name()),
		slog.Float64("size-mb", oldDbSizeMB))

	newDbPath := makeSwapTmpDbPath(p.dbPath, newEncoder)
	oldDbBackupPath := makeDbBackupPath(p.dbPath)
	if pathExists(newDbPath) {
		p.log.Info("Removing previous temp conversion db", slog.String("path", newDbPath))
		if err := os.RemoveAll(newDbPath); err != nil {
			return err
		}
	}

	if err := createMarker(newDbPath, newEncoder.Name()); err != nil {
		return err
	}

	newDb, err := pebble.Open(newDbPath, newConfig)
	if err != nil {
		return errors.Wrap(err, "failed to open new database")
	}

	if err := copyData(oldDb, oldEncoder, newDb, newEncoder); err != nil {
		return errors.Wrap(err, "failed to copy db data")
	}

	if err := oldDb.Close(); err != nil {
		return errors.Wrap(err, "failed to close old database")
	}

	if err := newDb.Close(); err != nil {
		return errors.Wrap(err, "failed to close new database")
	}

	if err := os.Rename(p.dbPath, oldDbBackupPath); err != nil {
		return errors.Wrap(err, "failed to backup old database")
	}

	if err = p.kvTrap.Trigger("convertCrashAfterMoveOldDb"); err != nil {
		return err
	}

	if err := os.Rename(newDbPath, p.dbPath); err != nil {
		return errors.Wrap(err, "failed to rename new database")
	}

	if err = p.kvTrap.Trigger("convertCrashAfterMoveNewDb"); err != nil {
		return err
	}

	// Delete the backup DB
	if err := os.RemoveAll(oldDbBackupPath); err != nil {
		return errors.Wrap(err, "failed to remove old database")
	}

	duration := time.Since(startTime)
	throughput := oldDbSizeMB / duration.Seconds()
	p.log.Info("Completed conversion of db",
		slog.String("path", p.dbPath),
		slog.Duration("elapsed-time-millis", duration),
		slog.Float64("throughput-mbps", throughput))
	return nil
}

const maxBatchCount = 1000

func copyData(from *pebble.DB, fromEncoder compare.Encoder,
	to *pebble.DB, toEncoder compare.Encoder) error {
	it, err := from.NewIter(&pebble.IterOptions{})
	if err != nil {
		return err
	}

	defer it.Close()

	it.First()

	wb := to.NewBatch()
	batchCount := 0
	for it.Valid() {
		key := fromEncoder.Decode(it.Key())
		value, err := it.ValueAndErr()
		if err != nil {
			return err
		}

		encodedKey := toEncoder.Encode(key)
		if err := wb.Set(encodedKey, value, pebble.NoSync); err != nil {
			return err
		}

		if batchCount++; batchCount >= maxBatchCount {
			if err := wb.Commit(pebble.NoSync); err != nil {
				return err
			}

			if err := wb.Close(); err != nil {
				return err
			}

			batchCount = 0
			wb = to.NewBatch()
		}

		it.Next()
	}

	// Close the last batch
	if batchCount > 0 {
		if err := wb.Commit(pebble.NoSync); err != nil {
			return err
		}
	}

	if err := wb.Close(); err != nil {
		return err
	}

	// Ensure everything is flushed to disk
	return to.Flush()
}
