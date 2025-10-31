package wal

import "testing"

func NewTestWalFactory(t *testing.T) Factory {
	t.Helper()
	return NewWalFactory(&FactoryOptions{
		BaseWalDir:  t.TempDir(),
		SegmentSize: 128 * 1024,
	})
}
