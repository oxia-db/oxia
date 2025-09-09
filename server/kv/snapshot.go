package kv

import (
	"io"
)

type SnapshotOptions struct {
	IncludeContent bool
}

type SnapshotMetadata struct {
	Term            int64
	CommitOffset    int64
	CommitVersionId int64
}

type SnapshotChunk interface {
	Name() string

	Index() int32

	TotalCount() int32

	Content() []byte
}

type Snapshot interface {
	io.Closer

	BasePath() string

	Metadata() *SnapshotMetadata

	Valid() bool

	Chunk() (SnapshotChunk, error)

	Next() bool
}

type SnapshotLoader interface {
	io.Closer

	AddChunk(fileName string, chunkIndex int32, chunkCount int32, content []byte) error

	// Complete signals that the snapshot is now complete
	Complete()
}
