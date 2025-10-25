package controllers

import (
	"sync"

	"github.com/oxia-db/oxia/coordinator/model"
)

type Metadata struct {
	sync.RWMutex
	shardMetadata model.ShardMetadata
}

func (s *Metadata) Load() model.ShardMetadata {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Clone()
}

func (s *Metadata) Compute(fn func(metadata *model.ShardMetadata)) model.ShardMetadata {
	s.Lock()
	defer s.Unlock()
	fn(&s.shardMetadata)
	return s.shardMetadata.Clone()
}

func (s *Metadata) Store(metadata model.ShardMetadata) {
	s.Lock()
	defer s.Unlock()
	s.shardMetadata = metadata
}

func (s *Metadata) Status() model.ShardStatus {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Status
}

func (s *Metadata) Leader() *model.Server {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Leader
}

func (s *Metadata) Term() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.shardMetadata.Term
}

func NewMetadata(metadata model.ShardMetadata) Metadata {
	return Metadata{
		RWMutex:       sync.RWMutex{},
		shardMetadata: metadata,
	}
}
