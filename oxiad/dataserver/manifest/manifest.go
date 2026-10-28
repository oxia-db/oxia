// Copyright 2023-2026 The Oxia Authors
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

package manifest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/oxia-db/oxia/common/commonio"
)

const filename = "MANIFEST"

type Document struct {
	InstanceID string `json:"instance_id,omitempty"`
}

type Manifest struct {
	path string
	mu   sync.Mutex

	document atomic.Pointer[Document]
}

func NewManifest(dir string) (*Manifest, error) {
	m := &Manifest{
		path: filepath.Join(dir, filename),
	}
	if err := m.doRecovery(); err != nil {
		return nil, err
	}
	return m, nil
}

// ReadInstanceID returns the instance id recorded in the manifest in dir, without
// creating the manifest. It is empty when the data server isn't bound to a cluster yet.
func ReadInstanceID(dir string) (string, error) {
	doc := &Document{}
	if _, err := commonio.ReadJSONFromFile(filepath.Join(dir, filename), doc); err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return doc.InstanceID, nil
}

func (m *Manifest) doRecovery() error {
	doc := &Document{}
	_, err := commonio.ReadJSONFromFile(m.path, doc)
	if err != nil {
		if os.IsNotExist(err) {
			if err := commonio.WriteJSONToFile(m.path, doc); err != nil {
				return err
			}
			m.document.Store(doc)
			return nil
		}
		return err
	}

	m.document.Store(doc)
	return nil
}

func (m *Manifest) GetInstanceID() string {
	doc := m.document.Load()
	if doc == nil || doc.InstanceID == "" {
		return ""
	}
	return doc.InstanceID
}

// SetInstanceID binds the data server to the instance id, and reports whether
// this call bound it: binding the bound id again does nothing, and another id
// fails with errs.ErrFailedPrecondition.
func (m *Manifest) SetInstanceID(id string) (bound bool, err error) {
	if id == "" {
		return false, errors.New("instance id must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.document.Load()
	switch {
	case current == nil || current.InstanceID == "":
		next := &Document{InstanceID: id}
		if err := commonio.WriteJSONToFile(m.path, next); err != nil {
			return false, err
		}
		m.document.Store(next)
		return true, nil
	case current.InstanceID == id:
		return false, nil
	default:
		return false, fmt.Errorf("%w: instance id %q, bound to %q", errs.ErrFailedPrecondition, id, current.InstanceID)
	}
}
