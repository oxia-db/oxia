package manifest

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	commonio "github.com/oxia-db/oxia/common/io"
)

const filename = "MANIFEST"

var ErrInstanceIDMismatch = errors.New("instance id mismatch")

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

func (m *Manifest) doRecovery() error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			doc := &Document{}
			if err := commonio.WriteJSONToFile(m.path, doc); err != nil {
				return err
			}
			m.document.Store(doc)
			return nil
		}
		return err
	}

	doc := &Document{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, doc); err != nil {
			return err
		}
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

func (m *Manifest) SetInstanceID(id string) error {
	if id == "" {
		return errors.New("instance id must not be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.document.Load()
	switch {
	case current == nil || current.InstanceID == "":
		next := &Document{InstanceID: id}
		if err := commonio.WriteJSONToFile(m.path, next); err != nil {
			return err
		}
		m.document.Store(next)
		return nil
	case current.InstanceID == id:
		return nil
	default:
		return ErrInstanceIDMismatch
	}
}
