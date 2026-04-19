package document

import (
	"io"

	gproto "google.golang.org/protobuf/proto"

	metadatapb "github.com/oxia-db/oxia/common/proto/metadata"
	commonoption "github.com/oxia-db/oxia/oxiad/common/option"
)

type Backend interface {
	io.Closer

	LeaseWatch() *commonoption.Watch[metadatapb.LeaseState]
	LeaseRevalidate() error

	Load(name MetaRecordName) *Versioned[gproto.Message]
	Store(name MetaRecordName, record *Versioned[gproto.Message]) error
}
