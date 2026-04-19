package metadata_v2

import (
	"context"
	"fmt"

	documentfile "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend/file"
	documentkubernetes "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend/kubernetes"
	documentraft "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/backend/raft"
	"github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/document"
	metadataerr "github.com/oxia-db/oxia/oxiad/coordinator/metadata_v2/error"
	"github.com/oxia-db/oxia/oxiad/coordinator/option"
)

const (
	MetadataProviderNameConfigmap = "configmap"
	MetadataProviderNameRaft      = "raft"
	MetadataProviderNameFile      = "file"
)

func NewMetadata(ctx context.Context, options option.MetadataOptions) Store {
	switch options.ProviderName {
	case MetadataProviderNameFile:
		return document.NewStore(ctx, documentfile.NewBackend(ctx, options.File))
	case MetadataProviderNameConfigmap:
		return document.NewStore(ctx, documentkubernetes.NewBackend(ctx, options.Kubernetes))
	case MetadataProviderNameRaft:
		return document.NewStore(ctx, documentraft.NewBackend(ctx, options.Raft))
	default:
		panic(fmt.Errorf("%w: unsupported metadata provider %q", metadataerr.ErrInvalidInput, options.ProviderName))
	}
}
