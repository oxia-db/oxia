package constant

import "github.com/oxia-db/oxia/proto"

const InvalidTerm int64 = -1
const InvalidOffset int64 = -1

var InvalidEntryId = &proto.EntryId{
	Term:   InvalidTerm,
	Offset: InvalidOffset,
}
