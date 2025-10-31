package protobuf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protojson"
	pb "google.golang.org/protobuf/proto"
)

func AssertProtoEqual(t *testing.T, expected, actual pb.Message) {
	t.Helper()

	if !pb.Equal(expected, actual) {
		protoMarshal := protojson.MarshalOptions{
			EmitUnpopulated: true,
		}
		expectedJSON, _ := protoMarshal.Marshal(expected)
		actualJSON, _ := protoMarshal.Marshal(actual)
		assert.Equal(t, string(expectedJSON), string(actualJSON))
	}
}
