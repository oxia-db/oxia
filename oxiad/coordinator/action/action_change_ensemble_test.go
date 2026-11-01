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

package action

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/common/proto"
)

func TestChangeEnsembleActionWait(t *testing.T) {
	// success
	action := NewChangeEnsembleAction(7, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})
	assert.Equal(t, int64(7), action.GetShard())
	go func() {
		action.Done(nil)
	}()
	_, err := action.Wait(context.Background())
	assert.Nil(t, err)

	// failure
	action = NewChangeEnsembleAction(0, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})
	go func() {
		action.Error(errors.New("error"))
	}()
	_, err = action.Wait(context.Background())
	assert.Error(t, err)
}

func TestChangeEnsembleActionDoneIgnoresLaterCompletions(t *testing.T) {
	action := NewChangeEnsembleAction(0, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})
	action.Done(nil)
	action.Done(nil)
	action.Error(errors.New("error"))

	_, err := action.Wait(context.Background())
	assert.NoError(t, err)
}

func TestChangeEnsembleActionErrorIgnoresLaterCompletions(t *testing.T) {
	action := NewChangeEnsembleAction(0, &proto.DataServerIdentity{}, &proto.DataServerIdentity{})
	first := errors.New("first")
	action.Error(first)
	action.Error(errors.New("second"))
	action.Done(nil)

	_, err := action.Wait(context.Background())
	assert.ErrorIs(t, err, first)
}
