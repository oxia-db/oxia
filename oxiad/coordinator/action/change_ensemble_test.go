// Copyright 2023-2025 The Oxia Authors
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
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/oxia-db/oxia/oxiad/coordinator/model"

	"github.com/oxia-db/oxia/common/concurrent"
)

func TestChangeEnsembleAction_WithCallback(t *testing.T) {
	finished := false
	var finishedErr error

	// success
	action := NewChangeEnsembleActionWithCallback(0, model.Server{}, model.Server{}, concurrent.NewOnce(func(t any) {
		finished = true
	}, func(err error) {
		finished = true
		finishedErr = err
	}))
	action.Done(nil)
	assert.True(t, finished)
	assert.Nil(t, finishedErr)

	// failure
	action = NewChangeEnsembleActionWithCallback(0, model.Server{}, model.Server{}, concurrent.NewOnce(func(t any) {
		finished = true
	}, func(err error) {
		finished = true
		finishedErr = err
	}))
	action.Error(errors.New("error"))
	assert.True(t, finished)
	assert.Error(t, finishedErr)
}

func TestChangeEnsembleAction_WithoutCallback(t *testing.T) {
	// success
	action := NewChangeEnsembleAction(0, model.Server{}, model.Server{})
	go func() {
		action.Done(nil)
	}()
	_, err := action.Wait()
	assert.Nil(t, err)

	// failure
	action = NewChangeEnsembleAction(0, model.Server{}, model.Server{})
	go func() {
		action.Error(errors.New("error"))
	}()
	_, err = action.Wait()
	assert.Error(t, err)
}
