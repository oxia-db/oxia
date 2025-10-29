package actions

import (
	"testing"

	"github.com/oxia-db/oxia/common/concurrent"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
