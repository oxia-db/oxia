package error

import "errors"

var (
	ErrAlreadyExists = errors.New("already exists")
	ErrInvalidInput  = errors.New("invalid input")
	ErrLeaseNotHeld  = errors.New("lease not held")
	ErrNotFound      = errors.New("not found")
)
