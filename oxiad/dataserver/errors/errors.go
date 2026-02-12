package errors

import "errors"

var (
	ErrInvalidTerm          = errors.New("invalid term")
	ErrResourceNotAvailable = errors.New("resource not available")
	ErrResourceConflict     = errors.New("resource conflict")
	ErrInvalidStatus        = errors.New("invalid status")
)
