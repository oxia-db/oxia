package io

import "io"

var _ io.Closer = &NopCloser{}

type NopCloser struct {
}

func (n NopCloser) Close() error {
	return nil
}
