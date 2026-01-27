package option

import (
	"fmt"

	"github.com/docker/go-units"
	"gopkg.in/yaml.v3"
)

type BytesSize uint64

func (b *BytesSize) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	v, err := units.RAMInBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size: %v", err)
	}
	*b = BytesSize(v)
	return nil
}

func (b BytesSize) MarshalYAML() (interface{}, error) {
	return units.BytesSize(float64(b)), nil
}
