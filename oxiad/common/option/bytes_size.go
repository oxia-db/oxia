package option

import (
	"fmt"

	"github.com/docker/go-units"
	"github.com/invopop/jsonschema"
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

// JSONSchema implements the jsonschema.ReflectType interface.
// Notice: We have no choice but to use the value receiver here.
func (BytesSize) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Title:       "Bytes Size",
		Description: "Bytes size string in human-readable format (e.g., \"1KiB\", \"100MiB\", \"2GiB\")",
		Pattern:     "^([0-9]+(\\.[0-9]+)?(B|KB|K|MB|M|GB|G|TB|T|PB|P|KiB|MiB|GiB|TiB|PiB))+$",
		Examples: []any{
			"1KiB",
			"100MiB",
			"2GiB",
		},
	}
}
