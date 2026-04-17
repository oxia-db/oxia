package metadata_v2

import (
	"bytes"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	gproto "google.golang.org/protobuf/proto"
)

func ReadProtoJSONFile(path string, msg gproto.Message) error {
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if len(bytes.TrimSpace(content)) == 0 {
		return nil
	}

	return protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}.Unmarshal(content, msg)
}

func WriteProtoJSONFile(path string, msg gproto.Message) error {
	content, err := protojson.MarshalOptions{
		Indent:    "  ",
		Multiline: true,
	}.Marshal(msg)
	if err != nil {
		return err
	}
	content = append(content, '\n')

	dir := filepath.Dir(path)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}

	tmpName := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpName)
	}()

	if _, err := tmpFile.Write(content); err != nil {
		return err
	}
	if err := tmpFile.Chmod(0o600); err != nil {
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return nil
}
