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

package oxia

import "github.com/oxia-db/oxia/proto"

func toPutResult(originalKey string, r *proto.PutResponse) PutResult {
	if err := toError(r.Status); err != nil {
		return PutResult{
			Err: err,
		}
	}
	pr := PutResult{
		Version: toVersion(r.Version),
	}

	if r.Key != nil {
		pr.Key = r.GetKey()
	} else {
		pr.Key = originalKey
	}

	return pr
}

func toDeleteResult(r *proto.DeleteResponse) error {
	return toError(r.Status)
}

func toDeleteRangeResult(r *proto.DeleteRangeResponse) error {
	return toError(r.Status)
}

func toGetResult(r *proto.GetResponse, originalKey string, err error) GetResult {
	if err != nil {
		return GetResult{Err: err}
	}

	if err := toError(r.Status); err != nil {
		return GetResult{
			Err: err,
		}
	}
	gr := GetResult{
		Value:   r.Value,
		Version: toVersion(r.Version),
	}

	if r.Key != nil {
		gr.Key = r.GetKey()
	} else {
		gr.Key = originalKey
	}

	return gr
}

func toVersion(version *proto.Version) Version {
	v := Version{
		VersionId:          version.VersionId,
		ModificationsCount: version.ModificationsCount,
		CreatedTimestamp:   version.CreatedTimestamp,
		ModifiedTimestamp:  version.ModifiedTimestamp,
		Ephemeral:          version.SessionId != nil,
	}
	if version.ClientIdentity != nil {
		v.ClientIdentity = *version.ClientIdentity
	}
	if version.SessionId != nil {
		v.SessionId = *version.SessionId
	}

	return v
}

func toError(status proto.Status) error {
	switch status {
	case proto.Status_OK:
		return nil
	case proto.Status_UNEXPECTED_VERSION_ID:
		return ErrUnexpectedVersionId
	case proto.Status_KEY_NOT_FOUND:
		return ErrKeyNotFound
	default:
		return ErrUnknownStatus
	}
}

func toSecondaryIndexes(secondaryIndexes []*secondaryIdxOption) (res []*proto.SecondaryIndex) {
	for _, si := range secondaryIndexes {
		res = append(res, &proto.SecondaryIndex{
			IndexName:    si.indexName,
			SecondaryKey: si.secondaryKey,
		})
	}

	return res
}
