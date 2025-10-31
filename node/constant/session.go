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

package constant

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	SessionKeyPrefix  = InternalKeyPrefix + "session"
	SessionKeyFormat  = SessionKeyPrefix + "/%016x"
	SessionKeyLength  = len(SessionKeyPrefix) + 1 + 16
	MaxSessionTimeout = 5 * time.Minute
	MinSessionTimeout = 2 * time.Second
)

type SessionId int64

func SessionKey(sessionId SessionId) string {
	return fmt.Sprintf("%s/%016x", SessionKeyPrefix, sessionId)
}

func ShadowKey(sessionId SessionId, key string) string {
	return fmt.Sprintf("%s/%016x/%s", SessionKeyPrefix, sessionId, url.PathEscape(key))
}

func KeyToId(key string) (SessionId, error) {
	var id int64
	items, err := fmt.Sscanf(key, SessionKeyFormat, &id)
	if err != nil {
		return 0, err
	}

	if items != 1 {
		return 0, errors.New("failed to parse session key: " + key)
	}

	return SessionId(id), nil
}

func IsSessionKey(key string) bool {
	if !strings.HasPrefix(key, SessionKeyPrefix) {
		return false
	}
	if len(key) != SessionKeyLength {
		return false
	}
	return true
}
