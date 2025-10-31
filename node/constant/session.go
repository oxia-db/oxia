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
