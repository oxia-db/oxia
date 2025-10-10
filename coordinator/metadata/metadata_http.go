// Copyright 2023 StreamNative, Inc.
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

package metadata

import (
	"bytes"
	"encoding/json"
	"github.com/oxia-db/oxia/coordinator/model"
	"github.com/pkg/errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

var _ Provider = (*metadataHttpProvider)(nil)

type metadataHttpProvider struct {
	sync.Mutex
	metadataApiURL string
	client         *http.Client
}

func (m *metadataHttpProvider) Close() error {
	return nil
}

func (m *metadataHttpProvider) Get() (cs *model.ClusterStatus, version Version, err error) {
	m.Lock()
	defer m.Unlock()

	return m.getWithoutLock()
}

func (m *metadataHttpProvider) getWithoutLock() (*model.ClusterStatus, Version, error) {
	resp, err := m.client.Get(m.metadataApiURL)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", errors.New("Http request status is not OK")
	}
	body := resp.Body
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	bts, err := io.ReadAll(body)
	if err != nil {
		slog.Error("Read http response body failed", slog.Any("error", err))
		return nil, "", err
	}

	c := &Container{}
	err = json.Unmarshal(bts, c)
	if err != nil {
		slog.Error("Unmarshall json failed", slog.Any("error", err))
		return nil, "", err
	}

	return c.ClusterStatus, c.Version, nil
}

func (m *metadataHttpProvider) Store(cs *model.ClusterStatus, expectedVersion Version) (newVersion Version, err error) {
	m.Lock()
	m.Unlock()

	_, version, err := m.getWithoutLock()

	if err != nil {
		return version, err
	}

	if version != expectedVersion {
		slog.Error("Store metadata failed for version mismatch",
			slog.Any("local-version", version),
			slog.Any("expected-version", expectedVersion))
		panic(ErrMetadataBadVersion)
	}

	c := &Container{Version: expectedVersion, ClusterStatus: cs}
	bts, err := json.Marshal(c)
	if err != nil {
		slog.Error("Marshall failed", slog.Any("c", c), slog.Any("error", err))
		return "", err
	}

	req, err := http.NewRequest(http.MethodPost, m.metadataApiURL, bytes.NewBuffer(bts))
	if err != nil {
		slog.Error("Build http request failed", slog.Any("error", err))
		return "", err
	}

	resp, err := m.client.Do(req)
	if err != nil {
		slog.Error("Send request to remote server failed", slog.Any("error", err))
		return "", err
	}

	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		slog.Error("Http post response code is not OK")
		return "", errors.New("Http post response code is not OK")
	}

	bts, err = io.ReadAll(resp.Body)
	if err != nil {
		slog.Warn("Read http response body failed", slog.Any("error", err))
		return "", err
	}

	return Version(bts), nil
}

func NewMetadataProviderHttp(metadataApiURL string) Provider {
	return &metadataHttpProvider{
		metadataApiURL: metadataApiURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}
