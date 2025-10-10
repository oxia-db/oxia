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
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/oxia-db/oxia/coordinator/model"
)

var (
	_fake = func() *fake.Clientset {
		f := fake.NewSimpleClientset()
		f.PrependReactor("*", "*", K8SResourceVersionSupport(f.Tracker()))
		return f
	}()
	metadataProviders = map[string]func(t *testing.T) Provider{
		"memory": func(t *testing.T) Provider {
			t.Helper()

			return NewMetadataProviderMemory()
		},
		"file": func(t *testing.T) Provider {
			t.Helper()

			return NewMetadataProviderFile(filepath.Join(t.TempDir(), "metadata"))
		},
		"configmap": func(t *testing.T) Provider {
			t.Helper()

			return NewMetadataProviderConfigMap(_fake, "ns", "n")
		},
		"http": func(t *testing.T) Provider {
			t.Helper()
			return NewMetadataProviderHttp("http://localhost:9191/api/oxia/metadata")
		},
	}
)

func TestMetadataProvider(t *testing.T) {
	inMem := NewMetadataProviderMemory()
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		uri := request.RequestURI
		if len(uri) <= 0 || uri != "/api/oxia/metadata" {
			_, _ = writer.Write([]byte("hello"))
			return
		}
		method := request.Method
		if method == http.MethodGet {
			cs, v, _ := inMem.Get()
			c := &Container{Version: v, ClusterStatus: cs}
			bts, _ := json.Marshal(c)
			_, _ = writer.Write(bts)
		} else if method == http.MethodPost {
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(request.Body)
			bts, err := io.ReadAll(request.Body)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			c := &Container{}
			err = json.Unmarshal(bts, c)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}

			v, _ := inMem.Store(c.ClusterStatus, c.Version)
			_, _ = writer.Write([]byte(v))
		}
	})

	httpserver := http.Server{
		Addr:    ":9191",
		Handler: handler,
	}

	go func() {
		_ = httpserver.ListenAndServe()
	}()

	defer func(httpserver *http.Server) {
		_ = httpserver.Close()
	}(&httpserver)

	for name, provider := range metadataProviders {
		t.Run(name, func(t *testing.T) {
			m := provider(t)

			res, version, err := m.Get()
			assert.NoError(t, err)
			assert.Equal(t, NotExists, version)
			assert.Nil(t, res)

			assert.PanicsWithError(t, ErrMetadataBadVersion.Error(), func() {
				_, err := m.Store(&model.ClusterStatus{
					Namespaces: map[string]model.NamespaceStatus{},
				}, "")
				assert.NoError(t, err)
			})

			newVersion, err := m.Store(&model.ClusterStatus{
				Namespaces: map[string]model.NamespaceStatus{},
			}, NotExists)
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), newVersion)

			res, version, err = m.Get()
			assert.NoError(t, err)
			assert.EqualValues(t, Version("0"), version)
			assert.Equal(t, &model.ClusterStatus{
				Namespaces: map[string]model.NamespaceStatus{},
			}, res)

			assert.NoError(t, m.Close())
		})
	}
}
