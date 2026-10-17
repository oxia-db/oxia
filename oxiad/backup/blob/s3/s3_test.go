// Copyright 2023-2026 The Oxia Authors
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

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
	"github.com/oxia-db/oxia/oxiad/backup/blob/blobtest"
)

// destinationEnv names the S3 destination that the conformance tests run
// against, e.g. "s3://bucket/prefix?endpoint=http://localhost:9000&path-style=true",
// with the credentials in the usual AWS_* variables.
const destinationEnv = "OXIA_TEST_S3_DESTINATION"

func TestStoreConformanceOnFakeAPI(t *testing.T) {
	blobtest.RunConformance(t, func(*testing.T) blob.Store {
		return New(newFakeAPI(), "bucket", "/base/prefix/")
	})
}

func TestStoreConformanceOnRealEndpoint(t *testing.T) {
	destination := os.Getenv(destinationEnv)
	if destination == "" {
		t.Skipf("%s is not set", destinationEnv)
	}
	blobtest.RunConformance(t, func(t *testing.T) blob.Store {
		t.Helper()
		u, err := url.Parse(destination)
		require.NoError(t, err)
		// Each conformance test wants an empty store
		u.Path = strings.TrimSuffix(u.Path, "/") + "/" + strings.ReplaceAll(t.Name(), "/", "-")
		store, err := blob.Open(t.Context(), u.String())
		require.NoError(t, err)
		createBucket(t, store.(*Store))
		t.Cleanup(func() {
			var keys []string
			assert.NoError(t, store.List(context.Background(), "", func(object blob.Object) error {
				keys = append(keys, object.Key)
				return nil
			}))
			assert.NoError(t, store.Delete(context.Background(), keys))
		})
		return store
	})
}

func TestStorePrefixesEveryKey(t *testing.T) {
	api := newFakeAPI()
	store := New(api, "bucket", "base")
	require.NoError(t, store.Put(t.Context(), "a/b", strings.NewReader("v")))
	assert.Equal(t, []string{"base/a/b"}, api.keys())
	dirs, err := store.ListDirs(t.Context(), "")
	require.NoError(t, err)
	assert.Equal(t, []string{"a/"}, dirs)
}

// A directory holding more keys than a page fits is still one entry of the
// listing: the fake must not repeat it on the next page, as S3 does not.
func TestFakeAPIListsADirectoryOnceAcrossPages(t *testing.T) {
	api := newFakeAPI()
	store := New(api, "bucket", "base")
	for i := range 1001 {
		require.NoError(t, store.Put(t.Context(), fmt.Sprintf("a/%04d", i), strings.NewReader("v")))
	}
	require.NoError(t, store.Put(t.Context(), "b/0", strings.NewReader("v")))
	dirs, err := store.ListDirs(t.Context(), "")
	require.NoError(t, err)
	assert.Equal(t, []string{"a/", "b/"}, dirs)
}

func TestOpenRejectsDestinationWithoutBucket(t *testing.T) {
	u, err := url.Parse("s3:///prefix")
	require.NoError(t, err)
	_, err = Open(t.Context(), u)
	assert.ErrorContains(t, err, "want s3://<bucket>/<prefix>")
}

func TestOpenAppliesEndpointAndPathStyle(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	u, err := url.Parse("s3://bucket/prefix?region=eu-west-1&endpoint=http://localhost:1&path-style=true")
	require.NoError(t, err)
	store, err := Open(t.Context(), u)
	require.NoError(t, err)
	api, ok := store.(*Store).api.(*awss3.Client)
	require.True(t, ok)
	options := api.Options()
	assert.Equal(t, "eu-west-1", options.Region)
	assert.Equal(t, "http://localhost:1", aws.ToString(options.BaseEndpoint))
	assert.True(t, options.UsePathStyle)
}

// createBucket creates the bucket of the store if it doesn't exist, for the
// local S3-compatible servers that the tests run against. An existing bucket
// is left alone: AWS rejects a CreateBucket without a location constraint
// outside us-east-1, before it reports that the bucket exists.
func createBucket(t *testing.T, store *Store) {
	t.Helper()
	api := store.api.(*awss3.Client)
	if _, err := api.HeadBucket(t.Context(), &awss3.HeadBucketInput{Bucket: aws.String(store.bucket)}); err == nil {
		return
	}
	_, err := api.CreateBucket(t.Context(), &awss3.CreateBucketInput{
		Bucket: aws.String(store.bucket),
	})
	var owned *types.BucketAlreadyOwnedByYou
	var exists *types.BucketAlreadyExists
	if err != nil && !errors.As(err, &owned) && !errors.As(err, &exists) {
		require.NoError(t, err)
	}
}

// fakeAPI is the S3 API on a map, with the listing and deletion limits of
// the real one.
type fakeAPI struct {
	mu      sync.Mutex
	objects map[string]fakeObject
}

type fakeObject struct {
	data     []byte
	modified time.Time
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{objects: map[string]fakeObject{}}
}

func (f *fakeAPI) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func (f *fakeAPI) PutObject(ctx context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[aws.ToString(params.Key)] = fakeObject{data: data, modified: time.Now()}
	return &awss3.PutObjectOutput{}, nil
}

func (*fakeAPI) UploadPart(context.Context, *awss3.UploadPartInput, ...func(*awss3.Options)) (*awss3.UploadPartOutput, error) {
	return nil, errors.New("multipart upload not supported by the fake")
}

func (*fakeAPI) CreateMultipartUpload(context.Context, *awss3.CreateMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CreateMultipartUploadOutput, error) {
	return nil, errors.New("multipart upload not supported by the fake")
}

func (*fakeAPI) CompleteMultipartUpload(context.Context, *awss3.CompleteMultipartUploadInput, ...func(*awss3.Options)) (*awss3.CompleteMultipartUploadOutput, error) {
	return nil, errors.New("multipart upload not supported by the fake")
}

func (*fakeAPI) AbortMultipartUpload(context.Context, *awss3.AbortMultipartUploadInput, ...func(*awss3.Options)) (*awss3.AbortMultipartUploadOutput, error) {
	return nil, errors.New("multipart upload not supported by the fake")
}

func (*fakeAPI) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return nil, errors.New("not supported by the fake")
}

func (f *fakeAPI) GetObject(ctx context.Context, params *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[aws.ToString(params.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &awss3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(object.data)),
		ContentLength: aws.Int64(int64(len(object.data))),
	}, nil
}

func (f *fakeAPI) ListObjectsV2(ctx context.Context, params *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := aws.ToString(params.Prefix)
	delimiter := aws.ToString(params.Delimiter)
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) && key > aws.ToString(params.ContinuationToken) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := &awss3.ListObjectsV2Output{}
	// A page holds up to pageSize entries, where a common prefix counts once
	// however many keys it groups, as on S3
	const pageSize = 1000
	count := 0
	for _, key := range keys {
		var dir string
		if delimiter != "" {
			rest := strings.TrimPrefix(key, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				dir = prefix + rest[:i+1]
			}
		}
		if n := len(out.CommonPrefixes); dir != "" && n > 0 && aws.ToString(out.CommonPrefixes[n-1].Prefix) == dir {
			// Another key of the last prefix: part of the same entry
			out.NextContinuationToken = aws.String(key)
			continue
		}
		if count == pageSize {
			out.IsTruncated = aws.Bool(true)
			break
		}
		count++
		out.NextContinuationToken = aws.String(key)
		if dir != "" {
			out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(dir)})
			continue
		}
		object := f.objects[key]
		out.Contents = append(out.Contents, types.Object{
			Key:          aws.String(key),
			Size:         aws.Int64(int64(len(object.data))),
			LastModified: aws.Time(object.modified),
		})
	}
	return out, nil
}

func (f *fakeAPI) DeleteObjects(ctx context.Context, params *awss3.DeleteObjectsInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(params.Delete.Objects) > deleteBatchSize {
		return nil, errors.New("too many keys")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, object := range params.Delete.Objects {
		delete(f.objects, aws.ToString(object.Key))
	}
	return &awss3.DeleteObjectsOutput{}, nil
}
