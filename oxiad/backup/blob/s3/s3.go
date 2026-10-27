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

// Package s3 is the blob store on Amazon S3, and on the stores that speak its
// API. It registers the "s3://" scheme with package blob:
//
//	s3://<bucket>/<prefix>[?region=<region>&endpoint=<url>&path-style=true]
//
// The credentials come from the environment, as the AWS SDK resolves them
// (IAM roles, workload identity, the AWS_* variables): never from the URL.
package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"strings"

	"github.com/oxia-db/oxia/oxiad/common/errs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"

	"github.com/oxia-db/oxia/oxiad/backup/blob"
)

// Scheme is the URL scheme of the S3 destinations.
const Scheme = "s3"

// deleteBatchSize is the most keys one DeleteObjects call accepts.
const deleteBatchSize = 1000

func init() {
	blob.Register(Scheme, Open)
}

// API is the part of the S3 client that the store uses. Every object goes
// up in one PutObject, which streams its body from the seekable reader the
// store is given: an SST is far below the 5 GiB limit of a single put,
// nothing is buffered, and a put that fails leaves nothing behind, unlike a
// multipart upload.
type API interface {
	awss3.ListObjectsV2APIClient
	PutObject(ctx context.Context, params *awss3.PutObjectInput, optFns ...func(*awss3.Options)) (*awss3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *awss3.GetObjectInput, optFns ...func(*awss3.Options)) (*awss3.GetObjectOutput, error)
	DeleteObjects(ctx context.Context, params *awss3.DeleteObjectsInput, optFns ...func(*awss3.Options)) (*awss3.DeleteObjectsOutput, error)
}

// Store is a blob.Store on one bucket, under one key prefix.
type Store struct {
	api    API
	bucket string
	prefix string
}

// Open opens the store of a destination URL, see the package documentation.
func Open(ctx context.Context, destination *url.URL) (blob.Store, error) {
	if destination.Scheme != Scheme || destination.Host == "" {
		return nil, fmt.Errorf("s3: invalid destination %q: want s3://<bucket>/<prefix>", destination)
	}
	query := destination.Query()
	loadOptions := []func(*config.LoadOptions) error{
		config.WithLogger(sdkLogger{log: slog.With(slog.String("component", "s3-blob-store"))}),
	}
	if region := query.Get("region"); region != "" {
		loadOptions = append(loadOptions, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("s3: load configuration: %w", err)
	}
	api := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if endpoint := query.Get("endpoint"); endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = query.Get("path-style") == "true"
	})
	return New(api, destination.Host, destination.Path), nil
}

// New returns a store on the bucket, under the prefix, through the API.
func New(api API, bucket string, prefix string) *Store {
	prefix = strings.Trim(prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	return &Store{
		api:    api,
		bucket: bucket,
		prefix: prefix,
	}
}

func (s *Store) Put(ctx context.Context, key string, body io.ReadSeeker) error {
	// The SDK sizes the request from the seekable body, and rewinds it on a
	// retry
	_, err := s.api.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("s3: put %s: %w", key, err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.api.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.prefix + key),
	})
	if err != nil {
		var notFound *types.NoSuchKey
		if errors.As(err, &notFound) {
			return nil, errs.ErrNotFound
		}
		return nil, fmt.Errorf("s3: get %s: %w", key, err)
	}
	return out.Body, nil
}

func (s *Store) List(ctx context.Context, prefix string, visit func(blob.Object) error) error {
	return s.list(ctx, prefix, nil, func(object blob.Object) error { return visit(object) }, nil)
}

func (s *Store) ListDirs(ctx context.Context, prefix string) ([]string, error) {
	var dirs []string
	err := s.list(ctx, prefix, aws.String("/"), nil, func(dir string) {
		dirs = append(dirs, dir)
	})
	return dirs, err
}

// list pages through the objects under prefix, giving the objects to
// visitObject and, with a delimiter, the common prefixes to visitDir.
func (s *Store) list(ctx context.Context, prefix string, delimiter *string,
	visitObject func(blob.Object) error, visitDir func(string)) error {
	paginator := awss3.NewListObjectsV2Paginator(s.api, &awss3.ListObjectsV2Input{
		Bucket:    aws.String(s.bucket),
		Prefix:    aws.String(s.prefix + prefix),
		Delimiter: delimiter,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("s3: list %s: %w", prefix, err)
		}
		for _, object := range page.Contents {
			if visitObject == nil {
				continue
			}
			err := visitObject(blob.Object{
				Key:          strings.TrimPrefix(aws.ToString(object.Key), s.prefix),
				Size:         aws.ToInt64(object.Size),
				LastModified: aws.ToTime(object.LastModified),
			})
			if err != nil {
				return err
			}
		}
		for _, common := range page.CommonPrefixes {
			if visitDir != nil {
				visitDir(strings.TrimPrefix(aws.ToString(common.Prefix), s.prefix))
			}
		}
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, keys []string) error {
	for start := 0; start < len(keys); start += deleteBatchSize {
		batch := keys[start:min(start+deleteBatchSize, len(keys))]
		objects := make([]types.ObjectIdentifier, 0, len(batch))
		for _, key := range batch {
			objects = append(objects, types.ObjectIdentifier{Key: aws.String(s.prefix + key)})
		}
		out, err := s.api.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(s.bucket),
			Delete: &types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("s3: delete %d objects: %w", len(batch), err)
		}
		for _, failed := range out.Errors {
			// A missing object is deleted already
			if code := aws.ToString(failed.Code); code == "NoSuchKey" {
				continue
			}
			return fmt.Errorf("s3: delete %s: %s: %s", strings.TrimPrefix(aws.ToString(failed.Key), s.prefix),
				aws.ToString(failed.Code), aws.ToString(failed.Message))
		}
	}
	return nil
}

func (*Store) Close() error {
	return nil
}

// sdkLogger gives the messages of the AWS SDK to slog, at their level. The
// SDK writes them to stderr otherwise, whatever their level: e.g. a debug
// line for every object read from a store that keeps no checksum of its
// objects, since there is none to validate the response with.
type sdkLogger struct {
	log *slog.Logger
}

func (l sdkLogger) Logf(classification logging.Classification, format string, v ...any) {
	var level slog.Level
	switch classification {
	case logging.Debug:
		level = slog.LevelDebug
	case logging.Warn:
		level = slog.LevelWarn
	default:
		level = slog.LevelInfo
	}
	ctx := context.Background()
	if !l.log.Enabled(ctx, level) {
		return
	}
	l.log.Log(ctx, level, fmt.Sprintf(format, v...))
}
