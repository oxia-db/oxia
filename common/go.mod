module github.com/oxia-db/oxia/common

go 1.25

require (
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/coreos/go-oidc/v3 v3.17.0
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/oxia-db/oxia/oxia v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10
	github.com/prometheus/client_golang v1.22.0
	github.com/rs/zerolog v1.34.0
	github.com/samber/slog-zerolog/v2 v2.7.3
	github.com/stretchr/testify v1.10.0
	github.com/zeebo/xxh3 v1.0.2
	go.opentelemetry.io/otel v1.35.0
	go.opentelemetry.io/otel/exporters/prometheus v0.57.0
	go.opentelemetry.io/otel/metric v1.35.0
	go.opentelemetry.io/otel/sdk/metric v1.35.0
	golang.org/x/exp v0.0.0-20250506013437-ce4c2cf36ca6
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/go-jose/go-jose/v4 v4.1.3 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.63.0 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/samber/lo v1.50.0 // indirect
	github.com/samber/slog-common v0.18.1 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/oauth2 v0.33.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/oxia-db/oxia/common => ../common
	github.com/oxia-db/oxia/oxia => ../oxia
)
