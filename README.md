<p align="center">
  <img src="docs/oxia-logo.svg" width="250"/>
</p>

<h2 align="center">Oxia</h1>
a robust, scalable metadata store and coordination system designed for large-scale distributed systems, with built-in support for stream index storage to optimize real-time data management.
<p align="center">
  <a href="https://oxia-db.github.io/docs/getting-started">Getting Started </a> | <a href="https://oxia-db.github.io/">Documentation</a>
</p>

<p align="center">
  <a href="https://github.com/oxia-db/oxia/releases"><img src="https://img.shields.io/github/v/release/oxia-db/oxia" alt="Latest Release"></a>
  <a href="https://github.com/oxia-db/oxia/actions/workflows/ci-build-test.yaml/badge.svg"><img src="https://github.com/oxia-db/oxia/actions/workflows/ci-build-test.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/oxia-db/oxia/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-white.svg" alt="License"></a>
  <a href="https://github.com/oxia-db/oxia/discussions/new/choose"><img src="https://img.shields.io/badge/Github-Discussion-blue.svg?logo=refinedgithub" alt="Github Discussion"></a>
</p>

<br><br><br>

### Why Oxia

<img src="docs/banner.svg" width="600"/>

<sub>Original image credited to  xkcd.com/2347, alterations by Qiang Zhao.</sub>

Oxia offers a sharding architecture designed to efficiently manage distributed metadata. In the world of cloud-native applications, scalability and high availability are essential. Traditional systems with a shard-nothing architecture are great for consistency but often face limitations when handling extremely large datasets or high-throughput scenarios. This is where Oxia shines.

With Oxia, you get a scalable, robust, and flexible solution for managing metadata in distributed systems, allowing you to unlock the potential of modern cloud-native architectures.

<br>

### Embedding Oxia

Besides running as dedicated `oxia server` / `oxia coordinator` processes, Oxia can be embedded as a library inside a Go application, so that each application node hosts an Oxia data server (and, on some nodes, the coordinator) in the same binary:

```go
import (
    "github.com/oxia-db/oxia/oxiad/dataserver"
    "github.com/oxia-db/oxia/oxiad/dataserver/option"
)

options := option.NewDefaultOptions()
options.Storage.Database.Dir = "./data/db"
options.Storage.WAL.Dir = "./data/wal"

server, err := dataserver.New(ctx, options)
```

These are the same entry points the `oxia` binary itself is built on. See the [`dataserver`](oxiad/dataserver/doc.go) package documentation for embedding a data server or a standalone single-node server, and the [`coordinator`](oxiad/coordinator/doc.go) package documentation for bootstrapping a whole cluster in-process with `coordinator.New`.

<br>

### Contributing to Oxia

Please 🌟 star the project if you like it. 

Feel free to open an [issue](https://github.com/oxia-db/oxia/issues/new) or start a [discussion](https://github.com/oxia-db/oxia/discussions/new/choose). You can also follow the development [guide]() to contribute and build on it.

### License

Copyright 2023-2026 The Oxia Authors

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
