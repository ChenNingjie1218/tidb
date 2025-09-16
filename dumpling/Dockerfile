# syntax=docker/dockerfile:1.3
# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builder image
FROM hub.pingcap.net/keyspace/builder/golang:1.24-bullseye as builder

RUN apt install -y wget make git gcc && mkdir -p /go/src/github.com/pingcap/tidb

WORKDIR /go/src/github.com/pingcap/tidb

ENV GOMODCACHE=/go/cache

# Cache dependencies
COPY go.mod .
COPY go.sum .
COPY pkg/parser/go.mod pkg/parser/go.mod
COPY pkg/parser/go.sum pkg/parser/go.sum

ARG GITHUB_TOKEN
RUN if [ -n "$GITHUB_TOKEN" ]; then git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"; fi

ENV GOPRIVATE=github.com/tidbcloud

# Overwrite metrics.EnvRegisterMetricsAtInit when we build dumpling binary file.
ENV REGISTER_METRICS_INIT=false
ENV MIN_TIKV_VERSION=6.1.0

RUN --mount=type=cache,target=/go/cache \
    GO111MODULE=on go mod download

# Build real binaries
COPY . .
RUN --mount=type=cache,target=/go/cache \
    --mount=type=cache,target=/root/.cache/go-build,sharing=locked \
    make build_dumpling\
    && cp /go/src/github.com/pingcap/tidb/bin/dumpling /tidb-dumpling


# Executable image
FROM hub.pingcap.net/keyspace/builder/debian:bullseye-slim
RUN apt update && apt install -y bash curl netcat procps dumb-init && rm /bin/sh && ln -s /bin/bash /bin/sh && apt-get clean

COPY --from=builder /go/src/github.com/pingcap/tidb/bin/dumpling /tidb-dumpling

WORKDIR /

EXPOSE 4000

ENTRYPOINT ["/usr/bin/dumb-init", "/tidb-dumpling"]
