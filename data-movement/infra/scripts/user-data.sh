#!/usr/bin/env bash

# Installs the benchmark toolchain: docker, go, duckdb.
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y git curl unzip

# Docker
curl -fsSL https://get.docker.com | sh
usermod -aG docker ubuntu

# Go (version matches go.mod)
GO_VERSION=1.26.4
curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" | tar -C /usr/local -xz
ln -sf /usr/local/go/bin/go /usr/local/bin/go

# duckdb
curl -fsSL https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip
unzip -o /tmp/duckdb.zip -d /usr/local/bin
chmod +x /usr/local/bin/duckdb
