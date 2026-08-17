// Package provider selects how a route's databases are provisioned.
package provider

import (
	"fmt"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider/iceberg"
	"github.com/galaxy-io/benchmarks/data-movement/provider/mysql"
	"github.com/galaxy-io/benchmarks/data-movement/provider/postgres"
)

// Docker provisions the engine as a container on the run's network.
func Docker(engine harness.Engine) (harness.Provider, error) {
	switch engine {
	case harness.Postgres:
		return postgres.Testcontainers{}, nil
	case harness.MySQL:
		return mysql.Testcontainers{}, nil
	case harness.Iceberg:
		return iceberg.Testcontainers{}, nil
	}
	return nil, fmt.Errorf("no container provider for engine %q", engine.Name())
}

// Remote addresses an engine already running at dsn.
func Remote(engine harness.Engine, dsn string) (harness.Provider, error) {
	switch engine {
	case harness.Postgres:
		return postgres.Remote{DSN: dsn}, nil
	case harness.MySQL:
		return mysql.Remote{DSN: dsn}, nil
	case harness.Iceberg:
		remote, err := iceberg.RemoteFromEnv(dsn)
		if err != nil {
			return nil, err
		}
		return remote, nil
	}
	return nil, fmt.Errorf("no remote provider for engine %q", engine.Name())
}
