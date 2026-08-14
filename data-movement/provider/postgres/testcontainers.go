// Package postgres provisions the benchmark's postgres database, either as a
// container or as one already running.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider/testcontainers"
	"github.com/moby/moby/api/types/container"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Testcontainers runs postgres as a container on the run's network.
type Testcontainers struct{}

// Provision starts a postgres container; the role doubles as its network alias.
func (Testcontainers) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	req := tc.ContainerRequest{
		Image: harness.PostgresImage,
		// Sized for the benchmark machine; durability settings stay stock.
		Cmd: []string{
			"-c", "shared_buffers=8GB",
			"-c", "max_wal_size=8GB",
			"-c", "checkpoint_completion_target=0.9",
			"-c", "work_mem=64MB",
			"-c", "maintenance_work_mem=1GB",
		},
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "bench",
			"POSTGRES_PASSWORD": "bench",
			"POSTGRES_DB":       "bench",
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ShmSize = 1 << 30
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.Start(ctx, spec, spec.Role, req)
	if err != nil {
		return nil, err
	}
	host, port, err := testcontainers.HostPort(ctx, c, "5432")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         fmt.Sprintf("postgresql://bench:bench@%s:%s/bench?sslmode=disable", host, port),
		InternalDSN: fmt.Sprintf("postgresql://bench:bench@%s:5432/bench?sslmode=disable", spec.Role),
		Close:       testcontainers.Terminate(c),
	}, nil
}
