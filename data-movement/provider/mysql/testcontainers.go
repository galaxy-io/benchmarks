// Package mysql provisions the benchmark's mysql database, either as a
// container or as one already running.
package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider/testcontainers"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Testcontainers runs mysql as a container on the run's network.
type Testcontainers struct{}

// Provision starts a mysql container; the role doubles as its network alias.
// The bench user gets global grants so tools can create their own working
// databases, and local_infile is on for LOAD DATA seeding.
func (Testcontainers) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	grants := "GRANT ALL PRIVILEGES ON *.* TO 'bench'@'%'; FLUSH PRIVILEGES;\n"
	req := tc.ContainerRequest{
		Image: harness.MySQLImage,
		// Use an 8 GiB buffer pool and 2 GiB redo log for local benchmark runs.
		Cmd:          []string{"--local-infile=ON", "--innodb-buffer-pool-size=8G", "--innodb-redo-log-capacity=2G"},
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "bench",
			"MYSQL_USER":          "bench",
			"MYSQL_PASSWORD":      "bench",
			"MYSQL_DATABASE":      "bench",
		},
		Files: []tc.ContainerFile{{
			Reader:            strings.NewReader(grants),
			ContainerFilePath: "/docker-entrypoint-initdb.d/grants.sql",
			FileMode:          0o644,
		}},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").
			WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.Start(ctx, spec, spec.Role, req)
	if err != nil {
		return nil, err
	}
	host, port, err := testcontainers.HostPort(ctx, c, "3306")
	if err != nil {
		_ = c.Terminate(ctx)
		return nil, err
	}
	return &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         fmt.Sprintf("mysql://bench:bench@%s:%s/bench", host, port),
		InternalDSN: fmt.Sprintf("mysql://bench:bench@%s:3306/bench", spec.Role),
		Close:       testcontainers.Terminate(c),
	}, nil
}
