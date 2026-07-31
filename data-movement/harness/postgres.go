package harness

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/moby/moby/api/types/container"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const PostgresImage = "postgres:16"

type postgresEngine struct{}

// Name identifies the engine.
func (postgresEngine) Name() string { return "postgres" }

// Start runs a postgres container on net; the alias doubles as the sampler role.
func (e postgresEngine) Start(ctx context.Context, net *tc.DockerNetwork, alias, runID string) (*DB, error) {
	req := tc.ContainerRequest{
		Image: PostgresImage,
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
		Labels:         map[string]string{LabelRun: runID, LabelRole: alias},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {alias}},
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.ShmSize = 1 << 30
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	host, port, err := hostPort(ctx, c, "5432")
	if err != nil {
		return nil, err
	}
	return &DB{
		Container:   c,
		Engine:      e,
		DSN:         fmt.Sprintf("postgresql://bench:bench@%s:%s/bench?sslmode=disable", host, port),
		InternalDSN: fmt.Sprintf("postgresql://bench:bench@%s:5432/bench?sslmode=disable", alias),
	}, nil
}

// Open opens a database/sql handle to db from the host.
func (postgresEngine) Open(db *DB) (*sql.DB, error) {
	return sql.Open("pgx", db.DSN)
}

// Load creates t in the bench schema and COPYs its csv in.
func (postgresEngine) Load(ctx context.Context, db *DB, t TableDef) (int64, error) {
	conn, err := pgx.Connect(ctx, db.DSN)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close(ctx) }()

	name := Namespace + "." + t.Name
	ddl := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s; DROP TABLE IF EXISTS %s; CREATE TABLE %s %s",
		Namespace, name, name, t.DDL)
	if _, err := conn.Exec(ctx, ddl); err != nil {
		return 0, fmt.Errorf("create %s: %w", t.Name, err)
	}
	f, err := os.Open(t.CSV)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()
	tag, err := conn.PgConn().CopyFrom(ctx, f, fmt.Sprintf("COPY %s FROM STDIN WITH (FORMAT csv)", name))
	if err != nil {
		return 0, fmt.Errorf("copy %s: %w", t.Name, err)
	}
	return tag.RowsAffected(), nil
}
