package harness

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	tc "github.com/testcontainers/testcontainers-go"
)

// Namespace is where every seeded and delivered table lives: a schema on
// postgres, a database on mysql.
const Namespace = "bench"

// TableDef is one table an engine can create and bulk load from a csv.
type TableDef struct {
	Name string
	DDL  string
	CSV  string
}

// Engine is one database engine: how to start it, address it, and bulk load into it.
type Engine interface {
	Name() string
	Start(ctx context.Context, net *tc.DockerNetwork, alias, runID string) (*DB, error)
	Open(db *DB) (*sql.DB, error)
	Load(ctx context.Context, db *DB, t TableDef) (int64, error)
}

// The two engines a route can name.
var (
	Postgres Engine = postgresEngine{}
	MySQL    Engine = mysqlEngine{}
)

// ParseRoute resolves a route like "pg-mysql" into its source and sink engines.
func ParseRoute(route string) (source, sink Engine, err error) {
	engines := map[string]Engine{"pg": Postgres, "mysql": MySQL}
	parts := strings.Split(route, "-")
	if len(parts) != 2 || engines[parts[0]] == nil || engines[parts[1]] == nil {
		return nil, nil, fmt.Errorf("route %q is not <engine>-<engine> over pg, mysql", route)
	}
	return engines[parts[0]], engines[parts[1]], nil
}
