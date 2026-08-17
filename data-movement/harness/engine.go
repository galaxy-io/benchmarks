package harness

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Namespace is where every seeded and delivered table lives: a Postgres schema,
// a MySQL database, or an Iceberg namespace.
const Namespace = "bench"

// TableDef is one table an engine can create and bulk load from a csv.
type TableDef struct {
	Name string
	DDL  string
	CSV  string
}

// Engine defines how to address a database, bulk-load it, and count rows for
// validation. It is independent of the database provider.
type Engine interface {
	Name() string
	Open(db *DB) (*sql.DB, error)
	Load(ctx context.Context, db *DB, t TableDef) (int64, error)
	Count(ctx context.Context, db *DB, table string) (int64, error)
}

// The engines a route can name; Iceberg is sink-only.
var (
	Postgres Engine = postgresEngine{}
	MySQL    Engine = mysqlEngine{}
	Iceberg  Engine = icebergEngine{}
)

// ParseRoute resolves a route like "pg-mysql" into its source and sink engines.
func ParseRoute(route string) (source, sink Engine, err error) {
	engines := map[string]Engine{"pg": Postgres, "mysql": MySQL, "iceberg": Iceberg}
	parts := strings.Split(route, "-")
	if len(parts) != 2 || engines[parts[0]] == nil || engines[parts[1]] == nil {
		return nil, nil, fmt.Errorf("route %q is not <engine>-<engine> over pg, mysql, iceberg", route)
	}
	if engines[parts[0]] == Iceberg {
		return nil, nil, fmt.Errorf("route %q: iceberg is sink-only", route)
	}
	return engines[parts[0]], engines[parts[1]], nil
}

// sqlCount counts one bench table through an engine's database/sql handle.
func sqlCount(ctx context.Context, db *DB, table string) (int64, error) {
	h, err := db.Open()
	if err != nil {
		return 0, err
	}
	defer func() { _ = h.Close() }()
	var n int64
	err = h.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", Namespace, table)).Scan(&n)
	return n, err
}
