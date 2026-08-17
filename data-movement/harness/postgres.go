package harness

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const PostgresImage = "postgres:16"

type postgresEngine struct{}

// Name identifies the engine.
func (postgresEngine) Name() string { return "postgres" }

// Open opens a database/sql handle to db from the host.
func (postgresEngine) Open(db *DB) (*sql.DB, error) {
	return sql.Open("pgx", db.DSN)
}

// Count counts one bench table.
func (postgresEngine) Count(ctx context.Context, db *DB, table string) (int64, error) {
	return sqlCount(ctx, db, table)
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
	// A plain COPY leaves pages unhinted and not all-visible: the first reader
	// dirties the whole table and insert autovacuum competes with it. Freeze
	// once here so no timed window pays for it.
	if _, err := conn.Exec(ctx, "VACUUM (FREEZE, ANALYZE) "+name); err != nil {
		return 0, fmt.Errorf("vacuum %s: %w", t.Name, err)
	}
	return tag.RowsAffected(), nil
}
