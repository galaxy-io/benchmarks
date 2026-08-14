package harness

import (
	"context"
	"fmt"
)

// DropNamespace removes the bench namespace and everything in it, returning a
// database to the state a fresh container starts in: postgres keeps its
// database and loses the schema, mysql loses and regains the database itself.
func DropNamespace(ctx context.Context, db *DB) error {
	conn, err := db.Open()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	var stmts []string
	switch db.Engine {
	case Postgres:
		stmts = []string{"DROP SCHEMA IF EXISTS " + Namespace + " CASCADE"}
	case MySQL:
		stmts = []string{
			"DROP DATABASE IF EXISTS " + Namespace,
			"CREATE DATABASE " + Namespace,
		}
	default:
		return fmt.Errorf("no namespace to drop for engine %s", db.Engine.Name())
	}
	for _, q := range stmts {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("drop namespace: %w", err)
		}
	}
	return nil
}
