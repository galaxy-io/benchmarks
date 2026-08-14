package mysql

import (
	"context"
	"fmt"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// Remote addresses a mysql that outlives the run, such as an rds instance.
type Remote struct {
	DSN string
}

// Provision addresses the existing database. The suts reach it at the same
// address the host does, and teardown leaves it running.
func (r Remote) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	if r.DSN == "" {
		return nil, fmt.Errorf("no dsn for the %s mysql", spec.Role)
	}
	db := &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         r.DSN,
		InternalDSN: r.DSN,
		Close:       func(context.Context) error { return nil },
	}
	// Match the fresh-container lifecycle for both persistent roles.
	if err := reset(ctx, db); err != nil {
		return nil, err
	}
	return db, nil
}

// reset empties the bench database.
func reset(ctx context.Context, db *harness.DB) error {
	conn, err := db.Open()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	for _, q := range []string{
		"DROP DATABASE IF EXISTS " + harness.Namespace,
		"CREATE DATABASE " + harness.Namespace,
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("reset %s: %w", db.Role, err)
		}
	}
	return nil
}
