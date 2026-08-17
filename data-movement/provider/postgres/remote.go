package postgres

import (
	"context"
	"fmt"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// Remote addresses a postgres that outlives the run, such as an rds instance.
type Remote struct {
	DSN string
}

// Provision addresses the existing database. The suts reach it at the same
// address the host does, and teardown leaves it running.
func (r Remote) Provision(ctx context.Context, spec harness.ProvisionSpec) (*harness.DB, error) {
	if r.DSN == "" {
		return nil, fmt.Errorf("no dsn for the %s postgres", spec.Role)
	}
	db := &harness.DB{
		Engine:      spec.Engine,
		Role:        spec.Role,
		DSN:         r.DSN,
		InternalDSN: r.DSN,
		Close:       func(context.Context) error { return nil },
	}
	if spec.Reset {
		// Sinks reset every repetition. A reusable source resets once when its
		// seed is prepared, then remains intact for the rest of the cohort.
		if err := reset(ctx, db); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// reset drops the bench schema, leaving the database as a fresh container
// leaves it: POSTGRES_DB makes the database, nothing makes the schema. Creating
// it here instead would break the suts that create it themselves.
func reset(ctx context.Context, db *harness.DB) error {
	conn, err := db.Open()
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+harness.Namespace+" CASCADE")
	if err != nil {
		return fmt.Errorf("reset %s: %w", db.Role, err)
	}
	return nil
}
