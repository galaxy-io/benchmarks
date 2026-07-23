// Package copy runs the naive baseline: pg_dump piped into psql, one
// single-threaded stream, schema and data, nothing clever. It is the floor
// every tool must beat and the planned reference for future index editions.
package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = harness.PostgresImage

const script = `set -e
pg_dump --no-owner --no-privileges "$SOURCE_DSN" | psql -q -v ON_ERROR_STOP=1 "$SINK_DSN"`

// Copy runs the pg_dump | psql pipeline in a postgres image.
type Copy struct {
	container tc.Container
}

// New returns the baseline.
func New() *Copy { return &Copy{} }

// Name identifies the baseline.
func (c *Copy) Name() string { return "copy" }

// Image reports the image under test.
func (c *Copy) Image() string { return Image }

// Config reports the pipeline shape.
func (c *Copy) Config() map[string]any { return map[string]any{"pipeline": "pg_dump | psql"} }

// Setup creates the container, unstarted, so Run times only the pipeline.
func (c *Copy) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	if len(tables) == 0 {
		return errors.New("no tables to copy")
	}
	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image: Image,
			Cmd:   []string{"sh", "-c", script},
			Env: map[string]string{
				"SOURCE_DSN": env.Source.InternalDSN,
				"SINK_DSN":   env.Sink.InternalDSN,
			},
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "copy"},
			Networks:   []string{env.Net.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Minute),
		},
		Started: false,
	})
	if err != nil {
		return fmt.Errorf("copy container: %w", err)
	}
	c.container = container
	return nil
}

// Teardown removes the container.
func (c *Copy) Teardown(ctx context.Context) {
	if c.container != nil {
		_ = c.container.Terminate(ctx)
	}
}

// Run starts the pipeline and blocks until it exits, erroring on a non-zero code.
func (c *Copy) Run(ctx context.Context) error {
	if err := c.container.Start(ctx); err != nil {
		return fmt.Errorf("copy start: %w", err)
	}
	state, err := c.container.State(ctx)
	if err != nil {
		return err
	}
	if state.ExitCode != 0 {
		return fmt.Errorf("copy exited %d:\n%s", state.ExitCode, c.logs(ctx))
	}
	return nil
}

func (c *Copy) logs(ctx context.Context) string {
	r, err := c.container.Logs(ctx)
	if err != nil {
		return "(logs unavailable)"
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		return "(logs unavailable)"
	}
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}
