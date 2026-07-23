// Package native runs the naive baseline: the engine's own dump client piped
// into its load client, one single-threaded stream, schema and data, nothing
// clever. It is the floor every tool must beat and the planned reference for
// future index editions. Cross-engine routes have no native pipeline, so the
// baseline only runs same-engine.
package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const pgScript = `set -e
pg_dump --no-owner --no-privileges "$SOURCE_DSN" | psql -q -v ON_ERROR_STOP=1 "$SINK_DSN"`

const mysqlScript = `set -e
mysqldump --host "$SOURCE_HOST" --user "$DB_USER" --single-transaction "$DB_NAME" |
mysql --host "$SINK_HOST" --user "$DB_USER" "$DB_NAME"`

// Native runs the dump-and-load pipeline in the engine's own image.
type Native struct {
	route     string
	container tc.Container
}

// New returns the baseline for a route.
func New(route string) *Native { return &Native{route: route} }

// Name identifies the baseline.
func (n *Native) Name() string { return "native" }

// Image reports the image under test.
func (n *Native) Image() string {
	switch n.route {
	case "pg-pg":
		return harness.PostgresImage
	case "mysql-mysql":
		return harness.MySQLImage
	}
	panic(fmt.Sprintf("native does not run route %q", n.route))
}

// Config reports the pipeline shape.
func (n *Native) Config() map[string]any {
	switch n.route {
	case "pg-pg":
		return map[string]any{"pipeline": "pg_dump | psql"}
	case "mysql-mysql":
		return map[string]any{"pipeline": "mysqldump | mysql"}
	}
	panic(fmt.Sprintf("native does not run route %q", n.route))
}

// Routes lists the same-engine routes the baseline supports.
func (n *Native) Routes() []string { return []string{"pg-pg", "mysql-mysql"} }

// Setup creates the container, unstarted, so Run times only the pipeline.
func (n *Native) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	if len(tables) == 0 {
		return errors.New("no tables to copy")
	}
	var script string
	var cenv map[string]string
	switch n.route {
	case "pg-pg":
		script = pgScript
		cenv = map[string]string{
			"SOURCE_DSN": env.Source.InternalDSN,
			"SINK_DSN":   env.Sink.InternalDSN,
		}
	case "mysql-mysql":
		src, err := url.Parse(env.Source.InternalDSN)
		if err != nil {
			return err
		}
		dst, err := url.Parse(env.Sink.InternalDSN)
		if err != nil {
			return err
		}
		pass, _ := src.User.Password()
		script = mysqlScript
		cenv = map[string]string{
			"SOURCE_HOST": src.Hostname(),
			"SINK_HOST":   dst.Hostname(),
			"DB_USER":     src.User.Username(),
			"DB_NAME":     strings.TrimPrefix(src.Path, "/"),
			"MYSQL_PWD":   pass,
		}
	default:
		return fmt.Errorf("native does not run route %q", n.route)
	}
	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      n.Image(),
			Cmd:        []string{"sh", "-c", script},
			Env:        cenv,
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "native"},
			Networks:   []string{env.Net.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Minute),
		},
		Started: false,
	})
	if err != nil {
		return fmt.Errorf("native container: %w", err)
	}
	n.container = container
	return nil
}

// Teardown removes the container.
func (n *Native) Teardown(ctx context.Context) {
	if n.container != nil {
		_ = n.container.Terminate(ctx)
	}
}

// Run starts the pipeline and blocks until it exits, erroring on a non-zero code.
func (n *Native) Run(ctx context.Context) error {
	if err := n.container.Start(ctx); err != nil {
		return fmt.Errorf("native start: %w", err)
	}
	state, err := n.container.State(ctx)
	if err != nil {
		return err
	}
	if state.ExitCode != 0 {
		return fmt.Errorf("native exited %d:\n%s", state.ExitCode, n.logs(ctx))
	}
	return nil
}

func (n *Native) logs(ctx context.Context) string {
	r, err := n.container.Logs(ctx)
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
