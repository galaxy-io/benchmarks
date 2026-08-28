// Package native runs each engine's dump client piped into its load client.
// The pipeline uses one stream for schema and data and supports same-engine
// routes only.
package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const pgScript = `set -e
pg_dump --schema=bench --no-owner --no-privileges --no-publications --no-subscriptions "$SOURCE_DSN" |
psql -q -v ON_ERROR_STOP=1 "$SINK_DSN"`

const mysqlScript = `set -e
MYSQL_PWD="$SOURCE_PASSWORD" mysqldump --host "$SOURCE_HOST" --port "$SOURCE_PORT" --user "$SOURCE_USER" $SOURCE_SSL_ARG --single-transaction "$SOURCE_DB" |
MYSQL_PWD="$SINK_PASSWORD" mysql --host "$SINK_HOST" --port "$SINK_PORT" --user "$SINK_USER" $SINK_SSL_ARG "$SINK_DB"`

// Native runs the dump-and-load pipeline in the engine's own image.
type Native struct {
	route     string
	container tc.Container
	script    string
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
		return map[string]any{
			"pipeline":      "pg_dump | psql",
			"schema":        harness.Namespace,
			"publications":  false,
			"subscriptions": false,
		}
	case "mysql-mysql":
		return map[string]any{"pipeline": "mysqldump | mysql"}
	}
	panic(fmt.Sprintf("native does not run route %q", n.route))
}

// Routes lists the same-engine routes the baseline supports.
func (n *Native) Routes() []string { return []string{"pg-pg", "mysql-mysql"} }

// Setup starts an idle container and prepares the pipeline command. Run starts
// the command inside that container.
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
		srcPass, _ := src.User.Password()
		dstPass, _ := dst.User.Password()
		script = mysqlScript
		cenv = map[string]string{
			"SOURCE_HOST":     src.Hostname(),
			"SOURCE_PORT":     portOr(src, "3306"),
			"SOURCE_USER":     src.User.Username(),
			"SOURCE_PASSWORD": srcPass,
			"SOURCE_DB":       strings.TrimPrefix(src.Path, "/"),
			"SOURCE_SSL_ARG":  mysqlSSLArg(src),
			"SINK_HOST":       dst.Hostname(),
			"SINK_PORT":       portOr(dst, "3306"),
			"SINK_USER":       dst.User.Username(),
			"SINK_PASSWORD":   dstPass,
			"SINK_DB":         strings.TrimPrefix(dst.Path, "/"),
			"SINK_SSL_ARG":    mysqlSSLArg(dst),
		}
	default:
		return fmt.Errorf("native does not run route %q", n.route)
	}
	container, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      n.Image(),
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{"sleep infinity"},
			Env:        cenv,
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "native"},
			Networks:   []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("native container: %w", err)
	}
	n.container = container
	n.script = script
	return nil
}

func portOr(u *url.URL, fallback string) string {
	if u.Port() != "" {
		return u.Port()
	}
	return fallback
}

func mysqlSSLArg(u *url.URL) string {
	switch u.Query().Get("tls") {
	case "", "false":
		return "--ssl-mode=DISABLED"
	case "true":
		return "--ssl-mode=VERIFY_IDENTITY"
	case "preferred":
		return "--ssl-mode=PREFERRED"
	default:
		return "--ssl-mode=REQUIRED"
	}
}

// Teardown removes the container.
func (n *Native) Teardown(ctx context.Context) {
	if n.container != nil {
		_ = n.container.Terminate(ctx)
	}
}

// Run executes only the prepared pipeline and observes the caller's timeout.
func (n *Native) Run(ctx context.Context) error {
	code, r, err := n.container.Exec(ctx, []string{"sh", "-c", n.script}, tcexec.Multiplexed())
	if err != nil {
		return fmt.Errorf("native run: %w", err)
	}
	out, readErr := io.ReadAll(r)
	if readErr != nil {
		return fmt.Errorf("native output: %w", readErr)
	}
	if code != 0 {
		return fmt.Errorf("native exited %d:\n%s", code, tail(out))
	}
	return nil
}

func tail(b []byte) string {
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}
