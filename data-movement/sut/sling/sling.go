// Package sling runs Sling (github.com/slingdata-io/sling-cli) from its
// official image. Each table is one full-refresh transfer and the transfers
// run concurrently, without relying on Sling CLI Pro's parallel-stream option.
package sling

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const (
	AMD64Image = "slingdata/sling:v1.5.20"
	ARM64Image = "slingdata/sling:1.5.20-arm64"
)

// Sling runs the official CLI image, with one transfer process per table.
type Sling struct {
	container tc.Container
	script    string
}

// New returns the Sling tool.
func New() *Sling { return &Sling{} }

// Name identifies the tool.
func (s *Sling) Name() string { return "sling" }

// Image reports the architecture-specific official image under test.
func (s *Sling) Image() string {
	switch runtime.GOARCH {
	case "amd64":
		return AMD64Image
	case "arm64":
		return ARM64Image
	default:
		return AMD64Image
	}
}

// Config reports the effective transfer configuration.
func (s *Sling) Config() map[string]any {
	return map[string]any{
		"mode":              "full-refresh",
		"tableParallel":     true,
		"processesPerTable": 1,
		"cliPro":            false,
	}
}

// Routes lists the SQL routes supported by this adapter.
func (s *Sling) Routes() []string {
	return []string{"pg-pg", "pg-mysql", "mysql-mysql", "mysql-pg"}
}

// Setup starts an idle Sling container and prepares one transfer per table.
// Container creation and destination namespace creation remain untimed.
func (s *Sling) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		return fmt.Errorf("sling has no configured image for architecture %s", runtime.GOARCH)
	}
	if len(tables) == 0 {
		return errors.New("no tables to transfer")
	}
	if env.Source.Engine == harness.Iceberg || env.Sink.Engine == harness.Iceberg {
		return errors.New("sling adapter only supports SQL routes")
	}

	if env.Sink.Engine == harness.Postgres {
		db, err := env.Sink.Open()
		if err != nil {
			return fmt.Errorf("open sink: %w", err)
		}
		defer func() { _ = db.Close() }()
		if _, err := db.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+harness.Namespace); err != nil {
			return fmt.Errorf("create sink schema: %w", err)
		}
	}

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      s.Image(),
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{"sleep infinity"},
			Env: map[string]string{
				"BENCH_SLING_SOURCE": env.Source.InternalDSN,
				"BENCH_SLING_TARGET": env.Sink.InternalDSN,
			},
			Labels:   map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "sling"},
			Networks: []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("sling container: %w", err)
	}
	s.container = c
	s.script = transferScript(tables)
	return nil
}

// transferScript launches all full-refresh table transfers and fails if any
// child process fails. Connection values stay in the environment so credentials
// are not interpolated into a shell command.
func transferScript(tables []string) string {
	var sb strings.Builder
	for _, table := range tables {
		object := harness.Namespace + "." + table
		fmt.Fprintf(&sb,
			"sling run --src-conn BENCH_SLING_SOURCE --src-stream %s --tgt-conn BENCH_SLING_TARGET --tgt-object %s --mode full-refresh &\npids=\"$pids $!\"\n",
			shellQuote(object), shellQuote(object))
	}
	sb.WriteString("fail=0\nfor p in $pids; do wait $p || fail=1; done\nexit $fail")
	return sb.String()
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

// Teardown removes the worker container.
func (s *Sling) Teardown(ctx context.Context) {
	if s.container != nil {
		_ = s.container.Terminate(ctx)
	}
}

// Run executes the prepared transfers and blocks until all tables finish.
func (s *Sling) Run(ctx context.Context) error {
	code, r, err := s.container.Exec(ctx, []string{"sh", "-c", s.script}, tcexec.Multiplexed())
	if err != nil {
		return fmt.Errorf("sling run: %w", err)
	}
	out, readErr := io.ReadAll(r)
	if readErr != nil {
		return fmt.Errorf("sling output: %w", readErr)
	}
	if code != 0 {
		return fmt.Errorf("sling exited %d:\n%s", code, tail(out))
	}
	return nil
}

func tail(b []byte) string {
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}
