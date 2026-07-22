// Package ingestr runs ingestr (github.com/bruin-data/ingestr) from its
// official image, one `ingestr ingest` per table. The image is amd64 only
// and nothing runs emulated: on other hosts the adapter refuses to run.
package ingestr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "ghcr.io/bruin-data/ingestr:latest"

// Ingestr runs the official image, one sequential ingest command per table.
type Ingestr struct {
	container tc.Container
}

// New returns the ingestr tool.
func New() *Ingestr { return &Ingestr{} }

// Name identifies the tool.
func (g *Ingestr) Name() string { return "ingestr" }

// Image reports the image under test.
func (g *Ingestr) Image() string { return Image }

// Config reports the configuration, nil for defaults.
func (g *Ingestr) Config() map[string]any { return map[string]any{"tableParallel": true} }

// Setup creates the container, unstarted, with every table's ingest command
// launched at once. Ingestr has no cross-table orchestration of its own and
// its vendor benchmark uses a single table; running the commands in parallel
// is the strongest configuration a user can reach with shell alone.
func (g *Ingestr) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	if runtime.GOARCH != "amd64" {
		return errors.New("ingestr's official image is amd64 only; run on the benchmark machine")
	}
	src := env.Source.InternalDSN
	dst := env.Sink.InternalDSN

	// All table loads launch at once; the script fails if any command failed.
	var sb strings.Builder
	for _, t := range tables {
		fmt.Fprintf(&sb,
			"ingestr ingest --source-uri '%s' --source-table 'public.%s' --dest-uri '%s' --dest-table 'public.%s' --yes &\npids=\"$pids $!\"\n",
			src, t, dst, t)
	}
	sb.WriteString("fail=0\nfor p in $pids; do wait $p || fail=1; done\nexit $fail")
	script := sb.String()

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      Image,
			Cmd:        []string{"sh", "-c", script},
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "ingestr"},
			Networks:   []string{env.Net.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Minute),
		},
		Started: false,
	})
	if err != nil {
		return fmt.Errorf("ingestr container: %w", err)
	}
	g.container = c
	return nil
}

// Teardown removes the container.
func (g *Ingestr) Teardown(ctx context.Context) {
	if g.container != nil {
		_ = g.container.Terminate(ctx)
	}
}

// Run starts the container and blocks until it exits, erroring on a non-zero code.
func (g *Ingestr) Run(ctx context.Context) error {
	if err := g.container.Start(ctx); err != nil {
		return fmt.Errorf("ingestr start: %w", err)
	}
	state, err := g.container.State(ctx)
	if err != nil {
		return err
	}
	if state.ExitCode != 0 {
		return fmt.Errorf("ingestr exited %d:\n%s", state.ExitCode, g.logs(ctx))
	}
	return nil
}

func (g *Ingestr) logs(ctx context.Context) string {
	r, err := g.container.Logs(ctx)
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
