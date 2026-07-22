// Package dlt runs dlt (github.com/dlt-hub/dlt), which ships as a Python
// library, not an image: a plain python container gets the library installed
// during untimed Setup and one pipeline.run moves every table. The pipeline
// mirrors the vendor's own benchmark configuration
// (github.com/dlt-hub/sql_database_benchmarking): connectorx backend,
// parallelized extraction, csv (COPY) load, and their recommended normalize
// and data-writer settings.
package dlt

import (
	"context"
	_ "embed"
	"fmt"
	"io"
	"strings"

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "python:3.11-slim"

// Requirements is what pip installs during Setup.
var Requirements = []string{"dlt[postgres,sql-database]", "connectorx", "pyarrow"}

// pipeline is the script Run executes; DSNs and tables arrive as env vars.
//
//go:embed pipeline.py
var pipeline []byte

// Dlt runs the library in a python container, one pipeline over all tables.
type Dlt struct {
	container tc.Container
}

// New returns the dlt tool.
func New() *Dlt { return &Dlt{} }

// Name identifies the tool.
func (d *Dlt) Name() string { return "dlt" }

// Image reports the image under test.
func (d *Dlt) Image() string { return Image + " + " + strings.Join(Requirements, " ") }

// Config reports the configuration, mirroring the vendor benchmark.
func (d *Dlt) Config() map[string]any {
	return map[string]any{
		"backend":          "connectorx",
		"chunkSize":        100000,
		"loaderFileFormat": "csv",
		"parallelize":      true,
		"normalizeWorkers": 3,
	}
}

// Setup starts an idle python container, copies the pipeline script in, and
// installs dlt; all of it stays outside the timed window.
func (d *Dlt) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image: Image,
			Cmd:   []string{"sleep", "infinity"},
			Env: map[string]string{
				"SOURCE_DSN": env.Source.InternalDSN,
				"SINK_DSN":   env.Sink.InternalDSN,
				"TABLES":     strings.Join(tables, ","),
				// The vendor benchmark's recommended performance settings.
				"NORMALIZE__WORKERS":                     "3",
				"SOURCES__DATA_WRITER__FILE_MAX_BYTES":   "3000000",
				"SOURCES__DATA_WRITER__BUFFER_MAX_ITEMS": "200000",
			},
			Labels:   map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "dlt"},
			Networks: []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("dlt container: %w", err)
	}
	d.container = c

	if err := c.CopyToContainer(ctx, pipeline, "/pipeline.py", 0o644); err != nil {
		return fmt.Errorf("copy pipeline: %w", err)
	}
	code, out, err := d.exec(ctx, append([]string{"pip", "install", "--quiet", "--no-cache-dir"}, Requirements...))
	if err != nil {
		return fmt.Errorf("pip install: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("pip install exited %d:\n%s", code, out)
	}
	return nil
}

// Teardown removes the container.
func (d *Dlt) Teardown(ctx context.Context) {
	if d.container != nil {
		_ = d.container.Terminate(ctx)
	}
}

// Run executes the pipeline script and blocks, erroring on a non-zero exit.
func (d *Dlt) Run(ctx context.Context) error {
	code, out, err := d.exec(ctx, []string{"python", "/pipeline.py"})
	if err != nil {
		return fmt.Errorf("dlt run: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("dlt exited %d:\n%s", code, out)
	}
	return nil
}

// exec runs cmd in the container, returning its exit code and tail of output.
func (d *Dlt) exec(ctx context.Context, cmd []string) (int, string, error) {
	code, r, err := d.container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return 0, "", err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return code, "(output unavailable)", nil
	}
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return code, string(b), nil
}
