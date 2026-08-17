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

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "ghcr.io/bruin-data/ingestr:latest"

// workerBudget is the configured parallelism for reference remote runs.
const workerBudget = 32

// Ingestr runs the official image, with one ingest command per table.
type Ingestr struct {
	container tc.Container
	script    string
}

// New returns the ingestr tool.
func New() *Ingestr { return &Ingestr{} }

// Name identifies the tool.
func (g *Ingestr) Name() string { return "ingestr" }

// Image reports the image under test.
func (g *Ingestr) Image() string { return Image }

// Config reports the effective ingest configuration.
func (g *Ingestr) Config() map[string]any {
	return map[string]any{
		"tableParallel": true, "sqlBackend": "pyarrow", "loaderFileFormat": "csv",
		"pageSize": 100000, "workerBudget": workerBudget,
	}
}

// Routes lists every route; ingestr speaks both engines on both sides.
func (g *Ingestr) Routes() []string { return []string{"pg-pg", "pg-mysql", "mysql-mysql", "mysql-pg"} }

// Setup starts an idle container and prepares one ingest command per table.
// Run launches the commands concurrently and divides the worker budget across
// tables that support partitioned extraction.
func (g *Ingestr) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	if runtime.GOARCH != "amd64" {
		return errors.New("ingestr's official image is amd64 only; run on the benchmark machine")
	}
	if len(tables) == 0 {
		return errors.New("no tables to ingest")
	}
	src := env.Source.InternalDSN
	dst := env.Sink.InternalDSN
	perTable := workerBudget / len(tables)
	if perTable < 1 {
		perTable = 1
	}

	// All table loads launch at once; the script fails if any command failed.
	var sb strings.Builder
	for _, t := range tables {
		partition := ""
		if key := partitionKey(t); key != "" {
			partition = fmt.Sprintf(" --extract-parallelism %d --extract-partition-by %s", perTable, shellQuote(key))
		}
		fmt.Fprintf(&sb,
			"ingestr ingest --source-uri %s --source-table %s --dest-uri %s --dest-table %s --sql-backend pyarrow --loader-file-format csv --page-size 100000%s --yes &\npids=\"$pids $!\"\n",
			shellQuote(src), shellQuote(harness.Namespace+"."+t), shellQuote(dst), shellQuote(harness.Namespace+"."+t), partition)
	}
	sb.WriteString("fail=0\nfor p in $pids; do wait $p || fail=1; done\nexit $fail")
	script := sb.String()

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      Image,
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{"sleep infinity"},
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "ingestr"},
			Networks:   []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("ingestr container: %w", err)
	}
	g.container = c
	g.script = script
	return nil
}

func partitionKey(table string) string {
	return map[string]string{
		"region": "r_regionkey", "nation": "n_nationkey", "supplier": "s_suppkey",
		"customer": "c_custkey", "part": "p_partkey", "partsupp": "ps_partkey",
		"orders": "o_orderkey", "lineitem": "l_orderkey", "trips": "id", "fhv_trips": "id",
	}[table]
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}

// Teardown removes the container.
func (g *Ingestr) Teardown(ctx context.Context) {
	if g.container != nil {
		_ = g.container.Terminate(ctx)
	}
}

// Run executes only the prepared ingest commands and observes the caller's timeout.
func (g *Ingestr) Run(ctx context.Context) error {
	code, r, err := g.container.Exec(ctx, []string{"sh", "-c", g.script}, tcexec.Multiplexed())
	if err != nil {
		return fmt.Errorf("ingestr run: %w", err)
	}
	out, readErr := io.ReadAll(r)
	if readErr != nil {
		return fmt.Errorf("ingestr output: %w", readErr)
	}
	if code != 0 {
		return fmt.Errorf("ingestr exited %d:\n%s", code, tail(out))
	}
	return nil
}

func tail(b []byte) string {
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return string(b)
}
