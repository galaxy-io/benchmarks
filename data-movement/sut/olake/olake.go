// Package olake runs OLake (github.com/datazip-inc/olake) from its official
// per-source images: one container runs discover to build the streams catalog
// (every bench table, full refresh, normalization on), then sync into iceberg.
package olake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const (
	PostgresImage = "olakego/source-postgres:latest"
	MySQLImage    = "olakego/source-mysql:latest"
)

// OLake runs the official image for the route's source engine.
type OLake struct {
	route     string
	container tc.Container
}

// New returns the olake tool for a route.
func New(route string) *OLake { return &OLake{route: route} }

// Name identifies the tool.
func (o *OLake) Name() string { return "olake" }

// Image reports the image under test, chosen by the route's source engine.
func (o *OLake) Image() string {
	switch {
	case strings.HasPrefix(o.route, "pg-"):
		return PostgresImage
	case strings.HasPrefix(o.route, "mysql-"):
		return MySQLImage
	}
	return ""
}

// Options is the run configuration, recorded in the result document. The
// thread count matches olake's own test configs; the unset default is 3.
var Options = map[string]any{
	"maxThreads": 30,
	"syncMode":   "full_refresh",
}

// Config reports the run options.
func (o *OLake) Config() map[string]any { return Options }

// Routes lists every route; olake only writes iceberg.
func (o *OLake) Routes() []string { return []string{"pg-iceberg", "mysql-iceberg"} }

// Setup creates the container, unstarted, with discover and sync chained so
// the timed window covers exactly what olake does for a full load.
func (o *OLake) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	source, err := sourceConfig(env.Source)
	if err != nil {
		return err
	}
	dest, err := destinationConfig(env.Sink)
	if err != nil {
		return err
	}

	// discover writes streams.json beside the source config; passing --streams
	// there would instead try to read it as a prior catalog and fail. The
	// generated catalog defaults to incremental, which needs a cursor column,
	// so every stream is forced to full_refresh before the sync.
	// The rewrite also pins destination_database: olake prefixes the connector
	// type onto its generated destination and the prefix flag cannot be blanked.
	script := `set -e
/home/olake discover --config /mnt/config/source.json
sed -i -e 's/"sync_mode":[[:space:]]*"[a-z_]*"/"sync_mode":"full_refresh"/g' \
       -e 's/"destination_database":[[:space:]]*"[^"]*"/"destination_database":"` + harness.Namespace + `"/g' /mnt/config/streams.json
/home/olake sync --config /mnt/config/source.json --destination /mnt/config/destination.json --catalog /mnt/config/streams.json`

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      o.Image(),
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{script},
			Files: []tc.ContainerFile{
				{Reader: strings.NewReader(source), ContainerFilePath: "/mnt/config/source.json", FileMode: 0o644},
				{Reader: strings.NewReader(dest), ContainerFilePath: "/mnt/config/destination.json", FileMode: 0o644},
			},
			Labels:     map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "olake"},
			Networks:   []string{env.Net.Name},
			WaitingFor: wait.ForExit().WithExitTimeout(30 * time.Minute),
		},
		Started: false,
	})
	if err != nil {
		return fmt.Errorf("olake container: %w", err)
	}
	o.container = c
	return nil
}

// Teardown removes the container.
func (o *OLake) Teardown(ctx context.Context) {
	if o.container != nil {
		_ = o.container.Terminate(ctx)
	}
}

// Run starts the container and blocks until it exits, erroring on a non-zero code.
func (o *OLake) Run(ctx context.Context) error {
	if err := o.container.Start(ctx); err != nil {
		return fmt.Errorf("olake start: %w", err)
	}
	state, err := o.container.State(ctx)
	if err != nil {
		return err
	}
	if state.ExitCode != 0 {
		return fmt.Errorf("olake exited %d:\n%s", state.ExitCode, o.logs(ctx))
	}
	return nil
}

// sourceConfig renders the source connector config json for db's engine.
func sourceConfig(db *harness.DB) (string, error) {
	var cfg map[string]any
	switch db.Engine {
	case harness.Postgres:
		cfg = map[string]any{
			"host":        "source",
			"port":        5432,
			"database":    "bench",
			"username":    "bench",
			"password":    "bench",
			"schemas":     []string{harness.Namespace},
			"ssl":         map[string]any{"mode": "disable"},
			"max_threads": Options["maxThreads"],
		}
	case harness.MySQL:
		cfg = map[string]any{
			"hosts":       "source",
			"port":        3306,
			"database":    harness.Namespace,
			"username":    "bench",
			"password":    "bench",
			"max_threads": Options["maxThreads"],
		}
	default:
		return "", fmt.Errorf("no olake source config for engine %v", db.Engine)
	}
	b, err := json.Marshal(cfg)
	return string(b), err
}

// destinationConfig renders the iceberg REST writer config json from the sink env.
func destinationConfig(db *harness.DB) (string, error) {
	if db.Engine != harness.Iceberg {
		return "", fmt.Errorf("no olake destination config for engine %v", db.Engine)
	}
	b, err := json.Marshal(map[string]any{
		"type": "ICEBERG",
		"writer": map[string]any{
			"catalog_type":     "rest",
			"catalog_name":     "olake_iceberg",
			"rest_catalog_url": db.InternalDSN,
			"iceberg_s3_path":  "s3://" + db.Props["warehouse"],
			"iceberg_db":       harness.Namespace,
			"s3_endpoint":      db.Props["s3.endpoint"],
			"s3_use_ssl":       false,
			"s3_path_style":    true,
			"aws_access_key":   db.Props["s3.access-key-id"],
			"aws_secret_key":   db.Props["s3.secret-access-key"],
			"aws_region":       db.Props["s3.region"],
		},
	})
	return string(b), err
}

func (o *OLake) logs(ctx context.Context) string {
	r, err := o.container.Logs(ctx)
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
