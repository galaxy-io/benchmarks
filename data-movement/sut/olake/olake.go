// Package olake runs OLake (github.com/datazip-inc/olake) from its official
// per-source images: one container runs discover to build the streams catalog
// (every bench table, full refresh, normalization on), then sync into Iceberg.
package olake

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

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
	syncCmd   []string
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

// Options is the reference run configuration recorded in results.
var Options = map[string]any{
	"maxThreads":      32,
	"readerBatchSize": 1000,
	"arrowWrites":     true,
	"syncMode":        "full_refresh",
}

// Config reports the run options.
func (o *OLake) Config() map[string]any { return Options }

// Routes lists every route; OLake only writes Iceberg.
func (o *OLake) Routes() []string { return []string{"pg-iceberg", "mysql-iceberg"} }

// Setup starts an idle worker and performs discovery outside the timed window,
// matching the setup boundary used by the Airbyte adapter.
func (o *OLake) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	source, err := sourceConfig(env.Source)
	if err != nil {
		return err
	}
	dest, err := destinationConfig(env.Sink)
	if err != nil {
		return err
	}

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      o.Image(),
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{"sleep infinity"},
			Files: []tc.ContainerFile{
				{Reader: strings.NewReader(source), ContainerFilePath: "/mnt/config/source.json", FileMode: 0o644},
				{Reader: strings.NewReader(dest), ContainerFilePath: "/mnt/config/destination.json", FileMode: 0o644},
			},
			Labels:   map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "olake"},
			Networks: []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("olake container: %w", err)
	}
	o.container = c
	discover := `/home/olake discover --config /mnt/config/source.json &&
sed -i -e 's/"sync_mode":[[:space:]]*"[a-z_]*"/"sync_mode":"full_refresh"/g' \
       -e 's/"destination_database":[[:space:]]*"[^"]*"/"destination_database":"` + harness.Namespace + `"/g' /mnt/config/streams.json`
	if code, out, err := o.exec(ctx, []string{"/bin/sh", "-c", discover}); err != nil {
		return fmt.Errorf("olake discover: %w", err)
	} else if code != 0 {
		return fmt.Errorf("olake discover exited %d:\n%s", code, out)
	}
	o.syncCmd = []string{"/home/olake", "sync", "--config", "/mnt/config/source.json",
		"--destination", "/mnt/config/destination.json", "--catalog", "/mnt/config/streams.json"}
	return nil
}

// Teardown removes the container.
func (o *OLake) Teardown(ctx context.Context) {
	if o.container != nil {
		_ = o.container.Terminate(ctx)
	}
}

// Run executes only the sync command and blocks until it exits.
func (o *OLake) Run(ctx context.Context) error {
	code, out, err := o.exec(ctx, o.syncCmd)
	if err != nil {
		return fmt.Errorf("olake sync: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("olake exited %d:\n%s", code, out)
	}
	return nil
}

// sourceConfig renders the source connector config json for db's engine.
func sourceConfig(db *harness.DB) (string, error) {
	u, err := url.Parse(db.InternalDSN)
	if err != nil {
		return "", err
	}
	pass, _ := u.User.Password()
	port := 0
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return "", fmt.Errorf("source port %q: %w", u.Port(), err)
		}
	}
	var cfg map[string]any
	switch db.Engine {
	case harness.Postgres:
		if port == 0 {
			port = 5432
		}
		sslMode := u.Query().Get("sslmode")
		if sslMode == "" {
			sslMode = "disable"
		}
		cfg = map[string]any{
			"host":              u.Hostname(),
			"port":              port,
			"database":          strings.TrimPrefix(u.Path, "/"),
			"username":          u.User.Username(),
			"password":          pass,
			"schemas":           []string{harness.Namespace},
			"ssl":               map[string]any{"mode": sslMode},
			"max_threads":       Options["maxThreads"],
			"reader_batch_size": Options["readerBatchSize"],
		}
	case harness.MySQL:
		if port == 0 {
			port = 3306
		}
		tls := u.Query().Get("tls")
		sslMode := "disable"
		if tls != "" && tls != "false" {
			sslMode = "require"
		}
		if tls == "true" {
			sslMode = "verify-full"
		}
		cfg = map[string]any{
			"hosts":             u.Hostname(),
			"port":              port,
			"database":          strings.TrimPrefix(u.Path, "/"),
			"username":          u.User.Username(),
			"password":          pass,
			"ssl":               map[string]any{"mode": sslMode},
			"tls_skip_verify":   tls == "skip-verify",
			"max_threads":       Options["maxThreads"],
			"reader_batch_size": Options["readerBatchSize"],
		}
	default:
		return "", fmt.Errorf("no olake source config for engine %v", db.Engine)
	}
	b, err := json.Marshal(cfg)
	return string(b), err
}

// destinationConfig renders the Iceberg REST writer config JSON from the sink environment.
func destinationConfig(db *harness.DB) (string, error) {
	if db.Engine != harness.Iceberg {
		return "", fmt.Errorf("no olake destination config for engine %v", db.Engine)
	}
	writer := map[string]any{
		"catalog_type":     "rest",
		"catalog_name":     "olake_iceberg",
		"rest_catalog_url": db.InternalDSN,
		"iceberg_s3_path":  "s3://" + db.Props[harness.PropWarehouse],
		"iceberg_db":       harness.Namespace,
		"s3_use_ssl":       db.Props[harness.PropS3UseSSL] == "true",
		"s3_path_style":    db.Props[harness.PropS3PathStyle] == "true",
		"aws_region":       db.Props[harness.PropS3Region],
		"arrow_writes":     Options["arrowWrites"],
	}
	// OLake reads the keys as optional and falls back to the instance profile,
	// so absent credentials stay out of the config rather than going in empty.
	if key := db.Props[harness.PropS3Key]; key != "" {
		writer["aws_access_key"] = key
		writer["aws_secret_key"] = db.Props[harness.PropS3Secret]
		if token := db.Props[harness.PropS3Token]; token != "" {
			writer["aws_session_token"] = token
		}
	}
	if endpoint := db.Props[harness.PropS3Endpoint]; endpoint != "" {
		writer["s3_endpoint"] = endpoint
	}
	b, err := json.Marshal(map[string]any{
		"type": "ICEBERG", "writer": writer,
	})
	return string(b), err
}

func (o *OLake) exec(ctx context.Context, cmd []string) (int, string, error) {
	code, r, err := o.container.Exec(ctx, cmd, tcexec.Multiplexed())
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
