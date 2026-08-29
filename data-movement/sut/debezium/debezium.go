// Package debezium runs Debezium Server with its JDBC sink in one container and
// without Kafka. A full load measures the initial snapshot, not steady-state
// streaming. Debezium Server does not exit after a snapshot, so Run stops when
// every destination row count reaches its source count. Polling adds up to one
// poll interval to the measured completion time.
package debezium

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "quay.io/debezium/server:3.6.0.Final"

// pollInterval paces the convergence check against the sink.
const pollInterval = 500 * time.Millisecond

// defaultBatchSize is used for source fetches, engine batches, and sink writes.
// BENCH_DEBEZIUM_BATCH_SIZE can override it before a benchmark cohort.
const defaultBatchSize = 32768

// Keep source-side snapshot buffering at Debezium's documented PostgreSQL
// default. It is allocated per snapshot thread, unlike the sink batch.
const snapshotFetchSize = 10240

const javaToolOptions = "-XX:InitialRAMPercentage=10 -XX:MaxRAMPercentage=50"

// Debezium runs Debezium Server with the JDBC sink.
type Debezium struct {
	route     string
	container tc.Container
	env       *harness.Env
	tables    []string
	expected  map[string]int64
	workers   int
	batchSize int
	slotName  string
	processID string
}

// New returns the debezium tool for a route.
func New(route string) *Debezium {
	return &Debezium{
		route: route, workers: 32,
		batchSize: tuningInt("BENCH_DEBEZIUM_BATCH_SIZE", defaultBatchSize),
	}
}

// Name identifies the tool.
func (d *Debezium) Name() string { return "debezium" }

// Image reports the image under test.
func (d *Debezium) Image() string { return Image }

// Routes lists the routes the JDBC sink and source connectors cover.
func (d *Debezium) Routes() []string { return []string{"pg-pg", "mysql-pg"} }

// Config reports the sync configuration and the completion rule.
func (d *Debezium) Config() map[string]any {
	return map[string]any{
		"sink":               "jdbc",
		"snapshotMode":       "initial_only",
		"snapshotMaxThreads": d.workers,
		"snapshotFetchSize":  snapshotFetchSize,
		"maxBatchSize":       d.batchSize,
		"maxQueueSize":       d.batchSize * 4,
		"sinkBatchSize":      d.batchSize,
		"heapMaxRAMPercent":  50,
		"insertMode":         "insert",
		"primaryKeyMode":     "none",
		"schemaEvolution":    "basic",
		"completionCheck":    fmt.Sprintf("row-count poll @%s", pollInterval),
	}
}

// Setup accepts the seed manifest, prepares the sink namespace, and starts an
// idle server container with its config baked in. Run starts the Debezium
// process itself, keeping Docker startup outside the timed window.
func (d *Debezium) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	d.env = env
	d.tables = tables
	d.slotName = strings.ReplaceAll(env.RunID, "-", "_")

	props, err := d.properties(env, tables)
	if err != nil {
		return err
	}

	d.expected = make(map[string]int64, len(tables))
	for _, t := range tables {
		n, ok := env.Expected[t]
		if !ok {
			return fmt.Errorf("seed manifest has no row count for %s", t)
		}
		d.expected[t] = n
	}

	// The sink schema must exist before the sink's schema evolution can
	// create tables in it; on mysql the bench database already does.
	if env.Sink.Engine == harness.Postgres {
		sink, err := env.Sink.Open()
		if err != nil {
			return fmt.Errorf("open sink: %w", err)
		}
		defer func() { _ = sink.Close() }()
		if _, err := sink.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+harness.Namespace); err != nil {
			return fmt.Errorf("create sink schema: %w", err)
		}
	}

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image:      Image,
			Entrypoint: []string{"/bin/sh", "-c"},
			Cmd:        []string{"sleep infinity"},
			Env:        map[string]string{"JAVA_TOOL_OPTIONS": javaToolOptions},
			Files: []tc.ContainerFile{{
				Reader:            strings.NewReader(props),
				ContainerFilePath: "/debezium/config/application.properties",
				FileMode:          0o644,
			}},
			Labels:   map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "debezium"},
			Networks: []string{env.Net.Name},
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("debezium container: %w", err)
	}
	d.container = c
	return nil
}

// Teardown removes the container.
func (d *Debezium) Teardown(ctx context.Context) {
	if d.container != nil {
		_ = d.container.Terminate(ctx)
	}
}

// Run starts the server process and blocks until every table converges on the sink.
// The server keeps running after the snapshot; convergence is completion. An
// early exit gets one final check, since the last events may have landed.
func (d *Debezium) Run(ctx context.Context) error {
	code, out, err := d.exec(ctx, []string{"/bin/sh", "-c", "/debezium/run.sh >/tmp/debezium.log 2>&1 & echo $!"})
	if err != nil {
		return fmt.Errorf("debezium start process: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("debezium start process exited %d:\n%s", code, out)
	}
	d.processID = strings.TrimSpace(out)
	if d.processID == "" {
		return fmt.Errorf("debezium start process returned no pid")
	}
	sink, err := d.env.Sink.Open()
	if err != nil {
		return fmt.Errorf("open sink: %w", err)
	}
	defer func() { _ = sink.Close() }()

	remaining := append([]string(nil), d.tables...)
	for {
		remaining, err = d.advance(ctx, sink, remaining)
		if err != nil {
			return err
		}
		if len(remaining) == 0 {
			return nil
		}
		running, serr := d.processRunning(ctx)
		if serr == nil && !running {
			remaining, err = d.advance(ctx, sink, remaining)
			if err != nil {
				return err
			}
			if len(remaining) == 0 {
				return nil
			}
			return fmt.Errorf("debezium exited before %s converged:\n%s",
				strings.Join(remaining, ", "), d.logs(ctx))
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("debezium: %s never converged: %w", strings.Join(remaining, ", "), ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

// advance pops every table whose sink count reached the source count. Tables
// convergence in seed order, so one count per tick does most of the waiting.
// A count query failing means the sink has not created the table yet. Upsert
// mode cannot overshoot, so a count above the source is a hard failure.
func (d *Debezium) advance(ctx context.Context, sink *sql.DB, remaining []string) ([]string, error) {
	for len(remaining) > 0 {
		t := remaining[0]
		var n int64
		q := fmt.Sprintf("SELECT count(*) FROM %s.%s", harness.Namespace, t)
		if err := sink.QueryRowContext(ctx, q).Scan(&n); err != nil || n < d.expected[t] {
			return remaining, nil
		}
		if n > d.expected[t] {
			return remaining, fmt.Errorf("debezium: %s has %d rows on the sink, source has %d", t, n, d.expected[t])
		}
		remaining = remaining[1:]
	}
	return remaining, nil
}

// properties renders application.properties for the route.
func (d *Debezium) properties(env *harness.Env, tables []string) (string, error) {
	sinkURL, err := jdbcURL(env.Sink)
	if err != nil {
		return "", err
	}
	_, _, _, sinkUser, sinkPass, err := splitDSN(env.Sink.InternalDSN)
	if err != nil {
		return "", err
	}
	srcHost, srcPort, srcDB, srcUser, srcPass, err := splitDSN(env.Source.InternalDSN)
	if err != nil {
		return "", err
	}

	qualified := make([]string, 0, len(tables))
	for _, t := range tables {
		qualified = append(qualified, harness.Namespace+"."+t)
	}
	lines := []string{
		"debezium.sink.type=jdbc",
		"debezium.sink.jdbc.connection.url=" + sinkURL,
		"debezium.sink.jdbc.connection.username=" + sinkUser,
		"debezium.sink.jdbc.connection.password=" + sinkPass,
		// A full load writes into a sink dropped and recreated moments earlier,
		// so there is nothing to conflict with: plain insert skips the per-row
		// index probe upsert pays for, and no key mode is needed to drive it. A
		// replayed batch would raise a unique violation on the table's own
		// primary key, failing the repetition rather than duplicating rows.
		"debezium.sink.jdbc.insert.mode=insert",
		"debezium.sink.jdbc.primary.key.mode=none",
		"debezium.sink.jdbc.schema.evolution=basic",
		// Source fetches are per snapshot thread, so keep them bounded separately
		// from the calibrated engine and sink batch size.
		fmt.Sprintf("debezium.sink.jdbc.batch.size=%d", d.batchSize),
		fmt.Sprintf("debezium.sink.jdbc.connection.pool.max_size=%d", d.workers),
		// $$ keeps quarkus from expanding the placeholder before debezium sees it.
		"debezium.sink.jdbc.collection.name.format=" + harness.Namespace + ".$${source.table}",
		"debezium.source.database.hostname=" + srcHost,
		"debezium.source.database.port=" + srcPort,
		"debezium.source.database.user=" + srcUser,
		"debezium.source.database.password=" + srcPass,
		"debezium.source.topic.prefix=dbz",
		"debezium.source.table.include.list=" + strings.Join(qualified, ","),
		"debezium.source.snapshot.mode=initial_only",
		fmt.Sprintf("debezium.source.snapshot.max.threads=%d", d.workers),
		fmt.Sprintf("debezium.source.snapshot.fetch.size=%d", snapshotFetchSize),
		fmt.Sprintf("debezium.source.max.batch.size=%d", d.batchSize),
		// Hold four batches in the queue. Debezium requires the queue size to
		// exceed the batch size.
		fmt.Sprintf("debezium.source.max.queue.size=%d", d.batchSize*4),
		"debezium.source.offset.storage.file.filename=/tmp/offsets.dat",
		"debezium.source.offset.flush.interval.ms=1000",
	}
	switch env.Source.Engine {
	case harness.Postgres:
		sourceURL, _ := url.Parse(env.Source.InternalDSN)
		sslMode := sourceURL.Query().Get("sslmode")
		if sslMode == "" {
			sslMode = "disable"
		}
		lines = append(lines,
			"debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
			"debezium.source.database.dbname="+srcDB,
			"debezium.source.plugin.name=pgoutput",
			"debezium.source.database.sslmode="+sslMode,
			"debezium.source.slot.name="+d.slotName,
			"debezium.source.publication.name="+d.slotName,
			"debezium.source.slot.drop.on.stop=true",
		)
	case harness.MySQL:
		sourceURL, _ := url.Parse(env.Source.InternalDSN)
		sslMode := "disabled"
		switch sourceURL.Query().Get("tls") {
		case "true":
			sslMode = "verify_identity"
		case "", "false":
		default:
			sslMode = "required"
		}
		lines = append(lines,
			"debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector",
			"debezium.source.database.include.list="+srcDB,
			"debezium.source.database.server.id=18054",
			"debezium.source.database.ssl.mode="+sslMode,
			"debezium.source.schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory",
			"debezium.source.schema.history.internal.file.filename=/tmp/schemahistory.dat",
		)
	default:
		return "", fmt.Errorf("no debezium source connector for engine %v", env.Source.Engine)
	}
	return strings.Join(lines, "\n") + "\n", nil
}

// jdbcURL renders the sink's JDBC connection URL.
func jdbcURL(db *harness.DB) (string, error) {
	host, port, name, _, _, err := splitDSN(db.InternalDSN)
	if err != nil {
		return "", err
	}
	switch db.Engine {
	case harness.Postgres:
		u, _ := url.Parse(db.InternalDSN)
		query := u.Query()
		return fmt.Sprintf("jdbc:postgresql://%s:%s/%s?%s", host, port, name, query.Encode()), nil
	case harness.MySQL:
		u, _ := url.Parse(db.InternalDSN)
		query := url.Values{}
		switch u.Query().Get("tls") {
		case "true":
			query.Set("sslMode", "VERIFY_IDENTITY")
		case "", "false":
			query.Set("sslMode", "DISABLED")
		default:
			query.Set("sslMode", "REQUIRED")
		}
		return fmt.Sprintf("jdbc:mysql://%s:%s/%s?%s", host, port, name, query.Encode()), nil
	}
	return "", fmt.Errorf("no jdbc dialect for engine %v", db.Engine)
}

func tuningInt(name string, fallback int) int {
	if raw := os.Getenv(name); raw != "" {
		if value, err := strconv.Atoi(raw); err == nil && value > 0 {
			return value
		}
	}
	return fallback
}

// splitDSN breaks a database URL into the parts the config needs.
func splitDSN(dsn string) (host, port, db, user, pass string, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", "", "", "", "", err
	}
	pass, _ = u.User.Password()
	return u.Hostname(), u.Port(), strings.TrimPrefix(u.Path, "/"), u.User.Username(), pass, nil
}

func (d *Debezium) exec(ctx context.Context, cmd []string) (int, string, error) {
	code, r, err := d.container.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		return 0, "", err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return code, "", err
	}
	if len(b) > 4000 {
		b = b[len(b)-4000:]
	}
	return code, string(b), nil
}

func (d *Debezium) processRunning(ctx context.Context) (bool, error) {
	// A finished orphan may remain as a zombie under the idle PID 1; kill -0
	// alone considers that alive, so reject the Z process state as well.
	check := "kill -0 " + d.processID + " && test \"$(awk '{print $3}' /proc/" + d.processID + "/stat)\" != Z"
	code, _, err := d.exec(ctx, []string{"/bin/sh", "-c", check})
	return code == 0, err
}

func (d *Debezium) logs(ctx context.Context) string {
	// JSON exceptions can exceed the generic output cap. Keep the beginning of
	// the final error record, which contains its type and database message.
	cmd := `line=$(grep -E '"level":"(ERROR|FATAL)"' /tmp/debezium.log | tail -n 1); if [ -n "$line" ]; then printf '%s\n' "$line" | cut -c 1-3500; else tail -c 3500 /tmp/debezium.log; fi`
	_, out, err := d.exec(ctx, []string{"/bin/sh", "-c", cmd})
	if err != nil {
		return "(logs unavailable)"
	}
	return out
}
