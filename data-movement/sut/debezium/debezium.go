// Package debezium runs Debezium Server (quay.io/debezium/server) with its
// JDBC sink: one container, no Kafka, the strongest configuration a user can
// reach without standing up a broker. A full load measures Debezium's initial
// snapshot path, not the steady-state streaming it is built for; the result
// should always say so. A CDC server never exits, so Run polls the sink until
// every table's row count matches the source, then stops the clock. The poll
// quantizes wall time by its interval, noise against real load times.
package debezium

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	tc "github.com/testcontainers/testcontainers-go"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

const Image = "quay.io/debezium/server:3.6.0.Final"

// pollInterval paces the convergence check against the sink.
const pollInterval = 500 * time.Millisecond

// Debezium runs Debezium Server with the JDBC sink.
type Debezium struct {
	route     string
	container tc.Container
	env       *harness.Env
	tables    []string
	expected  map[string]int64
}

// New returns the debezium tool for a route.
func New(route string) *Debezium { return &Debezium{route: route} }

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
		"snapshotMaxThreads": 8,
		"insertMode":         "upsert",
		"primaryKeyMode":     "record_key",
		"schemaEvolution":    "basic",
		"completionCheck":    fmt.Sprintf("row-count poll @%s", pollInterval),
	}
}

// Setup records the source row counts, prepares the sink namespace, and
// creates the server container, unstarted, with its config baked in.
func (d *Debezium) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	d.env = env
	d.tables = tables

	props, err := d.properties(env, tables)
	if err != nil {
		return err
	}

	src, err := env.Source.Open()
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = src.Close() }()
	d.expected = map[string]int64{}
	for _, t := range tables {
		var n int64
		q := fmt.Sprintf("SELECT count(*) FROM %s.%s", harness.Namespace, t)
		if err := src.QueryRowContext(ctx, q).Scan(&n); err != nil {
			return fmt.Errorf("count source %s: %w", t, err)
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
			Image: Image,
			Files: []tc.ContainerFile{{
				Reader:            strings.NewReader(props),
				ContainerFilePath: "/debezium/config/application.properties",
				FileMode:          0o644,
			}},
			Labels:   map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: "debezium"},
			Networks: []string{env.Net.Name},
		},
		Started: false,
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

// Run starts the server and blocks until every table converges on the sink.
// The server keeps running after the snapshot; convergence is completion. An
// early exit gets one final check, since the last events may have landed.
func (d *Debezium) Run(ctx context.Context) error {
	if err := d.container.Start(ctx); err != nil {
		return fmt.Errorf("debezium start: %w", err)
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
		state, serr := d.container.State(ctx)
		if serr == nil && !state.Running {
			remaining, err = d.advance(ctx, sink, remaining)
			if err != nil {
				return err
			}
			if len(remaining) == 0 {
				return nil
			}
			return fmt.Errorf("debezium exited %d before %s converged:\n%s",
				state.ExitCode, strings.Join(remaining, ", "), d.logs(ctx))
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
		"debezium.sink.jdbc.insert.mode=upsert",
		"debezium.sink.jdbc.primary.key.mode=record_key",
		"debezium.sink.jdbc.schema.evolution=basic",
		// $$ keeps quarkus from expanding the placeholder before debezium sees it.
		"debezium.sink.jdbc.collection.name.format=" + harness.Namespace + ".$${source.table}",
		"debezium.source.database.hostname=" + srcHost,
		"debezium.source.database.port=" + srcPort,
		"debezium.source.database.user=" + srcUser,
		"debezium.source.database.password=" + srcPass,
		"debezium.source.topic.prefix=dbz",
		"debezium.source.table.include.list=" + strings.Join(qualified, ","),
		"debezium.source.snapshot.mode=initial_only",
		"debezium.source.snapshot.max.threads=8",
		"debezium.source.offset.storage.file.filename=/tmp/offsets.dat",
		"debezium.source.offset.flush.interval.ms=1000",
	}
	switch env.Source.Engine {
	case harness.Postgres:
		lines = append(lines,
			"debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector",
			"debezium.source.database.dbname="+srcDB,
			"debezium.source.plugin.name=pgoutput",
		)
	case harness.MySQL:
		lines = append(lines,
			"debezium.source.connector.class=io.debezium.connector.mysql.MySqlConnector",
			"debezium.source.database.include.list="+srcDB,
			"debezium.source.database.server.id=18054",
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
		return fmt.Sprintf("jdbc:postgresql://%s:%s/%s", host, port, name), nil
	case harness.MySQL:
		return fmt.Sprintf("jdbc:mysql://%s:%s/%s", host, port, name), nil
	}
	return "", fmt.Errorf("no jdbc dialect for engine %v", db.Engine)
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

func (d *Debezium) logs(ctx context.Context) string {
	r, err := d.container.Logs(ctx)
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
