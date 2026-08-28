// Package peerdb runs PeerDB (github.com/PeerDB-io/peerdb), which ships as a
// stack rather than a binary: a catalog database, Temporal and its admin tools,
// the flow API, a general worker, a snapshot worker, and the server that speaks
// the postgres wire protocol. The whole stack starts during untimed Setup; the
// timed window is one CREATE MIRROR configured for an initial snapshot only.
// PeerDB marks the flow completed when that snapshot lands.
package peerdb

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"

	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	tc "github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
)

// The stack, pinned together: the flow images and the server ship as one
// release and are not mixed across versions.
const (
	Version           = "stable-v0.37.0"
	CatalogImage      = "postgres:16"
	TemporalImage     = "temporalio/auto-setup:1.29"
	TemporalCLIImage  = "temporalio/admin-tools:1.25.2-tctl-1.18.1-cli-1.1.1"
	FlowAPIImage      = "ghcr.io/peerdb-io/flow-api:" + Version
	FlowWorkerImage   = "ghcr.io/peerdb-io/flow-worker:" + Version
	SnapshotWorkImage = "ghcr.io/peerdb-io/flow-snapshot-worker:" + Version
	ServerImage       = "ghcr.io/peerdb-io/peerdb-server:" + Version
)

// pollInterval paces status checks against PeerDB's catalog.
const pollInterval = 500 * time.Millisecond

const slotCleanupTimeout = 10 * time.Second

// Options is the mirror configuration, recorded in the result document, and
// follows PeerDB's own scale test (PeerDB-io/ab-scale-testing): its sweep of
// 1, 8, 16, 32, and 48 threads settled on 32, and it raised the partition size
// to about 750k rows to cut a worker's round trips.
//
// Workers are per table and multiply by the tables running at once, so two
// tables at 32 fills the machine's 64 cores exactly. Their test moved one
// table, where the two settings collapse into one; ours move two and eight, so
// the product is what has to be held.
var Options = map[string]any{
	"doInitialCopy":               true,
	"initialCopyOnly":             true,
	"snapshotMaxParallelWorkers":  32,
	"snapshotNumTablesInParallel": 2,
	"snapshotNumRowsPerPartition": 750000,
}

// PeerDB runs the stack and drives one mirror through it.
type PeerDB struct {
	route      string
	containers []tc.Container
	server     string // host address of the peerdb server's wire protocol
	catalogDSN string
	env        *harness.Env
	tables     []string
	mirror     string
}

// New returns the peerdb tool for a route.
func New(route string) *PeerDB { return &PeerDB{route: route} }

// Name identifies the tool.
func (p *PeerDB) Name() string { return "peerdb" }

// Image reports the images under test.
func (p *PeerDB) Image() string { return ServerImage + " + " + FlowWorkerImage }

// Config reports the mirror configuration.
func (p *PeerDB) Config() map[string]any { return Options }

// Routes lists the routes PeerDB's peers cover here. It writes to several
// warehouses, but postgres to postgres is the path with a native binary COPY
// on both ends and the one its own benchmarks report.
func (p *PeerDB) Routes() []string { return []string{"pg-pg"} }

// Setup starts the stack and registers both peers.
func (p *PeerDB) Setup(ctx context.Context, env *harness.Env, tables []string) error {
	p.env = env
	p.tables = tables
	p.mirror = "bench_" + strings.ReplaceAll(env.RunID, "-", "_")

	// PeerDB creates the destination tables but not the schema holding them.
	sink, err := env.Sink.Open()
	if err != nil {
		return fmt.Errorf("open sink: %w", err)
	}
	defer func() { _ = sink.Close() }()
	if _, err := sink.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS "+harness.Namespace); err != nil {
		return fmt.Errorf("create sink schema: %w", err)
	}

	if err := p.start(ctx, env); err != nil {
		return err
	}
	return p.createPeers(ctx, env)
}

// start brings up the stack on the run's network, in dependency order.
func (p *PeerDB) start(ctx context.Context, env *harness.Env) error {
	const (
		catalogAlias  = "peerdb-catalog"
		temporalAlias = "peerdb-temporal"
		flowAPIAlias  = "peerdb-flow-api"
	)
	catalogEnv := map[string]string{
		"PEERDB_CATALOG_HOST":     catalogAlias,
		"PEERDB_CATALOG_PORT":     "5432",
		"PEERDB_CATALOG_USER":     "postgres",
		"PEERDB_CATALOG_PASSWORD": "postgres",
		"PEERDB_CATALOG_DATABASE": "postgres",
	}
	workerEnv := map[string]string{
		"TEMPORAL_HOST_PORT":        temporalAlias + ":7233",
		"PEERDB_TEMPORAL_NAMESPACE": "default",
	}

	// The catalog holds PeerDB's own metadata and backs Temporal's store.
	catalog, err := p.run(ctx, env, catalogAlias, tc.ContainerRequest{
		Image:        CatalogImage,
		Cmd:          []string{"-c", "wal_level=logical"},
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(90 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("peerdb catalog: %w", err)
	}
	catalogHost, err := catalog.Host(ctx)
	if err != nil {
		return err
	}
	catalogPort, err := catalog.MappedPort(ctx, "5432")
	if err != nil {
		return err
	}
	p.catalogDSN = fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", catalogHost, catalogPort.Port())

	if _, err := p.run(ctx, env, temporalAlias, tc.ContainerRequest{
		Image: TemporalImage,
		Env: map[string]string{
			"DB":             "postgres12",
			"DB_PORT":        "5432",
			"POSTGRES_USER":  "postgres",
			"POSTGRES_PWD":   "postgres",
			"POSTGRES_SEEDS": catalogAlias,
		},
		WaitingFor: wait.ForListeningPort("7233/tcp").WithStartupTimeout(180 * time.Second),
	}); err != nil {
		return fmt.Errorf("peerdb temporal: %w", err)
	}

	// PeerDB tags its workflows with a MirrorName search attribute, which has
	// to exist on the namespace before a mirror starts.
	cli, err := p.run(ctx, env, "peerdb-temporal-cli", tc.ContainerRequest{
		Image:      TemporalCLIImage,
		Entrypoint: []string{"/bin/sh", "-c"},
		Cmd:        []string{"sleep infinity"},
		Env:        map[string]string{"TEMPORAL_ADDRESS": temporalAlias + ":7233"},
	})
	if err != nil {
		return fmt.Errorf("peerdb temporal cli: %w", err)
	}
	if err := searchAttribute(ctx, cli); err != nil {
		return err
	}

	for _, w := range []struct{ alias, image string }{
		{flowAPIAlias, FlowAPIImage},
		{"peerdb-flow-worker", FlowWorkerImage},
		{"peerdb-snapshot-worker", SnapshotWorkImage},
	} {
		req := tc.ContainerRequest{Image: w.image, Env: merge(catalogEnv, workerEnv)}
		if w.alias == flowAPIAlias {
			req.WaitingFor = wait.ForListeningPort("8112/tcp").WithStartupTimeout(120 * time.Second)
		}
		if _, err := p.run(ctx, env, w.alias, req); err != nil {
			return fmt.Errorf("peerdb %s: %w", w.alias, err)
		}
	}

	server, err := p.run(ctx, env, "peerdb-server", tc.ContainerRequest{
		Image:        ServerImage,
		ExposedPorts: []string{"9900/tcp"},
		Env: merge(catalogEnv, map[string]string{
			"PEERDB_PASSWORD":            "peerdb",
			"PEERDB_FLOW_SERVER_ADDRESS": "grpc://" + flowAPIAlias + ":8112",
		}),
		WaitingFor: wait.ForListeningPort("9900/tcp").WithStartupTimeout(120 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("peerdb server: %w", err)
	}
	host, err := server.Host(ctx)
	if err != nil {
		return err
	}
	port, err := server.MappedPort(ctx, "9900")
	if err != nil {
		return err
	}
	p.server = fmt.Sprintf("postgres://peerdb:peerdb@%s:%s/peerdb", host, port.Port())
	return nil
}

// run starts one container on the run's network and records it for teardown.
func (p *PeerDB) run(ctx context.Context, env *harness.Env, alias string, req tc.ContainerRequest) (tc.Container, error) {
	req.Labels = map[string]string{harness.LabelRun: env.RunID, harness.LabelRole: alias}
	req.Networks = []string{env.Net.Name}
	req.NetworkAliases = map[string][]string{env.Net.Name: {alias}}
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	p.containers = append(p.containers, c)
	return c, nil
}

// searchAttribute registers MirrorName, tolerating one that already exists.
// Temporal's auto-setup opens its port before it registers the default
// namespace, so this retries rather than trusting the port: the first attempts
// fail with "Namespace default is not found".
func searchAttribute(ctx context.Context, cli tc.Container) error {
	cmd := []string{"temporal", "operator", "search-attribute", "create",
		"--name", "MirrorName", "--type", "Text", "--namespace", "default"}
	deadline := time.Now().Add(3 * time.Minute)
	for {
		code, r, err := cli.Exec(ctx, cmd, tcexec.Multiplexed())
		if err != nil {
			return fmt.Errorf("create search attribute: %w", err)
		}
		out, _ := io.ReadAll(r)
		if code == 0 || strings.Contains(string(out), "already exists") {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create search attribute exited %d:\n%s", code, out)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// createPeers registers the source and destination with the server.
func (p *PeerDB) createPeers(ctx context.Context, env *harness.Env) error {
	conn, err := pgx.Connect(ctx, p.server)
	if err != nil {
		return fmt.Errorf("connect peerdb server: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	for _, peer := range []struct {
		name string
		db   *harness.DB
	}{{"bench_source", env.Source}, {"bench_sink", env.Sink}} {
		stmt, err := createPeer(peer.name, peer.db)
		if err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create peer %s: %w", peer.name, err)
		}
	}
	return nil
}

// createPeer renders a CREATE PEER for a postgres database.
func createPeer(name string, db *harness.DB) (string, error) {
	if db.Engine != harness.Postgres {
		return "", fmt.Errorf("no peerdb peer for engine %v", db.Engine)
	}
	u, err := url.Parse(db.InternalDSN)
	if err != nil {
		return "", err
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	pass, _ := u.User.Password()
	return fmt.Sprintf(`CREATE PEER %s FROM POSTGRES WITH (
 host = '%s', port = %s, user = '%s', password = '%s', database = '%s')`,
		name, u.Hostname(), port, u.User.Username(), pass,
		strings.TrimPrefix(u.Path, "/")), nil
}

// Run creates the mirror and blocks until PeerDB reports that its initial-only
// snapshot completed. CREATE MIRROR returns once the workflow is accepted.
func (p *PeerDB) Run(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, p.server)
	if err != nil {
		return fmt.Errorf("connect peerdb server: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	mapping := make([]string, 0, len(p.tables))
	for _, t := range p.tables {
		mapping = append(mapping, fmt.Sprintf("%s.%s:%s.%s",
			harness.Namespace, t, harness.Namespace, t))
	}
	stmt := fmt.Sprintf(`CREATE MIRROR %s FROM bench_source TO bench_sink
 WITH TABLE MAPPING (%s)
 WITH (do_initial_copy = true,
 initial_copy_only = true,
 snapshot_num_rows_per_partition = %d,
 snapshot_max_parallel_workers = %d,
 snapshot_num_tables_in_parallel = %d)`,
		p.mirror, strings.Join(mapping, ", "),
		Options["snapshotNumRowsPerPartition"],
		Options["snapshotMaxParallelWorkers"],
		Options["snapshotNumTablesInParallel"])
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create mirror: %w", err)
	}

	catalog, err := pgx.Connect(ctx, p.catalogDSN)
	if err != nil {
		return fmt.Errorf("connect peerdb catalog: %w", err)
	}
	defer func() { _ = catalog.Close(ctx) }()

	for {
		var status int
		err := catalog.QueryRow(ctx, "SELECT status FROM flows WHERE name = $1", p.mirror).Scan(&status)
		if err != nil && err != pgx.ErrNoRows {
			return fmt.Errorf("read peerdb mirror status: %w", err)
		}
		if err == nil {
			switch status {
			case 8: // STATUS_COMPLETED: initial-only snapshot finished.
				return nil
			case 10: // STATUS_FAILED
				return fmt.Errorf("peerdb mirror failed")
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("peerdb mirror did not finish its snapshot: %w", ctx.Err())
		case <-time.After(pollInterval):
		}
	}
}

// Teardown drops the mirror, then removes the stack newest first so dependents
// stop before the catalog they write to. The mirror has to go first and it has
// to go at all: it leaves a replication slot and a publication on the source,
// and an inactive slot pins WAL there for every later run to carry.
func (p *PeerDB) Teardown(ctx context.Context) {
	if p.server != "" && p.mirror != "" {
		if conn, err := pgx.Connect(ctx, p.server); err == nil {
			if _, err := conn.Exec(ctx, "DROP MIRROR IF EXISTS "+p.mirror); err != nil {
				log.Printf("peerdb teardown: drop mirror %s: %v", p.mirror, err)
			}
			_ = conn.Close(ctx)
		} else {
			log.Printf("peerdb teardown: connect to server: %v", err)
		}
	}
	for i := len(p.containers) - 1; i >= 0; i-- {
		_ = p.containers[i].Terminate(ctx)
	}
	p.containers = nil
	// DROP MIRROR is asynchronous. Stop the workers first so their replication
	// connections release the slot, then clean up with a fresh bounded context;
	// the container teardown context may already be nearly exhausted.
	if err := p.dropBenchmarkReplicationArtifacts(context.Background()); err != nil {
		log.Printf("peerdb teardown: replication-artifact cleanup: %v", err)
	}
}

// dropBenchmarkReplicationArtifacts closes the gap between DROP MIRROR
// returning and PeerDB's asynchronous workflow cleanup. Only inactive slots
// created by this benchmark are removed; an active slot is never interrupted.
// The publication is database-wide, so dropping the bench schema does not
// remove it and later pg_dump runs would otherwise carry it to the sink.
func (p *PeerDB) dropBenchmarkReplicationArtifacts(ctx context.Context) error {
	if p.env == nil || p.env.Source == nil {
		return nil
	}
	db, err := p.env.Source.Open()
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer func() { _ = db.Close() }()

	cleanupCtx, cancel := context.WithTimeout(ctx, slotCleanupTimeout)
	defer cancel()
	for {
		rows, err := db.QueryContext(cleanupCtx, `SELECT slot_name, active
FROM pg_replication_slots
WHERE slot_name LIKE 'peerflow_slot_bench_bench\_%' ESCAPE '\'`)
		if err != nil {
			return err
		}
		var active bool
		var inactive []string
		for rows.Next() {
			var name string
			var isActive bool
			if err := rows.Scan(&name, &isActive); err != nil {
				_ = rows.Close()
				return err
			}
			if isActive {
				active = true
			} else {
				inactive = append(inactive, name)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, name := range inactive {
			if _, err := db.ExecContext(cleanupCtx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
				return fmt.Errorf("drop %s: %w", name, err)
			}
		}
		if !active {
			publication := pgx.Identifier{"peerflow_pub_" + p.mirror}.Sanitize()
			if _, err := db.ExecContext(cleanupCtx, "DROP PUBLICATION IF EXISTS "+publication); err != nil {
				return fmt.Errorf("drop %s: %w", publication, err)
			}
			return nil
		}
		select {
		case <-cleanupCtx.Done():
			return cleanupCtx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// merge combines env maps, later keys winning.
func merge(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}
