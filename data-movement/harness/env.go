// Package harness provisions the databases a benchmark runs against.
package harness

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// DB is one provisioned database, addressable from the host and from the
// network. Close releases whatever the provider started, and does nothing for
// a database that outlives the run.
type DB struct {
	Engine      Engine
	Role        string
	DSN         string            // reachable from the host
	InternalDSN string            // reachable from the suts
	Props       map[string]string // engine-specific addressing beyond the DSNs
	Close       func(context.Context) error
}

// Open opens a database/sql handle to the database from the host.
func (d *DB) Open() (*sql.DB, error) {
	if d == nil {
		return nil, fmt.Errorf("open database: nil DB")
	}
	if d.Engine == nil {
		return nil, fmt.Errorf("open database %q: no engine", d.Role)
	}
	return d.Engine.Open(d)
}

// ProvisionSpec is one request for an addressed database: the engine, the role
// it plays in the run, and the docker context a container-backed provider
// needs. Providers that address something already running ignore the rest.
type ProvisionSpec struct {
	Engine Engine
	Role   string
	RunID  string
	Net    *tc.DockerNetwork
	// Reset requests that a persistent provider empty its benchmark namespace.
	// Container providers are already fresh and ignore it.
	Reset bool
}

// Provider hands back an addressed database.
type Provider interface {
	Provision(ctx context.Context, spec ProvisionSpec) (*DB, error)
}

// Env contains one run's network, seeded source, empty sink, and sampler label.
type Env struct {
	RunID  string
	Net    *tc.DockerNetwork
	Source *DB
	Sink   *DB
	// Expected carries the seed manifest into adapters so setup never has to
	// scan the source merely to discover row counts.
	Expected map[string]int64
}

// NewEnv starts the network and provisions both databases for a route. The SUT
// containers use this network even when one or both databases are remote.
func NewEnv(ctx context.Context, route, runID string, source, sink Provider, resetSource bool) (*Env, error) {
	srcEngine, sinkEngine, err := ParseRoute(route)
	if err != nil {
		return nil, err
	}
	net, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("network: %w", err)
	}
	env := &Env{RunID: runID, Net: net}

	env.Source, err = source.Provision(ctx, ProvisionSpec{
		Engine: srcEngine, Role: "source", RunID: runID, Net: net, Reset: resetSource,
	})
	if err != nil {
		env.terminateAfterProvisionFailure()
		return nil, fmt.Errorf("source: %w", err)
	}
	env.Sink, err = sink.Provision(ctx, ProvisionSpec{
		Engine: sinkEngine, Role: "sink", RunID: runID, Net: net, Reset: true,
	})
	if err != nil {
		env.terminateAfterProvisionFailure()
		return nil, fmt.Errorf("sink: %w", err)
	}
	return env, nil
}

// terminateAfterProvisionFailure deliberately does not reuse the provisioning
// context: that context is commonly already canceled when startup fails.
func (e *Env) terminateAfterProvisionFailure() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e.Terminate(ctx)
}

// Terminate releases everything the env provisioned.
func (e *Env) Terminate(ctx context.Context) {
	for _, db := range []*DB{e.Source, e.Sink} {
		if db == nil || db.Close == nil {
			continue
		}
		_ = db.Close(ctx)
	}
	if e.Net != nil {
		_ = e.Net.Remove(ctx)
	}
}
