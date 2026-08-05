// Package harness provisions the containers a benchmark runs against.
package harness

import (
	"context"
	"database/sql"
	"fmt"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
)

// DB is one provisioned database, addressable from the host and from the network.
type DB struct {
	Container   tc.Container
	Aux         []tc.Container // supporting containers the engine started, torn down with the main one
	Engine      Engine
	DSN         string            // reachable from the host
	InternalDSN string            // reachable from other containers on the network
	Props       map[string]string // engine-specific addressing beyond the DSNs, network-internal
}

// Open opens a database/sql handle to the database from the host.
func (d *DB) Open() (*sql.DB, error) { return d.Engine.Open(d) }

// Env is one run's world: a network, a seeded source, an empty sink; RunID labels containers for the sampler.
type Env struct {
	RunID  string
	Net    *tc.DockerNetwork
	Source *DB
	Sink   *DB
}

// NewEnv starts the network and both databases for a route.
func NewEnv(ctx context.Context, route, runID string) (*Env, error) {
	srcEngine, sinkEngine, err := ParseRoute(route)
	if err != nil {
		return nil, err
	}
	net, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("network: %w", err)
	}
	source, err := srcEngine.Start(ctx, net, "source", runID)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	sink, err := sinkEngine.Start(ctx, net, "sink", runID)
	if err != nil {
		return nil, fmt.Errorf("sink: %w", err)
	}
	return &Env{RunID: runID, Net: net, Source: source, Sink: sink}, nil
}

// hostPort resolves the host-reachable address of a container port.
func hostPort(ctx context.Context, c tc.Container, port string) (string, string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", "", err
	}
	mapped, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", "", err
	}
	return host, mapped.Port(), nil
}

// Terminate tears down everything the env started.
func (e *Env) Terminate(ctx context.Context) {
	for _, db := range []*DB{e.Source, e.Sink} {
		if db == nil {
			continue
		}
		_ = db.Container.Terminate(ctx)
		for _, c := range db.Aux {
			_ = c.Terminate(ctx)
		}
	}
	if e.Net != nil {
		_ = e.Net.Remove(ctx)
	}
}
