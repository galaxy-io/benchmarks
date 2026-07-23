// Package harness provisions the containers a benchmark runs against.
package harness

import (
	"context"
	"fmt"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const PostgresImage = "postgres:16"

// DB is one provisioned database, addressable from the host and from the network.
type DB struct {
	Container   tc.Container
	DSN         string // reachable from the host
	InternalDSN string // reachable from other containers on the network
}

// Env is one run's world: a network, a seeded source, an empty sink; RunID labels containers for the sampler.
type Env struct {
	RunID  string
	Net    *tc.DockerNetwork
	Source *DB
	Sink   *DB
}

// NewEnv starts the network and both databases for a route.
func NewEnv(ctx context.Context, route, runID string) (*Env, error) {
	if route != "pg-pg" {
		return nil, fmt.Errorf("route %q not supported yet", route)
	}
	net, err := network.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("network: %w", err)
	}
	source, err := StartPostgres(ctx, net, "source", runID)
	if err != nil {
		return nil, fmt.Errorf("source: %w", err)
	}
	sink, err := StartPostgres(ctx, net, "sink", runID)
	if err != nil {
		return nil, fmt.Errorf("sink: %w", err)
	}
	return &Env{RunID: runID, Net: net, Source: source, Sink: sink}, nil
}

// StartPostgres runs a postgres container on net; the alias doubles as the sampler role.
func StartPostgres(ctx context.Context, net *tc.DockerNetwork, alias, runID string) (*DB, error) {
	req := tc.ContainerRequest{
		Image:        PostgresImage,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "bench",
			"POSTGRES_PASSWORD": "bench",
			"POSTGRES_DB":       "bench",
		},
		Labels:         map[string]string{LabelRun: runID, LabelRole: alias},
		Networks:       []string{net.Name},
		NetworkAliases: map[string][]string{net.Name: {alias}},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		return nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}
	return &DB{
		Container:   c,
		DSN:         fmt.Sprintf("postgresql://bench:bench@%s:%s/bench?sslmode=disable", host, port.Port()),
		InternalDSN: fmt.Sprintf("postgresql://bench:bench@%s:5432/bench?sslmode=disable", alias),
	}, nil
}

// Terminate tears down everything the env started.
func (e *Env) Terminate(ctx context.Context) {
	if e.Source != nil {
		_ = e.Source.Container.Terminate(ctx)
	}
	if e.Sink != nil {
		_ = e.Sink.Container.Terminate(ctx)
	}
	if e.Net != nil {
		_ = e.Net.Remove(ctx)
	}
}
