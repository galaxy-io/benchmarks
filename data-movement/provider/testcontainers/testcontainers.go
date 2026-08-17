// Package testcontainers holds the docker plumbing each engine's container
// provider needs.
package testcontainers

import (
	"context"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	tc "github.com/testcontainers/testcontainers-go"
)

// Start runs req on the run's network under alias, labelled for the sampler.
func Start(ctx context.Context, spec harness.ProvisionSpec, alias string, req tc.ContainerRequest) (tc.Container, error) {
	req.Labels = map[string]string{harness.LabelRun: spec.RunID, harness.LabelRole: alias}
	req.Networks = []string{spec.Net.Name}
	req.NetworkAliases = map[string][]string{spec.Net.Name: {alias}}
	return tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
}

// HostPort resolves the host-reachable address of a container port.
func HostPort(ctx context.Context, c tc.Container, port string) (string, string, error) {
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

// Terminate adapts a container's teardown to the DB's Close.
func Terminate(c tc.Container) func(context.Context) error {
	return func(ctx context.Context) error { return c.Terminate(ctx) }
}
