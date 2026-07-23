// Package sut defines the contract a system under test implements and the
// factory that builds one by name.
package sut

import (
	"context"

	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/sut/airbyte"
	"github.com/galaxy-io/benchmarks/data-movement/sut/dlt"
	"github.com/galaxy-io/benchmarks/data-movement/sut/filament"
	"github.com/galaxy-io/benchmarks/data-movement/sut/ingestr"
	"github.com/galaxy-io/benchmarks/data-movement/sut/native"
)

// SUT is one system under test. Setup is untimed preparation; Run is the
// timed window; Teardown releases whatever Setup started.
type SUT interface {
	Name() string
	Image() string
	Config() map[string]any
	Routes() []string
	Setup(ctx context.Context, env *harness.Env, tables []string) error
	Run(ctx context.Context) error
	Teardown(ctx context.Context)
}

// Names lists every SUT, in display order.
var Names = []string{"filament", "ingestr", "dlt", "airbyte", "native"}

// New builds the named SUT for a route, or nil for an unknown name.
func New(name, route string) SUT {
	switch name {
	case "filament":
		return filament.New()
	case "ingestr":
		return ingestr.New()
	case "dlt":
		return dlt.New()
	case "airbyte":
		return airbyte.New(route)
	case "native":
		return native.New(route)
	}
	return nil
}
