// Command bench runs one benchmark and writes its result JSON.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/datasets"
	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/provider"
	"github.com/galaxy-io/benchmarks/data-movement/sut"
)

var (
	scenarios  = []string{"full-load"}
	routes     = []string{"pg-pg", "pg-mysql", "mysql-mysql", "mysql-pg", "pg-iceberg", "mysql-iceberg"}
	datanames  = []string{"tpch", "taxi"}
	topologies = []string{"local", "remote", "hybrid"}
)

// seed names a dataset and its size: sf sizes tpch, months sizes taxi.
type seed struct {
	name   string
	sf     float64
	months int
}

// label renders the dataset label recorded in results.
func (s seed) label() string {
	switch s.name {
	case "tpch":
		return fmt.Sprintf("tpch-sf%v", s.sf)
	case "taxi":
		return fmt.Sprintf("taxi-%dmo", s.months)
	}
	return "unknown-" + s.name
}

// seed loads the dataset into db and reports its tables.
func (s seed) seed(ctx context.Context, db *harness.DB) ([]datasets.Table, error) {
	switch s.name {
	case "tpch":
		return datasets.SeedTPCH(ctx, db, s.sf)
	case "taxi":
		return datasets.SeedTaxi(ctx, db, s.months)
	}
	return nil, fmt.Errorf("unknown dataset %q", s.name)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "list":
		fmt.Println("scenarios: " + strings.Join(scenarios, ", "))
		fmt.Println("routes:    " + strings.Join(routes, ", "))
		fmt.Println("suts:")
		for _, n := range sut.Names {
			fmt.Printf("  %-10s %s\n", n, strings.Join(sut.New(n, routes[0]).Routes(), ", "))
		}
	case "run":
		if err := run(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench list")
	fmt.Fprintln(os.Stderr, "       bench run [flags]  (bench run -h for flags)")
	os.Exit(2)
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	scenario := fs.String("scenario", scenarios[0], "scenario to run")
	route := fs.String("route", routes[0], "source-to-destination route, or all")
	sutName := fs.String("sut", sut.Names[0], "system under test, or all")
	dataset := fs.String("dataset", datanames[0], "dataset to seed")
	sf := fs.Float64("sf", 0.01, "TPC-H scale factor (tpch dataset)")
	months := fs.Int("months", 1, "months of history back from 2024-12 to seed, 192 = all (taxi dataset)")
	reps := fs.Int("reps", 1, "repetitions, each with fresh containers and seed")
	out := fs.String("out", "results", "directory for result JSON")
	timeout := fs.Duration("timeout", time.Hour, "timeout for each repetition")
	topology := fs.String("topology", topologies[0], "database placement: local containers, remote DSNs, or exactly one of each (hybrid)")
	cohort := fs.String("cohort", time.Now().UTC().Format("20060102T150405Z"), "result cohort shared by every combination in this invocation")
	machine := fs.String("machine", os.Getenv("BENCH_MACHINE"), "machine label recorded in results, for example c7i.16xlarge")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, c := range []struct {
		name, val string
		known     []string
	}{
		{"scenario", *scenario, scenarios},
		{"route", *route, append([]string{"all"}, routes...)},
		{"sut", *sutName, append([]string{"all"}, sut.Names...)},
		{"dataset", *dataset, datanames},
		{"topology", *topology, topologies},
	} {
		if !slices.Contains(c.known, c.val) {
			return fmt.Errorf("unknown %s %q (known: %s)", c.name, c.val, strings.Join(c.known, ", "))
		}
	}
	if *reps < 1 {
		return fmt.Errorf("reps must be at least 1, got %d", *reps)
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", *timeout)
	}
	if *dataset == "tpch" && *sf <= 0 {
		return fmt.Errorf("TPC-H scale factor must be positive, got %v", *sf)
	}

	ctx := context.Background()

	sweep := *sutName == "all" || *route == "all"
	sutList := []string{*sutName}
	if *sutName == "all" {
		sutList = sut.Names
	}
	routeList := []string{*route}
	if *route == "all" {
		routeList = routes
	}
	if err := validateTopologyDSNs(routeList, *topology); err != nil {
		return err
	}

	sd := seed{name: *dataset, sf: *sf, months: *months}
	var combos []*benchmarkCombo
	for _, rt := range routeList {
		for _, sn := range sutList {
			meta := sut.New(sn, rt)
			if !slices.Contains(meta.Routes(), rt) {
				if !sweep {
					return fmt.Errorf("sut %q does not run route %q (runs: %s)",
						sn, rt, strings.Join(meta.Routes(), ", "))
				}
				log.Printf("skip %s %s: route not supported", sn, rt)
				continue
			}
			combo, err := newBenchmarkCombo(*scenario, rt, sn, sd, *topology, *cohort, *machine)
			if err != nil {
				return fmt.Errorf("%s/%s: %w", sn, rt, err)
			}
			combos = append(combos, combo)
		}
	}
	if len(combos) == 0 {
		return fmt.Errorf("no supported SUT/route combinations selected")
	}
	// Fail before provisioning or seeding if this cohort would collide with an
	// immutable result. WriteResult retains O_EXCL as a final race guard.
	for _, combo := range combos {
		path := harness.ResultPath(*out, combo.result)
		if _, err := os.Stat(path); err == nil {
			return fmt.Errorf("result already exists at %s; choose a new -cohort", path)
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("check result path %s: %w", path, err)
		}
	}
	return runCombos(ctx, combos, sd, *reps, *out, *topology, *timeout, sweep)
}

type benchmarkCombo struct {
	sutName string
	route   string
	result  *harness.Result
	err     error
}

func newBenchmarkCombo(scenario, route, sutName string, sd seed, topology, cohort, machine string) (*benchmarkCombo, error) {
	if err := validateDistinctRemoteEndpoints(route, topology); err != nil {
		return nil, err
	}
	meta := sut.New(sutName, route)
	source, sink, actualTopology, err := resultEndpoints(route, topology)
	if err != nil {
		return nil, err
	}
	return &benchmarkCombo{sutName: sutName, route: route, result: &harness.Result{
		SUT:          meta.Name(),
		Scenario:     scenario,
		Route:        route,
		Dataset:      sd.label(),
		Image:        meta.Image(),
		Cohort:       cohort,
		Topology:     actualTopology,
		Source:       source,
		Sink:         sink,
		Native:       sutName == "native",
		Config:       meta.Config(),
		StartedAt:    time.Now(),
		Machine:      machine,
		Verification: "row-count",
	}}, nil
}

// runCombos rotates the first combination each repetition. A remote sweep
// therefore does not give one SUT every cold run and another every warm run.
func runCombos(ctx context.Context, combos []*benchmarkCombo, sd seed, reps int, out, topology string, timeout time.Duration, sweep bool) error {
	for i := range reps {
		for offset := range len(combos) {
			combo := combos[(i+offset)%len(combos)]
			if combo.err != nil {
				continue
			}
			log.Printf("run %s %s %s, rep %d/%d", combo.sutName, combo.result.Scenario, combo.route, i+1, reps)
			repCtx, cancel := context.WithTimeout(ctx, timeout)
			rep, rows, err := runOnce(repCtx, sut.New(combo.sutName, combo.route), combo.route, sd, topology)
			cancel()
			if err != nil {
				combo.err = fmt.Errorf("rep %d: %w", i+1, err)
			} else if combo.result.Rows != 0 && rows != combo.result.Rows {
				combo.err = fmt.Errorf("rep %d seeded %d rows; earlier reps seeded %d", i+1, rows, combo.result.Rows)
			} else {
				combo.result.Rows = rows
				combo.result.Reps = append(combo.result.Reps, rep)
				log.Printf("%s %s rep %d/%d: %.3fs (%.0f rows/s), parity pass=%v",
					combo.sutName, combo.route, i+1, reps, rep.WallSeconds, rep.RowsPerSec, rep.ParityPass)
			}
			if combo.err != nil {
				if !sweep {
					return combo.err
				}
				log.Printf("FAIL %s %s: %v", combo.sutName, combo.route, combo.err)
			}
		}
	}

	var failed []string
	for _, combo := range combos {
		if combo.err == nil {
			combo.result.Aggregate()
			if !combo.result.ParityPass {
				combo.err = fmt.Errorf("parity failed; invalid results were not written")
			}
		}
		if combo.err == nil {
			path, err := harness.WriteResult(out, combo.result)
			if err != nil {
				combo.err = err
			} else {
				printResult(combo.result, path)
			}
		}
		if combo.err != nil {
			if !sweep {
				return combo.err
			}
			failed = append(failed, combo.sutName+"/"+combo.route)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed: %s", strings.Join(failed, ", "))
	}
	return nil
}

// validateTopologyDSNs checks every selected route against the topology before
// anything is provisioned. A sweep crosses both engines, so it names the exact
// variable a route is missing rather than falling back to a container.
func validateTopologyDSNs(routeList []string, topology string) error {
	if topology == "local" {
		return nil
	}
	for _, route := range routeList {
		ends, err := routeEnds(route)
		if err != nil {
			return err
		}
		var missing []string
		remote := 0
		for _, end := range ends {
			if remoteDSN(end.Role, end.Engine) == "" {
				missing = append(missing, dsnVar(end.Role, end.Engine))
				continue
			}
			remote++
		}
		switch topology {
		case "remote":
			if len(missing) > 0 {
				return fmt.Errorf("-topology remote: route %s needs %s; use hybrid for one remote end",
					route, strings.Join(missing, " and "))
			}
		case "hybrid":
			if remote != 1 {
				return fmt.Errorf("-topology hybrid: route %s has %d remote ends, needs exactly one", route, remote)
			}
		}
	}
	return nil
}

func validateDistinctRemoteEndpoints(route, topology string) error {
	if topology != "remote" {
		return nil
	}
	srcEngine, sinkEngine, err := harness.ParseRoute(route)
	if err != nil {
		return err
	}
	// An Iceberg sink is an object-store/catalog target, not a second SQL
	// endpoint. The distinct-host rule applies to two remote databases.
	if sinkEngine == harness.Iceberg {
		return nil
	}
	source, err := canonicalEndpoint(remoteDSN("source", srcEngine), srcEngine)
	if err != nil {
		return fmt.Errorf("%s: %w", dsnVar("source", srcEngine), err)
	}
	sink, err := canonicalEndpoint(remoteDSN("sink", sinkEngine), sinkEngine)
	if err != nil {
		return fmt.Errorf("%s: %w", dsnVar("sink", sinkEngine), err)
	}
	if source == sink {
		return fmt.Errorf("remote source and sink DSNs must address distinct endpoints (both resolve to %s)", source)
	}
	return nil
}

func canonicalEndpoint(dsn string, engine harness.Engine) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return "", fmt.Errorf("missing host")
	}
	port := u.Port()
	if port == "" {
		switch engine {
		case harness.Postgres:
			port = "5432"
		case harness.MySQL:
			port = "3306"
		default:
			return "", fmt.Errorf("engine %s has no network endpoint", engine.Name())
		}
	}
	return host + ":" + port, nil
}

func resultEndpoints(route, requestedTopology string) (harness.Endpoint, harness.Endpoint, string, error) {
	srcEngine, sinkEngine, err := harness.ParseRoute(route)
	if err != nil {
		return harness.Endpoint{}, harness.Endpoint{}, "", err
	}
	endpoint := func(engine harness.Engine, role string) harness.Endpoint {
		providerName := "testcontainers"
		if requestedTopology != "local" && remoteDSN(role, engine) != "" {
			providerName = "remote"
		}
		return harness.Endpoint{
			Engine: engine.Name(), Provider: providerName,
			Label: os.Getenv(fmt.Sprintf("BENCH_%s_%s_LABEL",
				strings.ToUpper(role), strings.ToUpper(engine.Name()))),
		}
	}
	source := endpoint(srcEngine, "source")
	sink := endpoint(sinkEngine, "sink")
	actual := "local"
	if source.Provider == "remote" && sink.Provider == "remote" {
		actual = "remote"
	} else if source.Provider != sink.Provider {
		actual = "hybrid-source-" + source.Provider + "-sink-" + sink.Provider
	}
	return source, sink, actual, nil
}

// printResult renders the run summary as a table on stdout.
func printResult(r *harness.Result, path string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "\n%s  %s  %s  %s  (%d rows)\n\n", r.SUT, r.Scenario, r.Route, r.Dataset, r.Rows)
	fmt.Fprintln(w, "REP\tWALL\tROWS/S\tSETUP\tPARITY")
	for i, rep := range r.Reps {
		fmt.Fprintf(w, "%d\t%.3fs\t%.0f\t%.1fs\t%s\n",
			i+1, rep.WallSeconds, rep.RowsPerSec, rep.SetupSeconds, parityWord(rep.ParityPass))
	}
	fmt.Fprintf(w, "median\t%.3fs\t%.0f\t\t%s\n", r.MedianWallSeconds, r.MedianRowsPerSec, parityWord(r.ParityPass))
	_ = w.Flush()
	fmt.Println("\nresult:", path)
}

func parityWord(pass bool) string {
	if pass {
		return "pass"
	}
	return "FAIL"
}

// resolveProvider builds the provider for one end of a route. A remote
// topology addresses whatever BENCH_{SOURCE,SINK}_DSN names and leaves the
// rest local, which is how a route with only its source on rds runs.
func resolveProvider(engine harness.Engine, role, topology string) (harness.Provider, error) {
	if dsn := remoteDSN(role, engine); topology != "local" && dsn != "" {
		return provider.Remote(engine, dsn)
	}
	return provider.Docker(engine)
}

// dsnVar names the variable holding one end's remote address. It carries the
// engine as well as the role because a sweep crosses both engines on both
// ends, and one variable per role would hand a mysql route the postgres
// endpoint.
func dsnVar(role string, engine harness.Engine) string {
	return fmt.Sprintf("BENCH_%s_%s_DSN", strings.ToUpper(role), strings.ToUpper(engine.Name()))
}

// remoteDSN reads one end's remote address, empty when that end runs locally.
func remoteDSN(role string, engine harness.Engine) string {
	return os.Getenv(dsnVar(role, engine))
}

// routeEnds pairs each end of a route with the role it plays.
func routeEnds(route string) ([]struct {
	Role   string
	Engine harness.Engine
}, error) {
	srcEngine, sinkEngine, err := harness.ParseRoute(route)
	if err != nil {
		return nil, err
	}
	return []struct {
		Role   string
		Engine harness.Engine
	}{{"source", srcEngine}, {"sink", sinkEngine}}, nil
}

// runOnce provisions a fresh environment, seeds, runs the SUT, and checks parity.
func runOnce(ctx context.Context, s sut.SUT, route string, sd seed, topology string) (harness.Rep, int64, error) {
	var rep harness.Rep

	srcEngine, sinkEngine, err := harness.ParseRoute(route)
	if err != nil {
		return rep, 0, err
	}
	source, err := resolveProvider(srcEngine, "source", topology)
	if err != nil {
		return rep, 0, err
	}
	sink, err := resolveProvider(sinkEngine, "sink", topology)
	if err != nil {
		return rep, 0, err
	}

	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	env, err := harness.NewEnv(ctx, route, runID, source, sink)
	if err != nil {
		return rep, 0, err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		env.Terminate(cleanupCtx)
	}()

	tables, err := sd.seed(ctx, env.Source)
	if err != nil {
		return rep, 0, fmt.Errorf("seed: %w", err)
	}
	var totalRows int64
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
		totalRows += t.Rows
	}

	setupStart := time.Now()
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		s.Teardown(cleanupCtx)
	}()
	if err := s.Setup(ctx, env, names); err != nil {
		return rep, 0, err
	}
	setup := time.Since(setupStart)

	sampler := startSampler(ctx, runID)
	defer stopSampler(sampler)
	started := time.Now()
	if err := s.Run(ctx); err != nil {
		return rep, 0, err
	}
	wall := time.Since(started)
	resources := stopSampler(sampler)

	parity, pass, err := harness.CheckParity(ctx, env.Source, env.Sink, names)
	if err != nil {
		return rep, 0, err
	}
	rep = harness.Rep{
		SetupSeconds: setup.Seconds(),
		WallSeconds:  wall.Seconds(),
		RowsPerSec:   float64(totalRows) / wall.Seconds(),
		Resources:    resources,
		Parity:       parity,
		ParityPass:   pass,
	}
	return rep, totalRows, nil
}

// startSampler begins resource sampling; a sampler failure never fails a run.
func startSampler(ctx context.Context, runID string) *harness.Sampler {
	s, err := harness.StartSampler(ctx, runID, 100*time.Millisecond)
	if err != nil {
		log.Printf("sampler unavailable: %v", err)
		return nil
	}
	return s
}

func stopSampler(s *harness.Sampler) []harness.Usage {
	if s == nil {
		return nil
	}
	return s.Stop()
}
