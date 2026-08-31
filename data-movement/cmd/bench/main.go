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
	reuse  bool
	// manifests is keyed by engine and DSN. Once a source is seeded, every SUT
	// receives these counts without issuing an untimed COUNT(*) against it.
	manifests map[string][]datasets.Table
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
func (s *seed) seed(ctx context.Context, db *harness.DB) ([]datasets.Table, error) {
	key := db.Engine.Name() + "|" + db.DSN
	// A full load only reads the source, so a seed already in place serves
	// every SUT and route in a sweep. The label has to match: a seed built
	// with other parameters is a different dataset wearing the same tables.
	if s.reuse {
		if tables, ok := s.manifests[key]; ok {
			return slices.Clone(tables), nil
		}
		if got := datasets.LoadedSeed(ctx, db); got == s.label() {
			log.Printf("reusing %s already seeded on the source", got)
			tables, err := datasets.CountSeeded(ctx, db, s.tableNames())
			if err != nil {
				return nil, err
			}
			s.manifests[key] = slices.Clone(tables)
			return tables, nil
		}
		// A crashed or interrupted cohort can leave a different seed behind.
		// Reset it once here; per-repetition provisioning deliberately preserves
		// the namespace when reuse is enabled.
		if err := harness.DropNamespace(ctx, db); err != nil {
			return nil, fmt.Errorf("reset reusable source: %w", err)
		}
	}
	var tables []datasets.Table
	var err error
	switch s.name {
	case "tpch":
		tables, err = datasets.SeedTPCH(ctx, db, s.sf)
	case "taxi":
		tables, err = datasets.SeedTaxi(ctx, db, s.months)
	default:
		return nil, fmt.Errorf("unknown dataset %q", s.name)
	}
	if err != nil {
		return nil, err
	}
	if s.reuse {
		if err := datasets.MarkSeed(ctx, db, s.label()); err != nil {
			return nil, err
		}
		s.manifests[key] = slices.Clone(tables)
	}
	return tables, nil
}

// tableNames lists the tables this dataset seeds.
func (s seed) tableNames() []string {
	switch s.name {
	case "tpch":
		return datasets.TPCHTableNames()
	case "taxi":
		return datasets.TaxiTableNames()
	}
	return nil
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

// parseSUTs resolves -sut into the tools to run: "all", or a comma-separated
// list, so one invocation can compare a few tools over a shared seed rather
// than reseeding once per tool.
func parseSUTs(value string) ([]string, error) {
	if value == "all" {
		return sut.Names, nil
	}
	var out []string
	for _, name := range strings.Split(value, ",") {
		name = strings.TrimSpace(name)
		if !slices.Contains(sut.Names, name) {
			return nil, fmt.Errorf("unknown sut %q (known: all, %s)", name, strings.Join(sut.Names, ", "))
		}
		if slices.Contains(out, name) {
			return nil, fmt.Errorf("sut %q listed twice", name)
		}
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no sut selected")
	}
	return out, nil
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
	reuseSeed := fs.Bool("reuse-seed", false, "seed the remote source once for the sweep, wiping it at the end")
	keepSeed := fs.Bool("keep-seed", false, "leave the reusable seed on the source so the next invocation reuses it")
	coldRDS := fs.Bool("cold-rds", false, "reboot remote SQL endpoints before every timed repetition")
	rdsMetrics := fs.Bool("rds-metrics", true, "capture RDS CloudWatch and database health metrics when RDS IDs are present")
	rdsSettle := fs.Duration("rds-settle", 30*time.Second, "quiet period after RDS recovery and SUT setup")
	quiesce := fs.Duration("quiesce-timeout", 5*time.Minute, "how long to wait for autovacuum to finish on an endpoint before a timed repetition")
	sf := fs.Float64("sf", 0.01, "TPC-H scale factor (tpch dataset)")
	months := fs.Int("months", 1, "months of history back from 2024-12 to seed, 192 = all (taxi dataset)")
	reps := fs.Int("reps", 1, "repetitions, each with fresh SUT containers and sink")
	out := fs.String("out", "results", "directory for result JSON")
	timeout := fs.Duration("timeout", time.Hour, "timeout for each repetition")
	topology := fs.String("topology", topologies[0], "database placement: local containers, remote DSNs, or exactly one of each (hybrid)")
	cohort := fs.String("cohort", time.Now().UTC().Format("20060102T150405Z"), "result cohort shared by every combination in this invocation")
	machine := fs.String("machine", os.Getenv("BENCH_MACHINE"), "machine label recorded in results, for example c7i.16xlarge")
	day := fs.String("date", "", "UTC date directory for results as YYYY-MM-DD, so a session of several invocations files together; defaults to the day this invocation starts")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, c := range []struct {
		name, val string
		known     []string
	}{
		{"scenario", *scenario, scenarios},
		{"route", *route, append([]string{"all"}, routes...)},
		{"dataset", *dataset, datanames},
		{"topology", *topology, topologies},
	} {
		if !slices.Contains(c.known, c.val) {
			return fmt.Errorf("unknown %s %q (known: %s)", c.name, c.val, strings.Join(c.known, ", "))
		}
	}
	sutList, err := parseSUTs(*sutName)
	if err != nil {
		return err
	}
	if *reps < 1 {
		return fmt.Errorf("reps must be at least 1, got %d", *reps)
	}
	if *timeout <= 0 {
		return fmt.Errorf("timeout must be positive, got %s", *timeout)
	}
	if *rdsSettle < 0 {
		return fmt.Errorf("rds-settle cannot be negative, got %s", *rdsSettle)
	}
	if *quiesce <= 0 {
		return fmt.Errorf("quiesce-timeout must be positive, got %s", *quiesce)
	}
	if *dataset == "tpch" && *sf <= 0 {
		return fmt.Errorf("TPC-H scale factor must be positive, got %v", *sf)
	}
	if *keepSeed && !*reuseSeed {
		return fmt.Errorf("-keep-seed needs -reuse-seed; without it every repetition seeds its own source")
	}
	if *day != "" {
		if _, err := time.Parse("2006-01-02", *day); err != nil {
			return fmt.Errorf("-date must be YYYY-MM-DD, got %q", *day)
		}
	}

	ctx := context.Background()

	// One named SUT on one named route is an exact request, so an unsupported
	// pair is an error rather than an empty run. Any wider selection describes a
	// set, and a member that cannot run the route is simply not in it.
	exact := len(sutList) == 1 && *route != "all"
	routeList := []string{*route}
	if *route == "all" {
		routeList = routes
	}
	if err := validateTopologyDSNs(routeList, *topology); err != nil {
		return err
	}
	if *coldRDS {
		if err := validateRDSIdentifiers(routeList, *topology); err != nil {
			return err
		}
	}

	var rdsControl *harness.RDSControl
	if *topology != "local" && (*coldRDS || *rdsMetrics) && hasRDSIdentifiers(routeList) {
		rdsControl, err = harness.NewRDSControl(ctx)
		if err != nil {
			return err
		}
	}
	runCfg := runConfig{
		topology: *topology, coldRDS: *coldRDS, rdsMetrics: *rdsMetrics,
		rdsSettle: *rdsSettle, rds: rdsControl, quiesce: *quiesce,
	}

	sd := &seed{name: *dataset, sf: *sf, months: *months, reuse: *reuseSeed, manifests: map[string][]datasets.Table{}}
	var combos []*benchmarkCombo
	for _, rt := range routeList {
		for _, sn := range sutList {
			meta := sut.New(sn, rt)
			if !slices.Contains(meta.Routes(), rt) {
				if exact {
					return fmt.Errorf("sut %q does not run route %q (runs: %s)",
						sn, rt, strings.Join(meta.Routes(), ", "))
				}
				log.Printf("skip %s %s: route not supported", sn, rt)
				continue
			}
			combo, err := newBenchmarkCombo(*scenario, rt, sn, sd, *topology, *cohort, *machine, *day)
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
	if err := prepareSeeds(ctx, routeList, *topology, sd); err != nil {
		return err
	}
	// A lone combination has nothing to survive its failure, so it reports the
	// error directly. Anything wider carries on and names what failed at the end.
	if err := runCombos(ctx, combos, sd, *reps, *out, runCfg, *timeout, len(combos) > 1); err != nil {
		return err
	}
	// The sweep is over, so the seed it shared has no next reader unless the
	// caller says another invocation wants it. Leaving it otherwise would
	// strand the dataset on the source and sit beside the next one, which
	// loads its own tables without touching these.
	if *keepSeed {
		log.Printf("keeping the %s seed on the source", sd.label())
		return nil
	}
	return wipeSeeds(ctx, routeList, *topology, sd)
}

type benchmarkCombo struct {
	sutName string
	route   string
	result  *harness.Result
	err     error
}

type runConfig struct {
	topology   string
	coldRDS    bool
	rdsMetrics bool
	rdsSettle  time.Duration
	quiesce    time.Duration
	rds        *harness.RDSControl
}

func newBenchmarkCombo(scenario, route, sutName string, sd *seed, topology, cohort, machine, day string) (*benchmarkCombo, error) {
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
		Day:          day,
		Machine:      machine,
		Verification: "row-count",
	}}, nil
}

// runCombos rotates the first combination each repetition. A remote sweep
// therefore does not give one SUT every cold run and another every warm run.
// Each combination writes its own result the moment its last repetition lands,
// so a failure later in the sweep cannot discard finished work.
func runCombos(ctx context.Context, combos []*benchmarkCombo, sd *seed, reps int, out string, cfg runConfig, timeout time.Duration, tolerate bool) error {
	for i := range reps {
		for offset := range len(combos) {
			combo := combos[(i+offset)%len(combos)]
			if combo.err != nil {
				continue
			}
			log.Printf("run %s %s %s, rep %d/%d", combo.sutName, combo.result.Scenario, combo.route, i+1, reps)
			repCtx, cancel := context.WithTimeout(ctx, timeout)
			rep, rows, err := runOnce(repCtx, sut.New(combo.sutName, combo.route), combo.route, sd, cfg)
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
				if i == reps-1 {
					writeCombo(out, combo)
				}
			}
			if combo.err != nil {
				if !tolerate {
					return combo.err
				}
				log.Printf("FAIL %s %s: %v", combo.sutName, combo.route, combo.err)
			}
		}
	}

	var failed []string
	for _, combo := range combos {
		if combo.err != nil {
			failed = append(failed, combo.sutName+"/"+combo.route)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("failed: %s", strings.Join(failed, ", "))
	}
	return nil
}

// writeCombo aggregates a finished combination and writes its result, recording
// any failure on the combination itself.
func writeCombo(out string, combo *benchmarkCombo) {
	combo.result.Aggregate()
	if !combo.result.ParityPass {
		combo.err = fmt.Errorf("parity failed; invalid results were not written")
		return
	}
	path, err := harness.WriteResult(out, combo.result)
	if err != nil {
		combo.err = err
		return
	}
	printResult(combo.result, path)
}

// validateTopologyDSNs checks every selected route against the topology before
// anything is provisioned. A sweep crosses both engines, so it names the exact
// variable a route is missing rather than falling back to a container.
// prepareSeeds loads the shared seed into every remote source before any
// repetition starts, so rep 1 no longer carries the load and its vacuum.
func prepareSeeds(ctx context.Context, routeList []string, topology string, sd *seed) error {
	if !sd.reuse || topology == "local" {
		return nil
	}
	seen := map[string]bool{}
	for _, route := range routeList {
		srcEngine, _, err := harness.ParseRoute(route)
		if err != nil {
			return err
		}
		dsn := remoteDSN("source", srcEngine)
		if dsn == "" || seen[srcEngine.Name()] {
			continue
		}
		seen[srcEngine.Name()] = true

		p, err := provider.Remote(srcEngine, dsn)
		if err != nil {
			return err
		}
		db, err := p.Provision(ctx, harness.ProvisionSpec{Engine: srcEngine, Role: "source"})
		if err != nil {
			return err
		}
		log.Printf("seeding %s on the %s source", sd.label(), srcEngine.Name())
		started := time.Now()
		tables, err := sd.seed(ctx, db)
		if err != nil {
			return fmt.Errorf("seed %s source: %w", srcEngine.Name(), err)
		}
		var rows int64
		for _, t := range tables {
			rows += t.Rows
		}
		log.Printf("seeded %s on the %s source: %d rows in %.1fs", sd.label(), srcEngine.Name(), rows, time.Since(started).Seconds())
	}
	return nil
}

// wipeSeeds drops the shared seed from every remote source the sweep read, so
// the next dataset starts on a clean namespace rather than beside this one. A
// local source is a container that has already been thrown away.
func wipeSeeds(ctx context.Context, routeList []string, topology string, sd *seed) error {
	if !sd.reuse || topology == "local" {
		return nil
	}
	seen := map[string]bool{}
	for _, route := range routeList {
		srcEngine, _, err := harness.ParseRoute(route)
		if err != nil {
			return err
		}
		dsn := remoteDSN("source", srcEngine)
		if dsn == "" || seen[srcEngine.Name()] {
			continue
		}
		seen[srcEngine.Name()] = true

		p, err := provider.Remote(srcEngine, dsn)
		if err != nil {
			return err
		}
		db, err := p.Provision(ctx, harness.ProvisionSpec{Engine: srcEngine, Role: "source"})
		if err != nil {
			return err
		}
		if err := harness.DropNamespace(ctx, db); err != nil {
			return fmt.Errorf("wipe %s seed: %w", srcEngine.Name(), err)
		}
		log.Printf("wiped %s seed from the %s source", sd.label(), srcEngine.Name())
	}
	return nil
}

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

func hasRDSIdentifiers(routeList []string) bool {
	for _, route := range routeList {
		ends, err := routeEnds(route)
		if err != nil {
			continue
		}
		for _, end := range ends {
			if end.Engine == harness.Iceberg {
				continue
			}
			name := fmt.Sprintf("BENCH_%s_%s_RDS_ID", strings.ToUpper(end.Role), strings.ToUpper(end.Engine.Name()))
			if os.Getenv(name) != "" {
				return true
			}
		}
	}
	return false
}

func validateRDSIdentifiers(routeList []string, topology string) error {
	if topology == "local" {
		return nil
	}
	for _, route := range routeList {
		ends, err := routeEnds(route)
		if err != nil {
			return err
		}
		for _, end := range ends {
			if end.Engine == harness.Iceberg || remoteDSN(end.Role, end.Engine) == "" {
				continue
			}
			name := fmt.Sprintf("BENCH_%s_%s_RDS_ID", strings.ToUpper(end.Role), strings.ToUpper(end.Engine.Name()))
			if os.Getenv(name) == "" {
				return fmt.Errorf("-cold-rds requires %s for route %s", name, route)
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
func runOnce(ctx context.Context, s sut.SUT, route string, sd *seed, cfg runConfig) (harness.Rep, int64, error) {
	var rep harness.Rep

	srcEngine, sinkEngine, err := harness.ParseRoute(route)
	if err != nil {
		return rep, 0, err
	}
	source, err := resolveProvider(srcEngine, "source", cfg.topology)
	if err != nil {
		return rep, 0, err
	}
	sink, err := resolveProvider(sinkEngine, "sink", cfg.topology)
	if err != nil {
		return rep, 0, err
	}

	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	// Reusable remote sources retain the cohort seed. Local providers are fresh
	// regardless, and non-reuse runs retain the original reset-per-rep behavior.
	resetSource := !sd.reuse || remoteDSN("source", srcEngine) == "" || cfg.topology == "local"
	provisionStarted := time.Now()
	env, err := harness.NewEnv(ctx, route, runID, source, sink, resetSource)
	if err != nil {
		return rep, 0, err
	}
	provision := time.Since(provisionStarted)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		env.Terminate(cleanupCtx)
	}()

	seedStarted := time.Now()
	tables, err := sd.seed(ctx, env.Source)
	if err != nil {
		return rep, 0, fmt.Errorf("seed: %w", err)
	}
	seedDuration := time.Since(seedStarted)
	var totalRows int64
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
		totalRows += t.Rows
	}
	env.Expected = make(map[string]int64, len(tables))
	for _, t := range tables {
		env.Expected[t.Name] = t.Rows
	}

	rdsEndpoints := harness.RDSEndpoints(env)
	if cfg.coldRDS {
		if cfg.rds == nil || len(rdsEndpoints) == 0 {
			return rep, 0, fmt.Errorf("cold RDS requested but no controlled endpoints were found")
		}
		rebooted, err := cfg.rds.Reboot(ctx, rdsEndpoints)
		if err != nil {
			return rep, 0, err
		}
		rep.ColdStart = &harness.ColdStart{RebootSeconds: rebooted.Seconds(), SettleSeconds: cfg.rdsSettle.Seconds()}
		log.Printf("RDS endpoints recovered after %.1fs", rebooted.Seconds())
	}

	setupStart := time.Now()
	teardownDone := false
	defer func() {
		if teardownDone {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		s.Teardown(cleanupCtx)
	}()
	if err := s.Setup(ctx, env, names); err != nil {
		return rep, 0, err
	}
	setup := time.Since(setupStart)
	if cfg.coldRDS && cfg.rdsSettle > 0 {
		select {
		case <-ctx.Done():
			return rep, 0, ctx.Err()
		case <-time.After(cfg.rdsSettle):
		}
	}

	for _, db := range []*harness.DB{env.Source, env.Sink} {
		waited, err := harness.WaitQuiescent(ctx, db, cfg.quiesce)
		if err != nil {
			return rep, 0, err
		}
		if waited > 5*time.Second {
			log.Printf("%s settled after %.1fs of autovacuum", db.Role, waited.Seconds())
		}
	}

	health := endpointHealth(env, rdsEndpoints)
	for i := range health {
		health[i].Before = snapshotEndpoint(ctx, env, health[i].Role)
	}

	sampler := startSampler(ctx, runID)
	started := time.Now().UTC()
	if err := s.Run(ctx); err != nil {
		_ = stopSampler(sampler)
		return rep, 0, err
	}
	ended := time.Now().UTC()
	wall := ended.Sub(started)
	resources := stopSampler(sampler)
	for i := range health {
		health[i].After = snapshotEndpoint(ctx, env, health[i].Role)
	}

	// Teardown is explicit so leaked replication slots are visible in a second
	// source snapshot before the next repetition begins.
	teardownStarted := time.Now()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	s.Teardown(cleanupCtx)
	cancel()
	teardownDuration := time.Since(teardownStarted)
	teardownDone = true
	for i := range health {
		health[i].AfterTeardown = snapshotEndpoint(ctx, env, health[i].Role)
	}

	validationStarted := time.Now()
	parity, pass, err := harness.CheckParity(ctx, env.Sink, names, env.Expected)
	if err != nil {
		return rep, 0, err
	}
	validationDuration := time.Since(validationStarted)
	rep = harness.Rep{
		StartedAt:         started,
		EndedAt:           ended,
		ColdStart:         rep.ColdStart,
		ProvisionSeconds:  provision.Seconds(),
		SeedSeconds:       seedDuration.Seconds(),
		SetupSeconds:      setup.Seconds(),
		WallSeconds:       wall.Seconds(),
		TeardownSeconds:   teardownDuration.Seconds(),
		ValidationSeconds: validationDuration.Seconds(),
		RowsPerSec:        float64(totalRows) / wall.Seconds(),
		Resources:         resources,
		Parity:            parity,
		ParityPass:        pass,
		Databases:         health,
	}
	if cfg.rdsMetrics && cfg.rds != nil {
		for i := range rep.Databases {
			if rep.Databases[i].RDSIdentifier == "" {
				continue
			}
			metrics, err := cfg.rds.Metrics(ctx, rep.Databases[i].RDSIdentifier, started, ended)
			if err != nil {
				rep.Databases[i].MetricsError = err.Error()
				continue
			}
			rep.Databases[i].CloudWatch = metrics
		}
	}
	return rep, totalRows, nil
}

func endpointHealth(env *harness.Env, endpoints []harness.RDSEndpoint) []harness.EndpointHealth {
	ids := map[string]string{}
	for _, endpoint := range endpoints {
		ids[endpoint.Role] = endpoint.Identifier
	}
	out := []harness.EndpointHealth{{Role: "source", Engine: env.Source.Engine.Name(), RDSIdentifier: ids["source"]}}
	if env.Sink.Engine != harness.Iceberg {
		out = append(out, harness.EndpointHealth{Role: "sink", Engine: env.Sink.Engine.Name(), RDSIdentifier: ids["sink"]})
	}
	return out
}

func snapshotEndpoint(ctx context.Context, env *harness.Env, role string) *harness.DatabaseSnapshot {
	if role == "source" {
		return harness.SnapshotDatabase(ctx, env.Source)
	}
	return harness.SnapshotDatabase(ctx, env.Sink)
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
