// Command bench runs one benchmark and writes its result JSON.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/galaxy-io/benchmarks/data-movement/datasets"
	"github.com/galaxy-io/benchmarks/data-movement/harness"
	"github.com/galaxy-io/benchmarks/data-movement/sut"
)

var (
	scenarios = []string{"full-load"}
	routes    = []string{"pg-pg"}
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "list":
		fmt.Println("scenarios: " + strings.Join(scenarios, ", "))
		fmt.Println("routes:    " + strings.Join(routes, ", "))
		fmt.Println("suts:      " + strings.Join(sut.Names, ", "))
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
	route := fs.String("route", routes[0], "source-to-destination route")
	sutName := fs.String("sut", sut.Names[0], "system under test")
	sf := fs.Float64("sf", 0.01, "TPC-H scale factor")
	reps := fs.Int("reps", 1, "repetitions, each with fresh containers and seed")
	out := fs.String("out", "results", "directory for result JSON")
	timeout := fs.Duration("timeout", time.Hour, "overall timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, c := range []struct {
		name, val string
		known     []string
	}{
		{"scenario", *scenario, scenarios},
		{"route", *route, routes},
		{"sut", *sutName, sut.Names},
	} {
		if !slices.Contains(c.known, c.val) {
			return fmt.Errorf("unknown %s %q (known: %s)", c.name, c.val, strings.Join(c.known, ", "))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	meta := sut.New(*sutName)
	result := &harness.Result{
		SUT:       meta.Name(),
		Scenario:  *scenario,
		Route:     *route,
		Dataset:   fmt.Sprintf("tpch-sf%v", *sf),
		Image:     meta.Image(),
		Native:    true,
		Config:    meta.Config(),
		StartedAt: time.Now(),
	}

	for i := range *reps {
		log.Printf("rep %d/%d", i+1, *reps)
		rep, rows, err := runOnce(ctx, sut.New(*sutName), *route, *sf)
		if err != nil {
			return fmt.Errorf("rep %d: %w", i+1, err)
		}
		if result.Rows != 0 && rows != result.Rows {
			return fmt.Errorf("rep %d seeded %d rows; earlier reps seeded %d", i+1, rows, result.Rows)
		}
		result.Rows = rows
		result.Reps = append(result.Reps, rep)
		log.Printf("rep %d/%d: %.3fs (%.0f rows/s), parity pass=%v",
			i+1, *reps, rep.WallSeconds, rep.RowsPerSec, rep.ParityPass)
	}
	result.Aggregate()

	path, err := harness.WriteResult(*out, result)
	if err != nil {
		return err
	}
	printResult(result, path)
	if !result.ParityPass {
		return fmt.Errorf("parity failed, result is not valid")
	}
	return nil
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

// runOnce provisions a fresh environment, seeds, runs the SUT, and checks parity.
func runOnce(ctx context.Context, s sut.SUT, route string, sf float64) (harness.Rep, int64, error) {
	var rep harness.Rep

	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	env, err := harness.NewEnv(ctx, route, runID)
	if err != nil {
		return rep, 0, err
	}
	defer env.Terminate(context.Background())

	tables, err := datasets.SeedTPCH(ctx, env.Source.DSN, sf)
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
	if err := s.Setup(ctx, env, names); err != nil {
		return rep, 0, err
	}
	defer s.Teardown(context.Background())
	setup := time.Since(setupStart)

	sampler := startSampler(ctx, runID)
	defer stopSampler(sampler)
	started := time.Now()
	if err := s.Run(ctx); err != nil {
		return rep, 0, err
	}
	wall := time.Since(started)
	resources := stopSampler(sampler)

	parity, pass, err := harness.CheckParity(ctx, env.Source.DSN, env.Sink.DSN, names)
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
	s, err := harness.StartSampler(ctx, runID, 500*time.Millisecond)
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
