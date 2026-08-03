# data-movement

Benchmarks data-movement tools: full load and change data capture between Postgres
and MySQL. Every tool moves the same data on the same hardware, and every row is
verified before a number is recorded.

> [!NOTE]
> We built [filament](https://github.com/galaxy-io/filament), one of the tools measured here.

## Systems under test

Each entry is a SUT, a system under test: the tool a run measures, started by the
harness, pointed at the same source and destination, and torn down after.

| SUT | How it runs | Comparison | Status |
|-----|-------------|------------|--------|
| filament | Standalone image | Measured | ✅ |
| Ingestr | CLI container | Measured | ✅ |
| Debezium\* | Debezium Server container, JDBC sink | Measured | ✅ |
| Airbyte | Pinned connector images | Measured | ✅ |
| dlt | Python container | Measured | ✅ |
| native | Engine dump client piped into its load client | Baseline | ✅ |
| Artie | Managed only | Claimed | ❌ |
| Fivetran | Managed only | Claimed | ❌ |
| AWS DMS | Managed only | Claimed | ❌ |

- **Measured**: run by the harness. Identical source and destination containers,
  pinned images, identical resource caps, fresh containers per repetition.
- **Baseline**: the floor a tool has to beat, run by the same harness. Same-engine
  routes only; cross-engine routes have no native pipeline.
- **Claimed**: the vendor's published number, cited. Filament runs on matching
  infrastructure: same instance class, dataset, and workload.

\* Debezium is a CDC engine; a full load measures its initial snapshot, the
first step of every deployment, not the steady-state streaming it is built for.

## Benchmarks

Routes: `pg → pg`, `pg → mysql`, `mysql → mysql`, `mysql → pg`. (where applicable)

| Scenario | Dataset | Routes | Shows | Status |
|----------|---------|--------|-------|--------|
| Full load | TPC-H | All four | Rows per second, wall time | ✅ |
| CDC latency | pgbench churn | pg → pg, mysql → mysql | End-to-end lag, p50/p95/p99 | Planned |
| Mixed churn | TPROC-C | pg → pg, mysql → mysql | Sustained apply rate under load | Planned |
| Retention | pgbench churn | pg → pg, mysql → mysql | WAL/binlog held on the source | Planned |
| Recovery | TPC-H | pg → pg | Time to resume after a mid-load kill | Planned |

## Measurement

- Runs are timed by watching the destination, never by asking the tool
- CDC lag (planned): heartbeat rows carrying commit timestamps, read back from the
  destination, reported as p50/p95/p99
- Per-container CPU, memory, and network from the Docker stats API
- Row counts must match between source and destination, or no result is recorded;
  content checksums are planned

Each run emits one JSON document: timings, resource use, parity, the image and
configuration under test, and the host's OS, architecture, and CPU count.

## Running

Requires Docker and `duckdb`.

```sh
go run ./cmd/bench list                                              # scenarios, routes, suts
go run ./cmd/bench run -scenario full-load -route pg-pg -sut filament
go run ./cmd/bench run -sut all -route all                           # sweep every supported pair
```

Each run writes one JSON document to `results/{date}/{sut}/`. Official results are
produced on a pinned EC2 instance type; vendor-matched results use the instance
type the vendor published. See [results/METHODOLOGY.md](results/METHODOLOGY.md).

## Layout

```
data-movement
├── cmd
│   └── bench       CLI: list | run
├── datasets        TPC-H via duckdb
├── harness         Provisioning, sampler, parity, results
├── infra           Terraform for the benchmark machine
├── results         Methodology, page generator, one folder per published date
└── sut             One adapter package per system under test
```
