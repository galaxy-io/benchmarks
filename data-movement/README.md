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
| Artie | Managed only | Claimed | ❌ |
| Fivetran | Managed only | Claimed | ❌ |
| AWS DMS | Managed only | Claimed | ❌ |

- **Measured**: run by the harness. Identical source and destination containers,
  pinned images, identical resource caps, fresh containers per repetition.
- **Claimed**: the vendor's published number, cited. Filament runs on matching
  infrastructure: same instance class, dataset, and workload.

\* Debezium is a CDC engine; a full load measures its initial snapshot, the
first step of every deployment, not the steady-state streaming it is built for.

## Benchmarks

Routes: `pg → pg`, `pg → mysql`, `mysql → mysql`, `mysql → pg`. (where applicable)

| Scenario | Dataset | Routes | Shows |
|----------|---------|--------|-------|
| Full load | TPC-H | All four | Rows per second, wall time |
| CDC latency | pgbench churn | pg → pg, mysql → mysql | End-to-end lag, p50/p95/p99 |
| Mixed churn | TPROC-C | pg → pg, mysql → mysql | Sustained apply rate under load |
| Retention | pgbench churn | pg → pg, mysql → mysql | WAL/binlog held on the source |
| Recovery | TPC-H | pg → pg | Time to resume after a mid-load kill |

## Measurement

- Runs are timed by watching the destination, never by asking the tool
- CDC lag: heartbeat rows carrying commit timestamps, read back from the destination,
  reported as p50/p95/p99
- Per-container CPU, memory, and network from the Docker stats API
- Row counts and checksums must match between source and destination, or no result is
  recorded

Each run emits one JSON document: timings, lag percentiles, resource use, parity, and
provenance (machine, commit SHAs, image digests).

## Running

Requires Docker and `duckdb`.

```sh
go run ./cmd/bench list                                              # scenarios, routes, suts
go run ./cmd/bench run -scenario full-load -route pg-pg -sut filament
go run ./cmd/bench run -sut all -route all                           # sweep every supported pair
```

Each run writes one JSON document to `results/`. Official results are produced on a
pinned EC2 instance type, recorded in the result; vendor-matched results use the
instance type the vendor published.

## Layout

```
data-movement
├── cmd
│   └── bench       CLI: list | run
├── datasets        TPC-H via duckdb
├── harness         Provisioning, sampler, parity, results
├── infra           Terraform for the benchmark machine
└── sut             One adapter package per system under test
```
