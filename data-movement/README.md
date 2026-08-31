# data-movement

A benchmark harness for moving complete datasets between Postgres, MySQL, and
Apache Iceberg. Remote databases are the primary benchmark topology.
Testcontainers support local development and smoke tests.

> [!NOTE]
> We built [filament](https://github.com/galaxy-io/filament), one of the systems
> measured here.

## Systems under test

| SUT | Benchmark path | Routes |
|-----|----------------|--------|
| filament | Standalone container | SQL sources to SQL or Iceberg |
| Sling | Official CLI container | Postgres and MySQL |
| Ingestr | Official CLI container | Postgres and MySQL |
| Debezium | Debezium Server with JDBC sink | Postgres and MySQL |
| Airbyte | Source and destination connector containers | Postgres and MySQL |
| dlt | Python container with ConnectorX | Postgres/MySQL to Postgres |
| OLake | Official source container and Iceberg writer | Postgres/MySQL to Iceberg |
| PeerDB | Official service stack | Postgres to Postgres |
| native | Engine dump client piped to its load client | Same-engine baseline |

Debezium is a CDC engine. The full-load benchmark measures its initial snapshot,
not the steady-state streaming workload it is primarily designed for.

## Workload

The implemented scenario is a full load of either TPC-H or NYC Taxi data. The
available routes are:

```text
pg → pg       pg → mysql       pg → iceberg
mysql → pg    mysql → mysql    mysql → iceberg
```

## Prerequisites

- The Go version declared in `go.mod`.
- Docker, for local databases and every containerized SUT.
- The DuckDB CLI on `PATH`, for dataset generation and Iceberg validation.
- Network access on the first run, so DuckDB can install its extensions and the
  Taxi seeder can download source Parquet files.

TPC-H data is generated deterministically for the requested scale factor. Taxi
data is read backward from December 2024 and cached under the operating
system's user cache directory at `galaxy-benchmarks/taxi`; later repetitions
reuse successfully downloaded files. Dataset generation, download, source
loading, and destination validation are outside the timed transfer window.

Remote result cohorts intended for publication use one immutable source seed,
a fresh sink per repetition, and `-cold-rds` to reboot the SQL endpoints before
every timed run.
The seed is loaded, vacuumed, frozen, and analyzed once before the first
repetition, and its row-count manifest is passed to adapters, so setup and
validation do not scan the source. Setup, the configured post-reboot settling
period, and a check that no autovacuum worker is running finish before the
timer starts. The timed window begins when the harness starts the
prepared transfer and includes any process initialization that follows. A
result is accepted only when every destination table has the manifest row count.

The harness records timing, effective SUT configuration, endpoint topology,
host details, Docker image IDs, and per-container CPU, memory, and network use.
When Terraform-provided RDS identifiers are present it also records database
counter snapshots before, after, and after teardown, plus raw CloudWatch points
around the timed window. Terraform enables one-second RDS Enhanced Monitoring
in CloudWatch Logs for deeper investigation.

## Running remotely

Remote runs require distinct source and sink endpoints. Labels are optional but
recommended because they make the result cohort self-describing.

Each end is named by its role and its engine, `BENCH_<ROLE>_<ENGINE>_DSN`, so one
sweep can cross both engines without a mysql route receiving the postgres
endpoint. Set the ends the selected routes need; `-route all` needs all of them.

```sh
export BENCH_SOURCE_POSTGRES_DSN='postgresql://bench:...@pg-source:5432/bench?sslmode=require'
export BENCH_SINK_POSTGRES_DSN='postgresql://bench:...@pg-sink:5432/bench?sslmode=require'
export BENCH_SOURCE_MYSQL_DSN='mysql://bench:...@my-source:3306/bench?tls=skip-verify'
export BENCH_SINK_MYSQL_DSN='mysql://bench:...@my-sink:3306/bench?tls=skip-verify'
export BENCH_SINK_ICEBERG_DSN='s3://bench-warehouse-0a1b2c3d'
export AWS_REGION='us-east-2'
export BENCH_SOURCE_POSTGRES_LABEL='rds-pg-source'
export BENCH_MACHINE='c7i.16xlarge'

go run ./cmd/bench run \
  -cohort tpch-sf1-all \
  -topology remote \
  -scenario full-load \
  -sut all \
  -route all \
  -dataset tpch -sf 1 \
  -reuse-seed -cold-rds \
  -reps 5 -timeout 24h
```

A repetition is one fresh execution of a SUT/route pair. A sweep is one command
that runs multiple SUT/route pairs. A cohort is the shared result identity set
by `-cohort`. Choose a new cohort name for each published run or calibration
attempt; result files are immutable.

`-timeout` bounds each repetition, including provisioning, seeding, setup,
transfer, and validation. Some startup and cleanup operations have shorter
component-specific deadlines. The one-time source seed created by `-reuse-seed`
happens before the repetition timeouts. Reference parallelism is explicit: 32
for Filament, OLake, and Ingestr, 16 for dlt, and 8 for Debezium. Debezium uses
table-parallel snapshots, 8,192-row engine/sink batches, 10,240-row snapshot
fetches, a 1 GiB queue byte limit, and a 64 GiB JVM heap. Airbyte connectors
have 16 GiB limits.
`BENCH_AIRBYTE_CONNECTOR_MEMORY`, `BENCH_DEBEZIUM_BATCH_SIZE`, and
`BENCH_DLT_BATCH_SIZE` remain calibration overrides; effective values are
written into the result.

Sling runs one `full-refresh` CLI process per table concurrently. This keeps
the benchmark on the free CLI and does not require a Sling CLI Pro token for
parallel-stream execution.

Multi-SUT sweeps rotate their execution order between repetitions so one tool
does not always receive the coldest or warmest run position.

A remote Iceberg sink takes `BENCH_SINK_ICEBERG_DSN` as `s3://bucket`, plus
`AWS_REGION`. Leave the credential variables unset on the benchmark host: every
writer, the REST catalog, and DuckDB then walk the AWS default chain to the
host's instance role, and the SDK refreshes those credentials for as long as the
sweep runs. Injected temporary credentials do not refresh, so a long sweep can
outlive them. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, with optional
`AWS_SESSION_TOKEN`, remain the fallback for reaching S3 from a host that has no
instance role; supply the key and secret together or not at all. A local Iceberg
sink needs none of them, since MinIO carries its own fixed test credentials.

## Running locally

Local mode is useful for development and smoke checks. It creates fresh source
and sink services with Testcontainers for every repetition.

```sh
go run ./cmd/bench list
go run ./cmd/bench run \
  -cohort smoke-filament \
  -topology local \
  -scenario full-load \
  -sut filament \
  -route pg-pg \
  -dataset tpch -sf 0.01 \
  -reps 1 -timeout 1h
```

`hybrid` mode is also available and requires exactly one remote end per selected
route, named the same way.

## Results

Results from different machines, topologies, endpoints, commits, or cohorts are
kept separate. Files are written without overwriting an earlier invocation:

```text
results/{date}/{cohort}/{sut}/{scenario}-{route}-{dataset}-{topology}.json
```

See [results/METHODOLOGY.md](results/METHODOLOGY.md) for measurement details and
[infra/README.md](infra/README.md) for the AWS environment.

## Layout

```text
data-movement/
├── cmd/bench       benchmark CLI
├── datasets        TPC-H and NYC Taxi seeders
├── harness         timing, engines, sampling, validation, and results
├── infra           Terraform for the remote benchmark environment
├── provider        Testcontainers and remote endpoint providers
├── results         methodology and raw cohorts
└── sut             one adapter per system under test
```
