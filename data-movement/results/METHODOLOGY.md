# Methodology

Every published number comes from a raw JSON result produced by this harness.
Remote databases are the reference topology. Local Testcontainers runs are for
development and are never compared with remote results.

## Terms

- A **repetition** is one fresh execution of one SUT and route.
- A **benchmark result** combines the repetitions for one scenario, route,
  dataset, SUT, and topology.
- A **sweep** is one command that runs multiple SUT/route pairs.
- A **cohort** is the shared result identity passed with `-cohort`.

## What is timed

The measured wall time starts when the harness triggers the prepared transfer.
It ends when the adapter reports completion. Process initialization after that
trigger is included.

The following work is outside the measured wall time:

- database provisioning and source seeding;
- dataset generation or download;
- image pulls, dependency installation, and container startup;
- discovery, connector registration, and other SUT setup;
- RDS reboot, recovery, and settling;
- destination validation and SUT teardown.

Most adapters complete when their process or job exits successfully. Streaming
systems such as Debezium and PeerDB complete when every destination table reaches
the expected source row count.

`-timeout` bounds each complete repetition, not just its measured transfer. A
one-time source seed created by `-reuse-seed` happens before the repetition
timeouts. Some setup and cleanup operations also have shorter internal limits.

## What happens in each repetition

1. The harness creates an isolated Docker network and an empty destination.
   Remote SQL sinks have the `bench` namespace reset; remote Iceberg sinks use a
   fresh warehouse prefix and REST catalog.
2. With `-reuse-seed`, every repetition reads the same immutable remote source
   seed and saved row-count manifest. Without it, the source is seeded again.
3. With `-cold-rds`, remote SQL endpoints reboot together. The harness waits for
   RDS availability, SQL connectivity, and the configured settling period.
4. The SUT is started and configured. Before timing begins, the harness also
   waits for PostgreSQL autovacuum to become idle.
5. The prepared transfer runs inside the measured window.
6. The harness captures final health data, tears down the SUT, and validates the
   destination.

Multi-SUT sweeps rotate their starting SUT between repetitions to spread
run-order effects.

## Correctness

Every destination table must have the row count recorded in the immutable seed
manifest. A mismatch fails that SUT/route result, and no JSON result is written
for it. The harness verifies row counts, not row contents.

## Reported measurements

Each result reports median wall time and rows per second, plus the fastest and
slowest repetition. It also records:

- effective SUT configuration and image identity;
- endpoint topology and labels;
- host hardware and harness commit/dirty state;
- Docker CPU, peak memory, and network use during the measured window;
- row-count validation for every destination table.

Docker containers are sampled every 100 ms. CPU and network values are deltas
over the measured window. Memory is each container's highest sample; summed
container peaks may have occurred at different times.

For remote RDS endpoints, database snapshots and CloudWatch points report CPU,
memory, I/O, latency, queue depth, network, storage, and replication-slot lag.
Terraform also enables one-second Enhanced Monitoring by default. S3 resource
use is not included in Docker totals.

## Fair comparison and tuning

All SUTs use normal database durability settings and the same row-count
validation. Setup and discovery remain outside the timer for every adapter.

Each SUT is tuned before a published cohort, and that configuration stays fixed
for all of its repetitions even if it performs worse. Effective values are
stored in the result. Reference parallelism is 32 workers for Filament, OLake,
Debezium, and Ingestr, and 16 normalize/load workers for dlt. Debezium uses
32,768-row source and sink batches; Airbyte connector containers have a 16 GiB
limit each. Adapter-specific environment overrides are for calibration runs.

## Reference environment

The reference SUT host is an AWS `c7i.16xlarge` in `us-east-2`: 64 vCPU,
128 GiB memory, Ubuntu 24.04, and a 750 GB gp3 volume with 16,000 IOPS and
1,000 MB/s throughput.

Each SQL source and sink is a separate RDS instance in the same availability
zone. The default is `db.m6i.8xlarge` with 32 vCPU, 128 GiB memory, and
provisioned gp3 storage. Remote source and sink endpoints must be distinct.

Local Postgres and MySQL use benchmark-sized buffer, WAL/redo, and checkpoint
settings while retaining normal durability. Local and remote numbers must not
be mixed.

## Result files

Results are immutable and include enough provenance to identify their SUT,
data, machine, endpoints, configuration, and code revision:

```text
results/{date}/{cohort}/{sut}/{scenario}-{route}-{dataset}-{topology}.json
```
