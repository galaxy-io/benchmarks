# Methodology

Every published number traces back to a raw JSON result produced by the same
harness. Remote databases are the primary topology. Local Testcontainers runs
are for development and are never mixed into a remote result cohort.

## Repetitions and timing

A benchmark is one scenario, route, dataset, and SUT. Each repetition:

1. Creates an isolated Docker network for the SUT.
2. Resets remote SQL benchmark namespaces or creates fresh Testcontainers
   databases in local mode. A remote Iceberg sink receives a fresh S3 prefix and
   REST catalog instead.
3. Seeds the source with the requested dataset.
4. Starts and configures the SUT. Image pulls, dependency installation,
   discovery, connector registration, and container startup are setup and are
   not timed.
5. Starts the prepared transfer and measures until the adapter's completion
   condition is met. Most adapters wait for a process or job to finish;
   Debezium waits for destination row counts to reach the source counts.
   Process initialization after the start trigger is timed.
6. Verifies the destination and tears down run-owned resources.

The configured timeout applies independently to each repetition and covers
provisioning, seeding, setup, transfer, and validation. Some component startup
and cleanup operations have shorter deadlines. Results report median wall time
and rows per second, plus the fastest and slowest repetition. Multi-SUT sweeps
rotate their starting SUT each repetition to distribute run-order effects.

## Validation

After the timed run, the harness compares source and destination row counts for
each table. It does not compare row contents.

If any table differs, that SUT and route fail and no result JSON is written for
them. Validation may scan table indexes or data to compute `COUNT(*)`, but it
does not sort or transfer row contents.

## Resource measurement

The harness samples labeled Docker containers every 100 ms. CPU and network
counters are reported as deltas over the timed window. Each container's memory
value is its highest sample during that window. The report sums these
per-container peaks for a SUT; the peaks may occur at different times. Sampling
captures image tags and immutable Docker image IDs.

On remote runs these metrics describe the SUT containers only. RDS and S3
resource use is outside Docker and is intentionally excluded rather than being
presented as a misleading total. Endpoint type and labels remain in the result.

## Configuration and tuning

Each adapter records its extraction, write, batch, worker, and completion
settings where applicable. Discovery and other preparation run before the timed
window. Effective settings are stored in every result file.

Reference parallelism is SUT-specific: 32 workers for Filament, OLake,
Debezium, and Ingestr, and 16 normalize/load workers for dlt. Debezium uses
32,768-row source and sink batches with a four-batch queue. Adapter-specific
batch and memory overrides are available for calibration. Tuning must be chosen
before the published cohort, applied equally to every repetition for that SUT,
and retained even when the result is slower.
Row-count validation and database durability settings are not changed during
tuning.

Airbyte connector containers default to a 16 GiB limit each. The limit prevents
the JVM from sizing itself from the entire host. The effective limit is recorded
with the result.

## Topology and hardware

The reference SUT host is an AWS `c7i.16xlarge` in `us-east-2`: 64 vCPU,
128 GiB of memory, Ubuntu 24.04, and a 750 GB gp3 volume configured for 16,000
IOPS and 1,000 MB/s. Each remote database role uses a separate RDS instance;
the default is `db.m6i.8xlarge` with 32 vCPU, 128 GiB of memory, and provisioned
gp3 storage. All endpoints are placed in one availability zone.

Two remote SQL databases must use distinct host/port endpoints. DSN credentials,
database names, query parameters, hostname case, and default ports do not bypass
that check. An Iceberg sink is identified by its S3 warehouse instead. Remote SQL
namespaces are reset before each repetition; remote Iceberg repetitions use
separate warehouse prefixes. Both give discovery-based tools a clean destination
without including database startup time in the measurement.

Local Postgres and MySQL containers receive benchmark-sized buffer, WAL/redo,
and checkpoint settings while retaining normal durability. Local numbers are a
different topology and must not be compared directly with remote results.

## Provenance and result layout

Each result records the SUT image description, immutable sampled image IDs,
effective configuration, endpoint providers and labels, host hardware, harness
commit and dirty state, timings, resource samples, and row-count validation.

```text
results/
  METHODOLOGY.md
  scripts/
  {date}/
    {cohort}/
      index.html
      {sut}/
        {scenario}-{route}-{dataset}-{topology}.json
```

The page generator rejects mixed cohorts, topologies, machines, harness
revisions, verification modes, row totals, and repetition counts. Endpoint
metadata must match within a route; different routes may use different engines
and endpoints on the same page.
