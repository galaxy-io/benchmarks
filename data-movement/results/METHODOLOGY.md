# Methodology

How these benchmarks are run and measured. The harness, the adapters, and
the raw results live in this repository. Every number on the results page
traces back to a JSON file beside it.

## What a benchmark is

One benchmark is a scenario, a route, and a dataset: for example, a full
load of TPC-H sf1 from Postgres to Postgres. Every system under test runs
the same combination on the same machine.

Each run repeats several times. A repetition:

1. Starts a fresh Docker network, source database, and sink database.
2. Seeds the source with the dataset.
3. Runs the system's setup: container start, installs, pipeline
   registration. None of this is timed.
4. Runs the load. The clock covers only this window.
5. Checks row counts and tears everything down.

We report the median across repetitions, along with the fastest and slowest
run.

## Measurement

While the clock runs, a sampler reads Docker stats for every container in
the run (the tool, the source, and the sink) every 500ms. From this we keep
CPU seconds, peak memory, and network bytes per container. Peaks are
sampled, so very short spikes can be missed.

## Correctness

A repetition passes parity if every table has the same row count in the
sink as in the source. Parity is a gate: a system that fails it gets no
results for that run. Correctness cannot be traded for speed. We plan to
replace row counts with content checksums, so corrupted rows cannot slip
through.

## Configuration policy

Every system runs the strongest configuration its vendor documents,
preferring the vendor's own published benchmark setup when one exists. The
exact image and settings are recorded in every result file and shown on the
results page. We do not tune competitors beyond their documentation, and we
do not detune them either.

The infrastructure the systems share is configured once and applied to every
system identically:

- The MySQL container runs with an 8 GiB InnoDB buffer pool and a 2 GiB
  redo log, sized as a practitioner would for a dedicated 32 GiB machine.
  Stock defaults (a 128 MB pool) punish tools with non-sequential write
  patterns and test a database nobody deploys.
- The Postgres containers get the matching treatment: 8 GiB shared buffers,
  an 8 GiB WAL ceiling, spread checkpoints, and working memory sized for the
  machine. Durability is never touched: fsync, synchronous commit, full page
  writes, and autovacuum all stay at stock. A sink that can lose data is not
  a benchmark target.
- The databases and the tool share one machine on one Docker network. This
  removes network variance and is generous to chatty tools: a per-row round
  trip costs nearly nothing on localhost. Adding real network distance would
  widen the gaps between systems, not narrow them.

One setting is specific to a single system. Airbyte's connector containers
run with a 4 GiB memory limit each, mirroring the limit the Airbyte platform
itself places on connector pods. Without one, the connector JVMs size their
heaps from the host and destabilize the machine.

## Baselines

Each scenario carries a naive baseline, run by the same harness as
everything else. For full load it is `native`: the engine's own dump client
piped into its load client, one stream, nothing clever. It is the floor a
tool has to beat. Cross-engine routes have no native pipeline, so the
baseline runs only on same-engine routes.

## Hardware

All published numbers come from one pinned machine: an AWS m7i.2xlarge
(8 vCPU Intel Sapphire Rapids, 32 GiB) in us-east-2, running Ubuntu 24.04
on a 100 GB gp3 volume. Source, sink, and tool containers share one Docker
network on that host. Numbers from any other machine are development noise
and are never compared against it.

## Results layout

Results are published as one directory per date:

```
results/
  METHODOLOGY.md
  scripts/                     page generator and template
  {date}/
    index.html                 rendered results page
    {provider}/
      {scenario}-{route}-{dataset}.json
```

Each JSON file is the complete record of one benchmark invocation: image,
configuration, per-repetition timings, resource samples, parity counts, and
the host's operating system, architecture, and CPU count.
