# Benchmark infrastructure

This Terraform module provisions the remote-first benchmark environment: one
dedicated EC2 host for the SUT and, when configured, a separate RDS instance for
each database role. Keeping source and sink off the SUT host avoids making the
tools compete with database storage and CPU.

The default SUT host is `c7i.16xlarge` with 64 vCPU and 128 GiB. Remote database
roles default to `db.m6i.8xlarge`, 1500 GB of gp3 storage, 51,200 provisioned
IOPS, and 4,000 MB/s throughput. Source and sink are separate instances even on
a same-engine route.

> [!WARNING]
> These defaults create expensive AWS resources. Destroy the environment when
> the benchmark session is finished.

## Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `region` | `us-east-2` | AWS region |
| `instance_type` | `c7i.16xlarge` | Dedicated SUT host |
| `key_name` | required | EC2 key pair name |
| `ssh_cidr` | required | CIDR allowed to SSH |
| `volume_gb` | `750` | Host gp3 volume size |
| `volume_iops` | `16000` | Host gp3 IOPS |
| `volume_throughput` | `1000` | Host gp3 throughput in MB/s |
| `remote_rds` | `[]` | Database ends on RDS: `source_postgres`, `sink_postgres`, `source_mysql`, `sink_mysql` |
| `rds_instance_class` | `db.m6i.8xlarge` | Instance class for remote database ends |
| `rds_allocated_gb` | `1500` | RDS gp3 storage size |
| `rds_iops` | `51200` | RDS provisioned IOPS |
| `rds_throughput` | `4000` | RDS storage throughput in MB/s |
| `rds_monitoring_interval` | `1` | Enhanced Monitoring interval; `0` disables it |
| `remote_iceberg` | `false` | Provision the S3 warehouse, instance role, and gateway endpoint |
| `warehouse_expire_days` | `7` | Days before a warehouse object expires; must outlast a sweep |

The RDS instances and benchmark host share an availability zone. Database ports
accept traffic only from the SUT host security group, and RDS is not publicly
accessible. Postgres has logical replication enabled. MySQL retains automated
backups because RDS requires them for binlog availability. Both engines require
encrypted client connections. Terraform emits Postgres DSNs with
`sslmode=require` and MySQL DSNs with `tls=skip-verify`; both encrypt traffic
without requiring the RDS CA in each SUT image.

## Provision remote endpoints

Provision the endpoints needed for the full six-route sweep. A smaller route
selection needs only its source and sink ends.

```sh
terraform init
terraform apply \
  -var key_name=<key> \
  -var ssh_cidr="$(curl -s ifconfig.me)/32" \
  -var 'remote_rds=["source_postgres","sink_postgres","source_mysql","sink_mysql"]' \
  -var remote_iceberg=true
```

`remote_iceberg` provisions the warehouse bucket, a bucket-scoped instance role
for the host, and an S3 gateway endpoint on its subnet; the harness still starts
the REST catalog on the host, so no managed catalog is involved.

Every end this stack provisioned comes back as shell exports, already named for
the variables the harness reads:

```sh
terraform output -raw ssh
terraform output -raw bench_env
```

The block contains no AWS credentials. Clients obtain refreshable temporary
credentials from the host's instance role. The role is scoped to the warehouse
bucket and this stack's RDS instances, and also permits read-only collection of
CloudWatch metrics. Do not inject temporary credentials for a long sweep.

## Run from the host

After connecting to the host, wait for cloud-init and clone the repository:

```sh
cloud-init status --wait
git clone git@github.com:galaxy-io/benchmarks.git
cd benchmarks/data-movement
```

Ship the environment block from the provisioning machine, then run the sweep in
`tmux`:

```sh
# on the provisioning machine
IP=$(terraform output -raw public_ip)
terraform output -raw bench_env |
  ssh ubuntu@$IP 'umask 077; cat > ~/.bench.env'
```

```sh
# on the host
source ~/.bench.env
export BENCH_MACHINE='c7i.16xlarge'
tmux new -s bench
```

Inside `tmux`, use the same canonical remote command as the main README:

```sh
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

`-cold-rds` reboots the SQL endpoints concurrently before each repetition,
waits for SQL recovery, and leaves a 30-second quiet period after SUT setup.
`-rds-metrics` defaults on and embeds CloudWatch points plus database-native
before/after snapshots when the Terraform RDS identifiers are present.

`-route all` needs every end of every route present, so the run stops before
provisioning if a variable is missing, naming the one it wanted.

`-timeout` applies independently to each repetition and covers provisioning,
seeding, setup, transfer, and validation. The one-time source seed created by
`-reuse-seed` happens before the repetition timeouts. A repetition is one fresh
execution of a SUT/route pair, a sweep is one command containing multiple pairs,
and a cohort is their shared result identity. Choose a new `-cohort` for
calibration; do not mix calibration trials into a published cohort.

## Tear down

```sh
terraform destroy -var key_name=<key> -var ssh_cidr=0.0.0.0/32 \
  -var 'remote_rds=["source_postgres","sink_postgres","source_mysql","sink_mysql"]' \
  -var remote_iceberg=true
```

Use the same resource-selection variables for apply and destroy. The specific
`ssh_cidr` value does not matter during destroy as long as it is valid.
