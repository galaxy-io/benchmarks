locals {
  rds_dsn = {
    for end, cfg in local.rds :
    end => cfg.engine == "postgres" ? format(
      "postgresql://bench:%s@%s:5432/bench?sslmode=require",
      random_password.rds[0].result, aws_db_instance.rds[end].address
      ) : format(
      // skip-verify, not true: RDS presents a certificate signed by the Amazon
      // RDS CA, which is in no default trust store, and tls=true verifies the
      // chain. This encrypts without verifying it, matching what sslmode=require
      // does on the postgres side, so both engines cross the wire the same way.
      "mysql://bench:%s@%s:3306/bench?tls=skip-verify",
      random_password.rds[0].result, aws_db_instance.rds[end].address
    )
  }

  # The variables the harness reads, in the order a shell would set them. Each
  # RDS key is already role_engine, so it names its own variable.
  bench_env = concat(
    [for end, dsn in local.rds_dsn : "export BENCH_${upper(end)}_DSN='${dsn}'"],
    [for end, db in aws_db_instance.rds : "export BENCH_${upper(end)}_RDS_ID='${db.identifier}'"],
    local.bench_identity ? ["export AWS_REGION='${var.region}'"] : [],
    local.warehouse == "" ? [] : [
      "export BENCH_SINK_ICEBERG_DSN='s3://${local.warehouse}'",
    ],
  )
}

output "public_ip" {
  value = aws_instance.bench.public_ip
}

output "ssh" {
  value = "ssh -A ubuntu@${aws_instance.bench.public_ip}"
}

# Every remote end this stack provisioned, as shell exports. Ship it to the
# host and source it; nothing else needs to be mapped by hand. It carries no
# AWS credentials on purpose: the host authenticates as its instance role.
output "bench_env" {
  value     = join("\n", local.bench_env)
  sensitive = true
}

# The warehouse a remote Iceberg sink writes to, empty unless remote_iceberg is
# set.
output "warehouse_dsn" {
  value = local.warehouse == "" ? "" : "s3://${local.warehouse}"
}

output "warehouse_region" {
  value = local.warehouse == "" ? "" : var.region
}
