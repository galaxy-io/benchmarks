terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region
}

# Ubuntu 24.04 amd64 AMI ID published by Canonical.
data "aws_ssm_parameter" "ubuntu" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

# The benchmark host and RDS instances use the same availability zone.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_subnet" "bench" {
  id = sort(data.aws_subnets.default.ids)[0]
}

locals {
  # Remote and local runs use the same major database versions.
  rds_engines = {
    postgres = {
      version = "16"
      port    = 5432
      family  = "postgres16"
      # Disable automated backups to avoid scheduled snapshots during a run.
      backup_days = 0
      # Enable logical replication for CDC adapters and require TLS.
      params = {
        "rds.logical_replication" = "1"
        "max_replication_slots"   = "20"
        "max_wal_senders"         = "20"
        "rds.force_ssl"           = "1"
      }
    }
    mysql = {
      version = "8.4"
      port    = 3306
      family  = "mysql8.4"
      # RDS requires automated backups to retain MySQL binary logs.
      backup_days = 1
      params = {
        # The seeder requires LOAD DATA LOCAL INFILE. MySQL 8.4 defaults
        # binlog_format to ROW.
        "local_infile"             = "1"
        "binlog_row_image"         = "FULL"
        "require_secure_transport" = "ON"
      }
    }
  }

  # Key by role and engine together. Same-engine routes need separate source
  # and sink instances, and a sweep crosses both engines, so all four ends can
  # exist at once. Each key is also the middle of the BENCH_<ROLE>_<ENGINE>_DSN
  # the harness reads.
  rds = {
    for end in var.remote_rds :
    end => merge(local.rds_engines[split("_", end)[1]], {
      engine   = split("_", end)[1],
      role     = split("_", end)[0],
      aws_name = replace(end, "_", "-")
    })
  }

  # Remote RDS control and remote Iceberg both authenticate through the host's
  # instance profile. Neither path uses long-lived AWS credentials.
  bench_identity = length(var.remote_rds) > 0 || var.remote_iceberg
}

resource "aws_security_group" "bench" {
  name_prefix = "bench-"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Allow database connections only from the benchmark host security group.
resource "aws_security_group" "rds" {
  count       = length(local.rds) > 0 ? 1 : 0
  name_prefix = "bench-rds-"
  vpc_id      = data.aws_vpc.default.id

  dynamic "ingress" {
    for_each = toset([for cfg in local.rds : cfg.port])
    content {
      from_port       = ingress.value
      to_port         = ingress.value
      protocol        = "tcp"
      security_groups = [aws_security_group.bench.id]
    }
  }
}

resource "aws_db_subnet_group" "bench" {
  count       = length(local.rds) > 0 ? 1 : 0
  name_prefix = "bench-"
  subnet_ids  = data.aws_subnets.default.ids
}

resource "random_password" "rds" {
  count  = length(local.rds) > 0 ? 1 : 0
  length = 24
  # Use an alphanumeric password so output DSNs do not require URL encoding.
  special = false
}

resource "aws_db_parameter_group" "rds" {
  for_each    = local.rds
  name_prefix = "bench-${each.value.aws_name}-"
  family      = each.value.family

  dynamic "parameter" {
    for_each = each.value.params
    content {
      name  = parameter.key
      value = parameter.value
      # The group is attached at instance creation, so pending-reboot values
      # apply during the initial startup.
      apply_method = "pending-reboot"
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

# RDS assumes this service role to publish one-second OS metrics to the
# RDSOSMetrics CloudWatch Logs group.
resource "aws_iam_role" "rds_monitoring" {
  count       = length(local.rds) > 0 && var.rds_monitoring_interval > 0 ? 1 : 0
  name_prefix = "bench-rds-monitoring-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "monitoring.rds.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  count      = length(aws_iam_role.rds_monitoring)
  role       = aws_iam_role.rds_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

resource "aws_db_instance" "rds" {
  for_each          = local.rds
  identifier_prefix = "bench-${each.value.aws_name}-"
  engine            = each.value.engine
  engine_version    = each.value.version
  instance_class    = var.rds_instance_class

  db_name  = "bench"
  username = "bench"
  password = random_password.rds[0].result

  storage_type       = "gp3"
  allocated_storage  = var.rds_allocated_gb
  iops               = var.rds_iops
  storage_throughput = var.rds_throughput

  availability_zone      = data.aws_subnet.bench.availability_zone
  db_subnet_group_name   = aws_db_subnet_group.bench[0].name
  vpc_security_group_ids = [aws_security_group.rds[0].id]
  parameter_group_name   = aws_db_parameter_group.rds[each.key].name
  publicly_accessible    = false

  monitoring_interval = var.rds_monitoring_interval
  monitoring_role_arn = var.rds_monitoring_interval > 0 ? aws_iam_role.rds_monitoring[0].arn : null

  backup_retention_period = each.value.backup_days
  multi_az                = false
  apply_immediately       = true
  skip_final_snapshot     = true
  deletion_protection     = false

  tags = {
    Name = "bench-${each.value.aws_name}"
  }
}

# Remote Iceberg sinks use this S3 warehouse instead of host-local MinIO. The
# REST catalog remains on the benchmark host. Clients obtain refreshable
# credentials from the host's instance role.
locals {
  warehouse = var.remote_iceberg ? aws_s3_bucket.warehouse[0].id : ""
}

resource "random_id" "warehouse" {
  count       = var.remote_iceberg ? 1 : 0
  byte_length = 4
}

resource "aws_s3_bucket" "warehouse" {
  count  = var.remote_iceberg ? 1 : 0
  bucket = "bench-warehouse-${random_id.warehouse[0].hex}"
  # A destroy at the end of a sweep would otherwise fail on the run's tables.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "warehouse" {
  count                   = var.remote_iceberg ? 1 : 0
  bucket                  = aws_s3_bucket.warehouse[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "warehouse" {
  count  = var.remote_iceberg ? 1 : 0
  bucket = aws_s3_bucket.warehouse[0].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "warehouse" {
  count  = var.remote_iceberg ? 1 : 0
  bucket = aws_s3_bucket.warehouse[0].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "warehouse" {
  count  = var.remote_iceberg ? 1 : 0
  bucket = aws_s3_bucket.warehouse[0].id

  # The harness writes a fresh prefix per run and never deletes one, so the
  # bucket empties itself rather than growing across sweeps.
  rule {
    id     = "expire-runs"
    status = "Enabled"
    filter {}
    expiration {
      days = var.warehouse_expire_days
    }
  }

  # A writer killed mid-upload leaves parts that bill until they are aborted.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_s3_bucket_policy" "warehouse" {
  count  = var.remote_iceberg ? 1 : 0
  bucket = aws_s3_bucket.warehouse[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        aws_s3_bucket.warehouse[0].arn,
        "${aws_s3_bucket.warehouse[0].arn}/*",
      ]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
  depends_on = [aws_s3_bucket_public_access_block.warehouse]
}

# A subnet can use either an explicitly associated route table or the VPC's main
# route table. The plural lookup may return no explicit association; the main
# lookup provides the fallback.
data "aws_route_tables" "bench_explicit" {
  count  = var.remote_iceberg ? 1 : 0
  vpc_id = data.aws_vpc.default.id

  filter {
    name   = "association.subnet-id"
    values = [data.aws_subnet.bench.id]
  }
}

data "aws_route_table" "bench_main" {
  count  = var.remote_iceberg ? 1 : 0
  vpc_id = data.aws_vpc.default.id

  filter {
    name   = "association.main"
    values = ["true"]
  }
}

locals {
  bench_route_table_id = var.remote_iceberg ? (
    length(data.aws_route_tables.bench_explicit[0].ids) > 0
    ? data.aws_route_tables.bench_explicit[0].ids[0]
    : data.aws_route_table.bench_main[0].id
  ) : null
}

# Warehouse traffic uses the S3 gateway endpoint rather than the internet path.
resource "aws_vpc_endpoint" "s3" {
  count             = var.remote_iceberg ? 1 : 0
  vpc_id            = data.aws_vpc.default.id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [local.bench_route_table_id]
}

# The bench host's identity. Attached policies scope mutations to this stack's
# warehouse bucket and RDS instances; CloudWatch access is read-only.
resource "aws_iam_role" "bench" {
  count       = local.bench_identity ? 1 : 0
  name_prefix = "bench-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

# The benchmark host can reboot only this stack's RDS instances. Describe and
# CloudWatch read APIs do not support useful resource-level scoping.
resource "aws_iam_role_policy" "rds_control" {
  count       = length(local.rds) > 0 ? 1 : 0
  name_prefix = "rds-control-"
  role        = aws_iam_role.bench[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["rds:RebootDBInstance"]
        Resource = [for db in aws_db_instance.rds : db.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "rds:DescribeDBInstances",
          "cloudwatch:GetMetricData",
          "cloudwatch:GetMetricStatistics",
          "logs:DescribeLogStreams",
          "logs:FilterLogEvents",
          "logs:GetLogEvents",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "warehouse" {
  count       = var.remote_iceberg ? 1 : 0
  name_prefix = "warehouse-"
  role        = aws_iam_role.bench[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
        Resource = aws_s3_bucket.warehouse[0].arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = "${aws_s3_bucket.warehouse[0].arn}/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "bench" {
  count       = local.bench_identity ? 1 : 0
  name_prefix = "bench-"
  role        = aws_iam_role.bench[0].name
}

resource "aws_instance" "bench" {
  ami                    = data.aws_ssm_parameter.ubuntu.value
  instance_type          = var.instance_type
  key_name               = var.key_name
  subnet_id              = data.aws_subnet.bench.id
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = local.bench_identity ? aws_iam_instance_profile.bench[0].name : null
  user_data              = file("${path.module}/scripts/user-data.sh")

  # Require IMDSv2 on every host. A hop limit of 2 lets benchmark containers
  # reach the instance role when remote Iceberg is enabled.
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size = var.volume_gb
    volume_type = "gp3"
    iops        = var.volume_iops
    throughput  = var.volume_throughput
  }

  tags = {
    Name = "bench-${var.instance_type}"
  }
}
