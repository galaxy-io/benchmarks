variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "instance_type" {
  description = "EC2 instance type; the pinned benchmark machine"
  type        = string
  default     = "c7i.16xlarge"
}

variable "key_name" {
  description = "EC2 key pair name for SSH"
  type        = string
}

variable "ssh_cidr" {
  description = "CIDR allowed to SSH, e.g. your IP as x.x.x.x/32"
  type        = string
}

variable "volume_gb" {
  description = "Root volume size in GB"
  type        = number
  default     = 750
}

variable "volume_iops" {
  description = "Root volume provisioned IOPS; gp3 tops out at 16000"
  type        = number
  default     = 16000
}

variable "volume_throughput" {
  description = "Root volume throughput in MB/s; gp3 tops out at 1000"
  type        = number
  default     = 1000
}

variable "remote_rds" {
  description = "Database ends provisioned on RDS, each named role_engine; a full sweep needs all four"
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for end in var.remote_rds :
      contains(["source_postgres", "sink_postgres", "source_mysql", "sink_mysql"], end)
    ])
    error_message = "remote_rds takes source_postgres, sink_postgres, source_mysql, and sink_mysql."
  }
}

variable "rds_instance_class" {
  description = "RDS instance class for remote roles"
  type        = string
  default     = "db.m6i.8xlarge"
}

variable "rds_allocated_gb" {
  description = "RDS gp3 storage in GB; 400 or more unlocks the higher IOPS tier"
  type        = number
  default     = 1500
}

variable "rds_iops" {
  description = "RDS provisioned IOPS; gp3 is fixed at 3000 below 400 GB and tops out at 64000 above it"
  type        = number
  default     = 51200
}

variable "rds_throughput" {
  description = "RDS storage throughput in MB/s; gp3 tops out at 4000"
  type        = number
  default     = 4000
}

variable "rds_monitoring_interval" {
  description = "Enhanced Monitoring interval in seconds; use 0 to disable"
  type        = number
  default     = 1

  validation {
    condition     = contains([0, 1, 5, 10, 15, 30, 60], var.rds_monitoring_interval)
    error_message = "rds_monitoring_interval must be 0, 1, 5, 10, 15, 30, or 60."
  }
}

variable "remote_iceberg" {
  description = "Provision an S3 warehouse for Iceberg sinks rather than running MinIO on the benchmark host"
  type        = bool
  default     = false
}

variable "warehouse_expire_days" {
  description = "Days before a warehouse object expires; longer than the longest sweep, so nothing expires mid-run"
  type        = number
  default     = 7

  validation {
    condition     = var.warehouse_expire_days >= 2
    error_message = "warehouse_expire_days must outlast a sweep; use 2 or more."
  }
}
