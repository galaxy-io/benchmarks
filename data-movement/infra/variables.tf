variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-2"
}

variable "instance_type" {
  description = "EC2 instance type; the pinned benchmark machine"
  type        = string
  default     = "m7i.2xlarge"
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
  default     = 100
}
