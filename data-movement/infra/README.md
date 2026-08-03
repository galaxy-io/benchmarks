# infra

One EC2 instance for benchmark runs: the pinned machine official results come from.
Vendor-matched runs use the same module with the vendor's published instance type.

> [!WARNING]
> Applying this module provisions AWS resources that bill by the hour: the instance
> (`m7i.2xlarge` is about $0.40/hour), its EBS volume, and data transfer. Destroy
> when the session ends.

## Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `region` | `us-east-2` | AWS region |
| `instance_type` | `m7i.2xlarge` | The pinned benchmark machine |
| `key_name` | required | EC2 key pair name |
| `ssh_cidr` | required | CIDR allowed to SSH, your IP as `x.x.x.x/32` |
| `volume_gb` | `100` | Root gp3 volume size |

The security group opens port 22 to `ssh_cidr` and nothing else; ports Docker maps
during runs are unreachable from outside.

## Example run

```sh
# Provision the machine and connect (-A forwards your agent for the clone)
terraform init
terraform apply -var key_name=<key> -var ssh_cidr="$(curl -s ifconfig.me)/32"
ssh -A -i ~/.ssh/<key> ubuntu@$(terraform output -raw public_ip)
```

```sh
# On the box: wait for tooling, clone, run, push results
cloud-init status --wait

git clone git@github.com:galaxy-io/benchmarks.git
cd benchmarks/data-movement

# -timeout covers the whole invocation, not one rep; the 1h default
# kills slow SUTs mid-sweep at sf 1.
go run ./cmd/bench run -sut filament -reps 5 -sf 1 -timeout 6h
go run ./cmd/bench run -sut all -route all -reps 5 -sf 1 -timeout 24h
```

```sh
# back on your machine: tear it down
terraform destroy -var key_name=<key> -var ssh_cidr=0.0.0.0/32
```

Notes
- Destroy accepts any valid `ssh_cidr`; the value only matters at apply
