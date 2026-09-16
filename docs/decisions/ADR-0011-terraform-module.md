# ADR-0011: A Terraform module is not shipped

- **Status:** Accepted
- **Date:** 2026-09-16
- **Supersedes:** none

## Context

Phase 9 asked whether a Terraform module for this project is worth shipping, as
research only. The constraints were set earlier: ADR-0001 §4 rejects a public cloud
demo (it costs money and breaks unattended) and defers Kubernetes and Helm, so a
module that only makes sense as a running demo, or that wraps a Helm chart, is out.
The only shape that fits is one a reviewer can `terraform apply` from their own
account and destroy with one command.

This record answers the five questions the plan posed and records the decision.

## What the module would provision

Two honest sizes exist.

**Small: one Compose host.** One VM, one security group, a cloud-init script that
installs Docker, clones the repository at a pinned commit and runs `make dev`. About
sixty lines of HCL plus a shell script. Everything the module knows about the
application is "run this Makefile target", which is also why it would stay in sync
with `deploy/docker-compose.yml` for free: it does not describe the services, it
delegates to the file that does.

**Large: managed database and a broker cluster.** A managed PostgreSQL instance with
the TimescaleDB extension, a NATS cluster on three VMs or a hosted NATS service, and
collectors on an autoscaling group behind a load balancer. This is a real
deployment topology and several hundred lines of HCL. It would also fork the
deployment story: Compose for development and Terraform for production, two
descriptions of the same system that drift unless both are maintained, which is the
exact objection ADR-0001 §4 raised to Helm.

Only the small shape fits the constraints.

## Which provider

One, not multi-cloud. AWS has the largest reviewer overlap and the best mock
support in `terraform test`; Hetzner or DigitalOcean is cheaper per hour and
simpler, with fewer resources to declare. Since the module's value is that a
reviewer can apply it from their own account, the provider with the most accounts
wins: AWS, `aws_instance` plus `aws_security_group` plus a key pair, in one region,
using a stock Ubuntu AMI looked up by data source.

## How it is tested without a paid account

Three tiers, each cheaper and weaker than the last:

1. `terraform validate` needs no provider credentials and catches syntax, type and
   reference errors. It runs in CI at no cost.
2. `terraform test` with a `mock_provider "aws" {}` block runs `plan` and `apply`
   against a provider that returns fabricated values, so `run` blocks can assert on
   the security group's ingress rules, the instance's user data containing the
   pinned commit, and the tags, with no account. Also free, also CI-able.
3. A real `terraform apply` followed by a health check on port 8080 and
   `terraform destroy`. This is the only tier that proves the module works, and it
   needs an account, a budget, and a schedule; unattended it is the public demo
   ADR-0001 §4 rejected.

The first two tiers together prove that the HCL is well formed and that the module
declares what it says it declares. They do not prove the instance boots, that
cloud-init succeeds, or that Docker on the chosen AMI runs the stack. A module whose
only tests are mocked is a module whose tests confirm it parses.

## How it stays in sync with `deploy/docker-compose.yml`

The small module never reads the compose file. It pins a commit and runs `make dev`
on the host, so a change to the compose file changes the deployment without touching
HCL. What can drift is the environment the Makefile assumes: Docker Compose v2, the
`make` binary, a user in the `docker` group, port bindings on `127.0.0.1` that the
security group would then have to reach through an SSH tunnel or that the module
would override with `LOGAGG_HTTP_ADDR`. Each of those is a line in cloud-init that a
mocked test cannot exercise.

## What it costs to keep green

- Provider and Terraform version pins that dependabot or renovate would bump weekly,
  each bump a CI run that proves nothing beyond tier 2.
- A Terraform binary in CI (`hashicorp/setup-terraform`), adding a toolchain to a
  repository whose CI is otherwise Go and Docker.
- The tier 3 check, if done at all, done by hand before a release, at a few cents per
  run plus the time to watch it.
- A second place where "how do I run this" is answered, for a reader who has already
  been told `make dev`.

## Decision

**Not shipped.** The module that fits the constraints is sixty lines that delegate to
`make dev`, cannot be tested for real without the paid, unattended footprint ADR-0001
rejected, and adds a toolchain to CI to prove that HCL parses. The signal a reviewer
would take from it, that the author can write a VM plus a security group, is not
one this repository needs to send; the signal it risks sending, an untested
deployment path, is one it needs to avoid.

If the decision is revisited, the scoped design is the small shape above: AWS,
`aws_instance` from a data-sourced Ubuntu AMI, `aws_security_group` allowing 22 from
the caller's IP and nothing else, cloud-init that installs Docker and runs
`make dev` at a pinned commit, outputs of the public IP and an SSH tunnel command,
`terraform validate` and a `mock_provider` test in CI, and a documented manual
`apply`, check, `destroy` before each tagged release. It lives under `deploy/terraform/`
and is listed as a stretch goal in ADR-0001, not as phase 9 work.

Rejected:

- **Shipping the small module untested beyond `validate`.** It would be the only part
  of the repository without a test that exercises its behaviour, in the one place
  where a failure costs the reader money.
- **The large module.** A second deployment description that drifts from Compose;
  the objection to Helm in ADR-0001 §4 applies unchanged.
- **A module that wraps a Helm chart.** Deferred with Kubernetes in ADR-0001 §4.
- **Multi-cloud.** Triples the untestable surface for no additional signal.

## Consequences

- `make dev` remains the single documented way to run the system, and the README's
  quickstart is the deployment guide.
- The stretch-goals list in ADR-0001 points here for the scoped design.
- `deploy/` stays Compose-only.
