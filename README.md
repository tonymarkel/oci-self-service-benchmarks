# Cloud Self-Service Benchmarks

[![CI](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/workflows/ci.yml/badge.svg)](https://github.com/tonymarkel/oci-self-service-benchmarks/actions/workflows/ci.yml)

A local web application that provisions benchmark VMs in Oracle Cloud
Infrastructure, Amazon Web Services, Google Cloud, or Microsoft Azure, runs
selected open-source benchmarks, produces HTML reports, and can destroy all
resources it created.

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Keep auto-reload disabled while benchmarks are active. A source-triggered
worker restart interrupts the local benchmark supervisor; the saved resource
manifest remains recoverable, but the benchmark itself cannot resume.

Open `http://127.0.0.1:8000` and choose a cloud provider.

## Development and contributions

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the pull
request process and run the complete local check suite with:

```bash
PYTHON_BIN=.venv/bin/python scripts/check.sh
```

Every pull request runs the same credential-free checks on the supported
Python runtime endpoints. Pull-request CI never authenticates to OCI, AWS,
GCP, or Azure and never provisions cloud resources; live cloud validation
remains a separate, maintainer-controlled step.

The Previous runs page can compare two to eight completed runs with matching
workload contracts. Its single-metric explorer links to a dedicated,
shareable all-metrics page with one baseline across every chart, expandable
exact-value tables, and explicit provenance or exclusion notes.

- OCI reads `~/.oci/config` using the `DEFAULT` profile. The OCI principal must be allowed to create VCNs, compute instances, volumes, and public IPs in the chosen compartment.
- AWS uses Boto3 with the selected local shared-config profile (`default` by default). The AWS CLI is not invoked by the app. Access keys, session tokens, and other AWS credential material stay in the normal local AWS credential chain and are never copied into a benchmark plan, guest VM, or report.
- GCP uses local Application Default Credentials (ADC) through the Google Cloud
  Python SDK. OAuth tokens and credential files are never copied into a plan,
  guest VM, or report.
- Azure uses the active local Azure CLI session through `AzureCliCredential`.
  Access tokens and Azure credentials are never copied into a plan, guest VM,
  or report.

AWS defaults to `us-east-2`, resolves AWS's latest standard Amazon Linux 2023
AMI through its public Systems Manager parameter, and supports Sysbench CPU,
memory, and file I/O; STREAM; fio; iperf3 TCP/UDP/SCTP; the curated Phoronix
profiles; ApacheBench; DeathStarBench; and the pinned CPU-only llama.cpp
benchmark. EC2 instance type discovery determines the fixed vCPU and memory
values.

The selected AWS profile needs permission to read its caller identity, enabled
regions, Availability Zones, EC2 instance types and offerings, Amazon-owned
AMI metadata, and the public Amazon Linux SSM parameter. For a run it also
needs permission to create, tag, describe, and delete the job's VPC, subnet,
internet gateway, route table, security groups, EC2 key pair, instances, and
EBS volumes, and to attach and detach the optional data volume. The guest
receives no AWS credentials and does not need an instance role.

## Google Cloud setup

Configure ADC on the local machine before selecting Google Cloud in the form:

```bash
gcloud config set project PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project PROJECT_ID
```

The Compute Engine API must be enabled for the project. A project administrator
can enable it once with:

```bash
gcloud services enable compute.googleapis.com --project PROJECT_ID
```

The app uses `GCP_PROJECT_ID` when set and otherwise discovers the ADC project,
but it still records an explicit project ID and zone in every submitted plan.
The default region is `us-east1` (override it with `GCP_DEFAULT_REGION`); choose
the desired region and zone in the form.
The local ADC principal needs Compute Engine access at roughly
`roles/compute.admin` for the current flow and
`roles/serviceusage.serviceUsageConsumer`
on the quota project. Enabling the API itself requires separate Service Usage
administration permission. No service account or cloud credential is attached
to any guest VM, so `iam.serviceAccounts.actAs` is not required by this
flow.

GCP runs use Google's latest standard Rocky Linux 9 image family for the
selected x86_64 or Arm64 machine type. The supported suite is Sysbench CPU,
memory, and file I/O; STREAM; fio; iperf3 TCP/UDP/SCTP; the curated Phoronix
profiles; ApacheBench; DeathStarBench; and the pinned CPU-only llama.cpp
benchmark. Each run uses a dedicated custom-mode VPC and regional subnet. An
iperf3 selection adds a same-machine-type peer in the same zone and sends test
traffic only to its private address. Selecting ApacheBench or DeathStarBench
adds one shared, fixed `n2-standard-2` web load generator in that zone.

The GCP storage profile is derived from the selected machine type. Most
supported types use a zonal `pd-balanced` Persistent Disk, whose performance
scales with disk size and VM vCPU count. C4A uses Hyperdisk Balanced for both
boot and optional `/data` disks at an explicit baseline of 3,000 IOPS and
140 MiB/s, with NVMe disks and gVNIC as required by the machine series. The app
records that resolved profile in every benchmark result; it does not accept a
client-supplied disk or NIC override. C4A `-lssd`, bare-metal, and accelerator
variants remain excluded. Other families that require an unimplemented
Hyperdisk profile, bundled Local SSD, or accelerators remain hidden. Discovery
also lists only machine types whose Compute Engine metadata reports an explicit
x86_64 or Arm64 architecture; legacy types with unspecified architecture stay
hidden rather than risking selection of the wrong guest image. That strict rule
continues to apply to customer-selected targets. The fixed
`n2-standard-2` web load generator is a separate, documented x86_64 contract:
when its Compute API response omits `architecture`, the app uses x86_64 only
for that exact load-generator type and rejects any explicit contradictory
architecture value.

C4A launches consume the regional C4A VM-family vCPU quota plus Hyperdisk
Balanced capacity, IOPS, and throughput quota. A quota failure is distinct from
a temporary zonal-capacity failure: the former needs a quota adjustment or a
smaller plan, while the latter can often be retried in another advertised zone.

## Microsoft Azure setup

Sign in locally, select the subscription, and confirm it before choosing Azure
in the form:

```bash
az login
az account set --subscription SUBSCRIPTION_ID
az account show
az provider register --namespace Microsoft.Compute --wait
az provider register --namespace Microsoft.Network --wait
az vm image terms accept --urn resf:rockylinux-x86_64:9-base:latest
az vm image terms accept --urn resf:rockylinux-aarch64:rockylinux-aarch64-9:latest
```

Set `AZURE_SUBSCRIPTION_ID` to preselect a subscription and
`AZURE_DEFAULT_REGION` to override the `eastus2` discovery default. The app
still records an explicit subscription, region, and availability zone in every
Azure plan. The local principal needs permission to create and delete resource
groups plus Compute and Network resources. `Contributor` at subscription scope
is the simplest current configuration because every run creates one dedicated,
tagged resource group and deletes that exact group during cleanup. `Owner` is
not required. The app verifies that `Microsoft.Compute` and
`Microsoft.Network` are registered and that the Rocky Linux marketplace terms
for each selected architecture are accepted before creating a resource group;
it never accepts legal terms automatically.

Azure discovery reads subscription-specific VM SKU capabilities, zones, and
restrictions rather than maintaining a static size list. A `QuotaId`
restriction needs regional or VM-family quota; a size marked unavailable for
the subscription is not repaired by a quota request. Actual zonal capacity can
also fail after discovery and is reported separately.

Azure runs use a pinned Rocky Linux 9 image matching the selected x86_64 or
Arm64 VM. Cobalt `Dpsv6` and `Dplsv6` sizes are exposed when Azure reports them
available. Storage tests use a zonal Premium SSD v2 `/data` disk at its baseline
3,000 IOPS and 125 MiB/s profile. The guest mounts only the persisted Azure LUN
through Azure's stable disk links. Azure iperf3 supports TCP and UDP; SCTP is
hidden and rejected until Azure Virtual Network support is proven.

## Default SSH key pair

To prefill the SSH key fields for local testing, copy `.env.example` to `.env`
and set the paths to a matching private/public key pair:

```dotenv
BENCHMARK_SSH_PRIVATE_KEY_FILE=~/.ssh/id_ed25519
BENCHMARK_SSH_PUBLIC_KEY_FILE=~/.ssh/id_ed25519.pub
```

Relative paths are resolved from the project directory. The `.env` file is
ignored by Git. Key contents are served only to a browser connected through
the local loopback interface and are never written to benchmark reports. The
existing `OCI_BENCHMARK_SSH_*` names remain supported as compatibility aliases.
AWS imports only the public key as a tagged, per-run EC2 key pair and deletes it
during cleanup; AWS EC2 accepts RSA and Ed25519 keys for this flow.

## Important notes

- OCI runs use Oracle Linux. AWS runs use the latest default Amazon Linux 2023 AMI. GCP and Azure runs use a pinned Rocky Linux 9 image for the selected architecture.
- OCI, AWS, and GCP support the full exposed workload set: Sysbench CPU, memory, and file I/O; STREAM; fio; iperf3 TCP, UDP, and SCTP; the curated Phoronix profiles; ApacheBench; DeathStarBench; and CPU-only llama.cpp. Azure supports the same surface except SCTP; its iperf3 choices are TCP and UDP.
- AWS storage tests use an optional encrypted gp3 `/data` volume with selectable capacity and the gp3 baseline of 3,000 IOPS and 125 MiB/s. It is mounted by its exact EBS volume ID with a persistent filesystem UUID and is included in ownership-safe cleanup.
- GCP storage tests use `pd-balanced` on the broadly compatible machine families. C4A uses Hyperdisk Balanced at 3,000 IOPS / 140 MiB/s with NVMe and gVNIC. An untouched C4A `/data` size defaults to 100 GiB; larger plans still require sufficient regional Hyperdisk capacity and performance quota.
- Azure storage tests use a zonal Premium SSD v2 `/data` disk at 3,000 IOPS and 125 MiB/s. Region, zone, VM-size, and disk support are validated before launch.
- Sysbench is selected once, then its CPU, memory, and file I/O workloads can be enabled independently. CPU is the default; file I/O uses the additional `/data` volume.
- iperf3 is selected once and TCP is the default. OCI, AWS, and GCP support TCP, UDP, and SCTP; Azure supports TCP and UDP. Every provider uses a separate peer and targets only its private address. AWS, GCP, and Azure use a same-size peer in the runner's Availability Zone or zone so a small helper does not cap the result. TCP/5201 is retained for control and TCP data, and UDP/5201 is opened only when selected. SCTP rules and guest kernel validation apply only to providers advertising SCTP support.
- llama.cpp runs CPU-only on the selected x86_64 or Arm64 target and uses all detected logical CPUs. It builds release `b10218` at commit `de699957b92f490efebad149665b0dccf127eaff` with GPU backends disabled. x86_64 uses a fixed portable AVX2-era profile with AVX-512 and AMX disabled so every cloud executes the same instruction baseline; Arm64 retains native optimization. It then benchmarks `tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf` from immutable model revision `c1d7cb837a660d93ba28f936efb148591bfba3e9`. Rocky Linux runners use the coherent GCC Toolset 15 compiler/binutils stack; OCI retains GCC Toolset 12 and AWS uses its Amazon Linux toolchain. Before building, every runner records and validates the CPU build profile plus the selected GCC and GNU assembler paths and versions. The validated x86_64 portable and Arm64 native profiles implement one architecture-appropriate CPU benchmark method, so matching workloads can be compared across providers and architectures. Charts show non-blocking provenance warnings with each exact architecture, build profile, and toolchain because ISA-specific kernels and compiler code generation can affect performance; historical or unrecognized build profiles remain separate. The model is Q4_K_M, its expected SHA-256 is `9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0`, and reports verify the build, artifact, CPU backend, zero GPU offload, thread count, toolchain, downloaded file size, and llama-bench payload size.
- Phoronix Test Suite is selected once, then pinned 7-Zip, OpenSSL SHA-256, Linux kernel compilation, and Tinymembench profiles can be enabled independently; 7-Zip is the default. Each profile runs three measured trials with fixed options, structured metrics, and its own report section. The Linux kernel profile has a 60-minute execution limit; the other profiles have a 30-minute limit. The Phoronix client revision and exact profile versions are recorded in the report.
- ApacheBench runs Apache HTTP Server on the customer-selected shape and drives it from the separate web load generator over the provider's private network. New-connection and Keep-Alive workloads are independently selectable; request count, concurrency, response size, warm-up requests, and measured trials are configurable. Reports include throughput, latency percentiles, failures, transfer rate, and load-generator CPU and network saturation warnings.
- DeathStarBench supports Media Microservices, Hotel Reservation, and Social Network. Warm-up, duration, threads, connections, and request rate are selectable; connections and rate must divide evenly across threads. The target needs at least 16 GB of memory.
- ApacheBench and DeathStarBench share one fixed x86 web load generator when both are selected. On AWS it is the first available same-AZ `m7i.large`, `m6i.large`, or `m5.large`, each validated as 2 vCPUs / 8 GiB; on GCP it is a same-zone `n2-standard-2`; on Azure it is a same-zone `Standard_D2as_v7`; those GCP and Azure choices are also validated as 2 vCPUs / 8 GiB. On OCI it is the first available supported x86 load-generator shape configured at 2 OCPUs / 8 GB. DeathStarBench sends wrk2 traffic to the selected benchmark VM's private IP, and its reports include sent/completed/successful requests, HTTP errors, socket error events, achieved throughput, p50/p95/p99 latency, generator utilization, and complete raw output.
- DeathStarBench uses rootful, daemonless Podman on the service VM. Oracle Linux 9 enables Oracle EPEL and uses `/usr/bin/podman-compose`; Amazon Linux 2023 validates the minimum SPAL-capable system release, enables SPAL for Podman, and installs checksum-pinned `podman-compose` 1.5.0 at `/usr/local/bin/podman-compose`; Rocky Linux 9 enables CRB and EPEL 9 and uses `/usr/bin/podman-compose`. The app never invokes Docker or Docker Compose. A verified `podman-docker` compatibility wrapper (or a direct Docker-to-Podman symlink) is permitted but ignored; Docker Engine and real Docker Compose remain blocked. Custom workload images are built natively on the selected x86_64 or aarch64 benchmark VM.
- Account for helper-instance cost when selecting network or web workloads. A normal run uses one target VM; iperf3 adds one peer; either or both web benchmarks add one shared load generator. Combining iperf3 with ApacheBench and/or DeathStarBench therefore uses three VMs total, with corresponding boot disks, addresses, and network usage. Charges continue for retained resources until they are destroyed.
- The required SSH public/private key pair is verified before provisioning. Private-key material is never persisted or written to a report; a retained-run key can be downloaded only from the same live page session that submitted it.
- Benchmark results are comparative measurements, not cloud service guarantees. Each report records configuration, tool versions, commands, and raw output.

## Safety

Every managed resource carries a job ownership contract. OCI and AWS use
tags; GCP uses labels where supported and deterministic names, ownership
descriptions, persisted numeric IDs, and parent relationships for VPC resources
that do not support labels. Azure uses a deterministic, tagged, app-exclusive
resource group persisted before the first create. Use **Destroy** in the report screen to remove a
retained resource stack. Failed provisioning attempts are cleaned up where
possible, and created AWS, GCP, and Azure resource identities are persisted immediately
so an interrupted run can be recovered and destroyed after restarting the app.

The cloud API rejects non-loopback clients. Keep Uvicorn bound to its default
`127.0.0.1` address; this app is not designed to be exposed as a shared web
service.

The current AWS implementation creates a dedicated VPC and public subnet and,
as explicitly requested for initial testing, allows SSH TCP/22 to the runner
and any web load generator from `0.0.0.0/0`. Each guest still requires the
submitted private key and disables password authentication. An iperf3 peer
receives a temporary public address for outbound Amazon Linux package setup,
but its security group permits no public ingress and the benchmark uses only
its private address. Web service ports are reachable only from the web load
generator's security group. This ingress scope should be narrowed before using
the app beyond controlled testing. Leave **Destroy all infrastructure** checked
unless you intentionally want to retain the EC2 resources and network.

The current GCP implementation likewise creates a dedicated VPC/subnet and
allows SSH TCP/22 to the runner and any web load generator from `0.0.0.0/0`.
The submitted private key is still required and guest password authentication
is disabled. The private iperf3 peer has no public ingress; its benchmark
listener accepts traffic only from the runner network tag. Web service ports
accept traffic only from the load-generator network tag. This broad SSH rule is
temporary initial-testing behavior: keep **Destroy all infrastructure**
checked, or use **Destroy** promptly after inspecting a retained or recovered
run.

Azure creates a dedicated resource group and VNet/subnet for each run. The
runner, an optional iperf3 peer, and any web load generator receive Standard
public IPs for guest setup and permit SSH TCP/22 from `0.0.0.0/0`, matching the
current controlled-testing policy. Benchmark peer and web workload traffic
uses private VNet addresses and subnet-scoped NSG rules. Cleanup verifies the
saved subscription, exact resource-group identity, location, ownership tags,
and contained-resource allowlist before deleting the group; foreign or
ambiguous resources fail closed instead of being removed.
