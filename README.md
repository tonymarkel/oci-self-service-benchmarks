# OCI Self-Service Benchmarks

A local web application that provisions an Oracle Linux VM using the default OCI profile, runs selected open-source benchmarks, produces HTML reports, and can destroy all resources it created.

## Run locally

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
```

Open `http://127.0.0.1:8000`. The process reads `~/.oci/config` using the `DEFAULT` profile. The OCI principal must be allowed to create VCNs, compute instances, volumes, and public IPs in the chosen compartment.

## Default SSH key pair

To prefill the SSH key fields for local testing, copy `.env.example` to `.env`
and set the paths to a matching private/public key pair:

```dotenv
OCI_BENCHMARK_SSH_PRIVATE_KEY_FILE=~/.ssh/id_ed25519
OCI_BENCHMARK_SSH_PUBLIC_KEY_FILE=~/.ssh/id_ed25519.pub
```

Relative paths are resolved from the project directory. The `.env` file is
ignored by Git. Key contents are served only to a browser connected through
the local loopback interface and are never written to benchmark reports.

## Important notes

- Oracle Linux is the only supported guest OS in this release.
- Network tests automatically create a small private `VM.Standard.E5.Flex` peer (1 OCPU / 8 GB) and run `iperf3` over the private subnet. This makes the measured connection explicit in the report.
- DeathStarBench supports Media Microservices, Hotel Reservation, and Social Network. Warm-up, duration, threads, connections, and request rate are selectable; connections and rate must divide evenly across threads. The target needs at least 16 GB of memory.
- A separate fixed x86 load-generator VM (2 OCPUs / 8 GB) sends wrk2 traffic to the selected benchmark VM's private IP. Reports include sent/completed/successful requests, HTTP errors, socket error events, achieved throughput, p50/p95/p99 latency, generator utilization, and complete raw output.
- DeathStarBench uses rootful, daemonless Podman and `/usr/bin/podman-compose` on Oracle Linux 9. The app never invokes Docker or Docker Compose. A verified `podman-docker` compatibility wrapper (or a direct Docker-to-Podman symlink) is permitted but ignored; Docker Engine and real Docker Compose remain blocked. Custom workload images are built natively on the selected x86_64 or aarch64 benchmark VM.
- The required SSH public/private key pair is verified before provisioning. Private-key material is never persisted or written to a report; a retained-run key can be downloaded only from the same live page session that submitted it.
- Benchmark results are comparative measurements, not OCI service guarantees. Each report records configuration, tool versions, commands, and raw output.

## Safety

Every resource is tagged with the job identifier. Use **Destroy** in the report screen to remove the resource stack when a run is retained. Failed provisioning attempts are cleaned up automatically where possible.
