# Contributing

Thank you for helping improve Cloud Self-Service Benchmarks. Changes should be
small enough to review, covered by tests, and explicit about any effect on
cloud resources or cleanup behavior.

## Development setup

Create a virtual environment and install the application dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run the same checks used by GitHub Actions before opening a pull request:

```bash
PYTHON_BIN=.venv/bin/python scripts/check.sh
```

The CI matrix checks Python 3.10 and 3.14, which represent the supported range.
Node.js 22 is used for the JavaScript syntax and comparison UI tests. Tests
whose filenames contain `integration` use mocked or injected cloud clients;
the pull-request workflow does not authenticate to a provider or create cloud
resources.

The separate candidate-artifact job checks out the exact audited DeathStarBench
revision and recursive LuaJIT source, verifies a checksum-pinned LuaSocket rock,
and prepares Hotel/Media artifacts twice. It runs Go correctness tests with the
pinned native builder (network disabled, input read-only), and requires LuaJIT
semantic harness tests against the actual prepared scripts. It uploads only
source-preparation receipts; it does not publish images or qualify live cloud
workloads. Local fixture tests do not require a container runtime or interpreter;
the interpreter-backed gate is mandatory in this CI job. See the
[workload expansion checkpoint](docs/deathstarbench-workload-expansion.md) for
the local preparation command and remaining release blockers.

Hotel/Media image publication is a separate maintainer-triggered workflow,
using native x86/Arm runners and package-write permission only for explicit
publication. It is never triggered by PR CI. Candidate locks do not enable
either workload, and authenticated image pulls do not satisfy public-access
qualification. See the [artifact checkpoint](docs/deathstarbench-workload-expansion.md#native-image-publication-checkpoint).

## Pull requests

In the pull request description:

- Explain the user-visible behavior and why it should change.
- List the automated checks and any manual validation performed.
- Identify every affected cloud provider and resource type.
- Describe cleanup, cancellation, and recovery behavior when resource
  lifecycle code changes.
- Include or update tests and documentation for changed behavior.

Do not commit credentials, `.env`, SSH private keys, cloud configuration,
access tokens, or saved run artifacts. Do not add cloud credentials to the
pull-request workflow. Live cloud validation is maintainer-controlled and is
performed only after the proposed code and its resource lifecycle have been
reviewed.

## Test expectations

Add focused tests alongside a change. Provider changes should normally cover:

- plan validation and capability discovery;
- mocked provider API behavior and ownership checks;
- generated guest commands or cloud-init;
- failure, cancellation, and cleanup paths; and
- relevant form, history, report, or comparison behavior.

The stable `Required CI` check summarizes the complete Python matrix and
candidate-artifact preparation job and is the check that should be required
before merging to `main`.
