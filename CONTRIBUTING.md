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

The stable `Required CI` check summarizes the complete matrix and is the check
that should be required before merging to `main`.
