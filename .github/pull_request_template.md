## What changed

<!-- Describe the behavior, not only the files changed. -->

## Why

<!-- What problem or request does this address? -->

## Validation

- [ ] `PYTHON_BIN=.venv/bin/python scripts/check.sh`
- [ ] Automated tests were added or updated where appropriate

Additional manual validation:

<!-- Include relevant browser checks or sanitized benchmark run IDs. -->

## Cloud and resource impact

Affected providers and resources:

<!-- Write "None" or list OCI, AWS, GCP, Azure, and the resource types. -->

Cleanup, cancellation, or recovery considerations:

<!-- Required for resource lifecycle changes; otherwise write "None". -->

## Security checklist

- [ ] No credentials, tokens, private keys, `.env`, or saved run artifacts are included
- [ ] Pull-request CI remains credential-free and does not provision cloud resources
- [ ] New external downloads are pinned and integrity-checked where practical
