#!/usr/bin/env bash

set -euo pipefail

python_bin="${PYTHON_BIN:-python3}"

"${python_bin}" -m pip check
"${python_bin}" -m compileall -q app tests

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js is required for the comparison UI tests." >&2
  exit 1
fi

while IFS= read -r javascript_file; do
  node --check "${javascript_file}"
done < <(find app/static -maxdepth 1 -type f -name '*.js' -print | sort)

while IFS= read -r shell_file; do
  bash -n "${shell_file}"
done < <(find scripts -maxdepth 1 -type f -name '*.sh' -print | sort)

"${python_bin}" -m unittest discover -s tests -v
