#!/usr/bin/env bash
# Delete every AVAILABLE VCN in one OCI compartment.
#
# OCI can delete a VCN only after its dependent resources (subnets, gateways,
# DRG attachments, etc.) have been removed. Any VCN that is not empty is
# reported as a failure and the script continues with the remaining VCNs.

set -uo pipefail

usage() {
  cat <<'EOF'
Usage: delete-compartment-vcns.sh --compartment-id OCID [--profile PROFILE] [--execute]

Lists all AVAILABLE VCNs in the compartment. By default this is a dry run.
Pass --execute and type the confirmation phrase to submit deletions.

Requirements: OCI CLI configured with permission to list and delete VCNs, jq.
EOF
}

compartment_id=""
profile=""
execute=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compartment-id)
      compartment_id="${2:-}"
      shift 2
      ;;
    --profile)
      profile="${2:-}"
      shift 2
      ;;
    --execute)
      execute=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'Unknown option: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$compartment_id" ]]; then
  printf '%s\n' '--compartment-id is required.' >&2
  usage >&2
  exit 2
fi

command -v oci >/dev/null || { printf '%s\n' 'OCI CLI is required.' >&2; exit 1; }
command -v jq >/dev/null || { printf '%s\n' 'jq is required.' >&2; exit 1; }

oci_args=()
if [[ -n "$profile" ]]; then
  oci_args+=(--profile "$profile")
fi

vcn_json="$(oci network vcn list --compartment-id "$compartment_id" --output json)"
vcn_ids=()
while IFS= read -r vcn_id; do
  [[ -n "$vcn_id" ]] && vcn_ids+=("$vcn_id")
done < <(jq -r '.data[] | select(."lifecycle-state" == "AVAILABLE") | .id' <<<"$vcn_json")

if (( ${#vcn_ids[@]} == 0 )); then
  printf 'No AVAILABLE VCNs found in compartment %s.\n' "$compartment_id"
  exit 0
fi

printf 'Found %d AVAILABLE VCN(s) in compartment %s:\n' "${#vcn_ids[@]}" "$compartment_id"
printf '  %s\n' "${vcn_ids[@]}"

if [[ "$execute" != true ]]; then
  printf '%s\n' 'Dry run only. Re-run with --execute to delete these VCNs.'
  exit 0
fi

read -r -p 'Type DELETE-ALL-VCNS to continue: ' confirmation
if [[ "$confirmation" != 'DELETE-ALL-VCNS' ]]; then
  printf '%s\n' 'Confirmation did not match; no VCNs were deleted.'
  exit 0
fi

failed=0
for vcn_id in "${vcn_ids[@]}"; do
  printf 'Deleting %s...\n' "$vcn_id"
  if ! oci network vcn delete --force --vcn-id "$vcn_id"; then
    printf 'Failed to delete %s (it may still contain dependent resources).\n' "$vcn_id" >&2
    ((failed += 1))
  fi
done

if (( failed > 0 )); then
  printf '%d VCN deletion(s) failed.\n' "$failed" >&2
  exit 1
fi

printf '%s\n' 'Deletion requests submitted successfully.'