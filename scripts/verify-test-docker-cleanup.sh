#!/usr/bin/env bash
set -euo pipefail

readonly owner_label_key='io.dsub.test-owner'
readonly owner_label_value='go-open-discogs-batch'
readonly run_label_key='io.dsub.test-run'
readonly run_id_env='OPEN_DISCOGS_TEST_RUN_ID'
readonly run_id_max_length=128

run_id="${OPEN_DISCOGS_TEST_RUN_ID:-}"
if [[ -z "$run_id" ]]; then
  printf '%s must be set for Docker cleanup verification\n' "$run_id_env" >&2
  exit 2
fi
if (( ${#run_id} > run_id_max_length )) ||
  [[ ! "$run_id" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
  printf '%s has an invalid value\n' "$run_id_env" >&2
  exit 2
fi

readonly owner_filter="label=${owner_label_key}=${owner_label_value}"
readonly run_filter="label=${run_label_key}=${run_id}"

containers="$(
  docker container ls --all --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
)"
networks="$(
  docker network ls --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
)"
volumes="$(
  docker volume ls --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
)"

status=0
if [[ -n "$containers" ]]; then
  printf 'test Docker containers remain for run %s:\n%s\n' "$run_id" "$containers" >&2
  status=1
fi
if [[ -n "$networks" ]]; then
  printf 'test Docker networks remain for run %s:\n%s\n' "$run_id" "$networks" >&2
  status=1
fi
if [[ -n "$volumes" ]]; then
  printf 'test Docker volumes remain for run %s:\n%s\n' "$run_id" "$volumes" >&2
  status=1
fi

if (( status != 0 )); then
  exit "$status"
fi

printf 'test Docker cleanup verified for run %s: 0 containers, 0 networks, 0 volumes\n' "$run_id"
