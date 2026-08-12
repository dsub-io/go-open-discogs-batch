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
readonly expected_labels="${owner_label_value}|${run_id}"

list_containers() {
  docker container ls --all --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
}

list_networks() {
  docker network ls --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
}

list_volumes() {
  docker volume ls --quiet \
    --filter "$owner_filter" \
    --filter "$run_filter"
}

inspect_labels() {
  local resource_type="$1"
  local resource_id="$2"

  case "$resource_type" in
    container)
      docker container inspect --format \
        '{{ index .Config.Labels "io.dsub.test-owner" }}|{{ index .Config.Labels "io.dsub.test-run" }}' \
        "$resource_id"
      ;;
    network)
      docker network inspect --format \
        '{{ index .Labels "io.dsub.test-owner" }}|{{ index .Labels "io.dsub.test-run" }}' \
        "$resource_id"
      ;;
    volume)
      docker volume inspect --format \
        '{{ index .Labels "io.dsub.test-owner" }}|{{ index .Labels "io.dsub.test-run" }}' \
        "$resource_id"
      ;;
    *)
      printf 'unsupported Docker resource type: %s\n' "$resource_type" >&2
      return 2
      ;;
  esac
}

remove_owned_resource() {
  local resource_type="$1"
  local resource_id="$2"
  local actual_labels

  if ! actual_labels="$(inspect_labels "$resource_type" "$resource_id")"; then
    printf 'refusing to remove uninspectable Docker %s %s\n' \
      "$resource_type" "$resource_id" >&2
    return 1
  fi
  if [[ "$actual_labels" != "$expected_labels" ]]; then
    printf 'refusing to remove Docker %s %s with labels %s\n' \
      "$resource_type" "$resource_id" "$actual_labels" >&2
    return 1
  fi

  case "$resource_type" in
    container)
      docker container rm --force "$resource_id" >/dev/null
      ;;
    network)
      docker network rm "$resource_id" >/dev/null
      ;;
    volume)
      docker volume rm "$resource_id" >/dev/null
      ;;
  esac
}

status=0
while IFS= read -r resource_id; do
  [[ -z "$resource_id" ]] && continue
  remove_owned_resource container "$resource_id" || status=1
done < <(list_containers)

while IFS= read -r resource_id; do
  [[ -z "$resource_id" ]] && continue
  remove_owned_resource network "$resource_id" || status=1
done < <(list_networks)

while IFS= read -r resource_id; do
  [[ -z "$resource_id" ]] && continue
  remove_owned_resource volume "$resource_id" || status=1
done < <(list_volumes)

containers="$(list_containers)"
networks="$(list_networks)"
volumes="$(list_volumes)"

if [[ -n "$containers" ]]; then
  printf 'owned test Docker containers remain for run %s:\n%s\n' \
    "$run_id" "$containers" >&2
  status=1
fi
if [[ -n "$networks" ]]; then
  printf 'owned test Docker networks remain for run %s:\n%s\n' \
    "$run_id" "$networks" >&2
  status=1
fi
if [[ -n "$volumes" ]]; then
  printf 'owned test Docker volumes remain for run %s:\n%s\n' \
    "$run_id" "$volumes" >&2
  status=1
fi

if (( status != 0 )); then
  exit "$status"
fi

printf 'test Docker cleanup verified for run %s: 0 containers, 0 networks, 0 volumes\n' "$run_id"
