#!/bin/bash
set -euo pipefail # Enable strict error handling

SCRIPT_DIR="$(
    cd "$(dirname "$0")"
    pwd -P
)"
# Self-contained on purpose: this directory's contents are also published on their own,
# at the root of the public datalake-playground repo, where nothing else exists.
if ! command -v docker >/dev/null 2>&1; then
    echo "ERROR: Docker is not installed. Please install docker and rerun." >&2
    exit 1
fi
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    echo "ERROR: Docker Compose is not installed (neither 'docker compose' nor 'docker-compose')." >&2
    exit 1
fi
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: The docker daemon is not running or accessible. Please start docker and rerun." >&2
    exit 1
fi

# The compose files read ${PLATFORM} for every service. The published images are
# linux/amd64 only, so that is the default: on Apple Silicon they run under emulation,
# and asking for linux/arm64 would fail the pull with "no matching manifest". Set
# PLATFORM=linux/arm64 only if you built arm64 images yourself.
PLATFORM="${PLATFORM:-linux/amd64}"
export PLATFORM

# Spark 3 and Spark 4 are separate images: ranga-spark keeps the original name so older
# pulls keep working, and ranga-spark4 is the new line. Derive which one this
# SPARK_VERSION wants (default 3.5.x); an explicit SPARK_IMAGE still wins.
if [ -z "${SPARK_IMAGE:-}" ]; then
    case "${SPARK_VERSION:-}" in
    4.*) SPARK_IMAGE="ranga-spark4" ;;
    *) SPARK_IMAGE="ranga-spark" ;;
    esac
fi
export SPARK_IMAGE

# PROFILE=core (default) starts docker-compose.yml.
# PROFILE=all starts docker-compose_all.yml, which adds MySQL, Trino and Jupyter.
PROFILE="${PROFILE:-core}"
case "$PROFILE" in
core) COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml" ;;
all) COMPOSE_FILE="$SCRIPT_DIR/docker-compose_all.yml" ;;
*)
    echo "Error: Invalid PROFILE '$PROFILE'. Expected 'core' or 'all'."
    exit 1
    ;;
esac

state=${1:-"start"}
state=$(echo "$state" | tr '[:upper:]' '[:lower:]')

compose() {
    # Word splitting on COMPOSE_CMD is intentional: it is either "docker compose" or "docker-compose".
    # shellcheck disable=SC2086
    $COMPOSE_CMD -f "$COMPOSE_FILE" "$@"
}

start_datalake() {
    echo "Starting Datalake services ($PROFILE profile)..."
    compose up -d
    echo "Datalake services are started."
}

stop_datalake() {
    echo "Stopping Datalake services ($PROFILE profile)..."
    # --remove-orphans: a plain "down" leaves behind any container whose service has
    # since been deleted from the compose file. Those keep running, keep their ports and
    # keep their image pinned, so a removed component looks removed in git and is still
    # up in Docker.
    compose down --remove-orphans
    echo "Datalake services are stopped."
}

restart_datalake() {
    stop_datalake
    start_datalake
}

status_datalake() {
    compose ps
}

logs_datalake() {
    shift || true
    compose logs -f --tail=100 "$@"
}

validate_datalake() {
    echo "Validating $COMPOSE_FILE ..."
    compose config -q
    echo "OK: $COMPOSE_FILE is a valid compose project."
}

case $state in
start)
    validate_datalake
    start_datalake
    ;;
stop)
    stop_datalake
    ;;
restart)
    restart_datalake
    ;;
status | ps)
    status_datalake
    ;;
logs)
    logs_datalake "$@"
    ;;
validate | config)
    validate_datalake
    ;;
*)
    echo "Error: Invalid state '$state'. Usage: $0 {start|stop|restart|status|logs [service...]|validate}"
    echo "       Set PROFILE=all to include MySQL, Trino and Jupyter."
    exit 1
    ;;
esac
