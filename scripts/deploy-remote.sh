#!/usr/bin/env bash
# Runs on the VM, invoked over SSH by .github/workflows/deploy.yml.
#
#   deploy-remote.sh <app-dir> <commit-sha>
#
# Moves the checkout to the tested commit, pulls the images CI built for that
# commit, restarts the stack and waits for the backend to report healthy. If it
# doesn't come up, the previously deployed images are started again.
set -euo pipefail

APP_DIR="$1"
SHA="$2"
COMPOSE="docker compose"

cd "$APP_DIR"

# Compose reads COMPOSE_FILE from the environment or from .env; the VM can set
# it in .env to add override files (e.g. docker-compose.prod.yml:monitoring.yml).
if [ -z "${COMPOSE_FILE:-}" ] && ! grep -qs "^COMPOSE_FILE=" .env; then
    export COMPOSE_FILE=docker-compose.prod.yml
fi

echo "==> Updating checkout to $SHA"
git fetch --quiet origin master
git merge --ff-only --quiet "$SHA"

PREVIOUS_TAG="$(cat .deployed-tag 2>/dev/null || echo latest)"

start() {
    MILKRUN_IMAGE_TAG="$1" $COMPOSE up -d --remove-orphans
}

healthy() {
    # The backend publishes no host port; ask it from inside its container.
    for _ in $(seq 1 45); do
        if $COMPOSE exec -T backend wget -qO- http://localhost:8080/api/observability/live 2>/dev/null | grep -q UP; then
            return 0
        fi
        sleep 4
    done
    return 1
}

echo "==> Pulling images for $SHA"
MILKRUN_IMAGE_TAG="$SHA" $COMPOSE pull backend simulator frontend

echo "==> Starting $SHA (previous: $PREVIOUS_TAG)"
start "$SHA"

if healthy; then
    echo "$SHA" > .deployed-tag
    docker image prune -f > /dev/null
    echo "==> Deployed $SHA"
else
    echo "!! Backend did not become healthy; rolling back to $PREVIOUS_TAG" >&2
    $COMPOSE logs --tail 80 backend >&2 || true
    start "$PREVIOUS_TAG"
    exit 1
fi
