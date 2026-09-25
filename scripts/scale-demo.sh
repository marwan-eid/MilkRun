#!/usr/bin/env bash
# Runs the production stack with two backend instances and checks that they
# split the work and each serve the whole fleet, then stops one and checks
# that the other takes over.
#
#   VAN_COUNT=10 scripts/scale-demo.sh
#
# Needs Docker Compose v2 and python3. Extra override files (for example to
# move Caddy off ports 80/443) can be passed in EXTRA_COMPOSE_FILES.
set -euo pipefail
cd "$(dirname "$0")/.."

export COMPOSE_FILE="docker-compose.prod.yml:docker-compose.scale.yml${EXTRA_COMPOSE_FILES:+:$EXTRA_COMPOSE_FILES}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-milkrun-scale}"
export VAN_COUNT="${VAN_COUNT:-10}"

count() { python3 -c 'import sys,json; print(len(json.load(sys.stdin)))'; }
backends() { docker compose ps -q backend; }
api() { docker exec "$1" wget -qO- "http://localhost:8080$2"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
consumers() {
    docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
        --describe --group milkrun-engine --members | awk '$1 == "milkrun-engine"' | wc -l
}

echo "==> Starting the stack with 2 backend instances and $VAN_COUNT vans"
docker compose up -d --build --scale backend=2 >/dev/null

echo "==> Waiting for both backends"
for c in $(backends); do
    for _ in $(seq 1 60); do api "$c" /api/observability/live >/dev/null 2>&1 && break; sleep 3; done
    api "$c" /api/observability/live >/dev/null || fail "backend $c did not start"
done

echo "==> Waiting for every van to be on the road"
first=$(backends | head -1)
for _ in $(seq 1 60); do
    [ "$(api "$first" /api/vans | count)" -ge "$VAN_COUNT" ] && break
    sleep 5
done

echo "==> Partition assignment of the GPS consumer group"
docker compose exec -T kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
    --describe --group milkrun-engine --members --verbose | awk '$1 == "milkrun-engine" {print "    " $0}'
members=$(consumers)
[ "$members" -eq 2 ] || fail "expected 2 consumers in milkrun-engine, found $members"

echo "==> Each instance processes part of the fleet but serves all of it"
for c in $(backends); do
    vans=$(api "$c" /api/vans | count)
    processed=$(api "$c" /actuator/prometheus | awk '/^milkrun_events_processed_total/ {print $2}')
    echo "    $(docker inspect -f '{{.Name}}' "$c"): serves $vans vans, processed $processed GPS events itself"
    [ "$vans" -ge "$VAN_COUNT" ] || fail "instance $c serves only $vans of $VAN_COUNT vans"
done

victim=$(backends | head -1)
survivor=$(backends | tail -1)
echo "==> Stopping $(docker inspect -f '{{.Name}}' "$victim")"
docker stop "$victim" >/dev/null
sleep 45

members=$(consumers)
[ "$members" -eq 1 ] || fail "expected 1 consumer after failover, found $members"
fresh=$(api "$survivor" /api/vans | python3 -c '
import sys, json, re
from datetime import datetime, timezone
now = datetime.now(timezone.utc)
def age(ts):  # drop fractional seconds: Java writes up to 9 digits
    return (now - datetime.fromisoformat(re.sub(r"\.\d+", "", ts).replace("Z", "+00:00"))).total_seconds()
print(sum(1 for v in json.load(sys.stdin) if age(v["last_updated"]) < 15))')
echo "    survivor: $fresh vans updated in the last 15 s"
[ "$fresh" -ge "$VAN_COUNT" ] || fail "only $fresh of $VAN_COUNT vans are being updated after failover"

docker start "$victim" >/dev/null
echo "PASS: work is split across instances, each serves the whole fleet, and one instance takes over when the other stops"
echo "(stop with: COMPOSE_FILE=$COMPOSE_FILE COMPOSE_PROJECT_NAME=$COMPOSE_PROJECT_NAME docker compose down -v)"
