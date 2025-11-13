#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
COMPOSE_DIR="$REPO_ROOT/tests/integrations"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"

cleanup() {
  docker compose -f "$COMPOSE_FILE" down -v >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "Starting Neo4j test container..."
docker compose -f "$COMPOSE_FILE" up -d >/dev/null

CONTAINER_ID=$(docker compose -f "$COMPOSE_FILE" ps -q neo4j)
if [[ -z "$CONTAINER_ID" ]]; then
  echo "Failed to start Neo4j container"
  exit 1
fi

echo "Waiting for Neo4j to become healthy..."
for attempt in {1..30}; do
  STATUS=$(docker inspect --format '{{.State.Health.Status}}' "$CONTAINER_ID") || STATUS="unknown"
  if [[ "$STATUS" == "healthy" ]]; then
    echo "Neo4j is healthy."
    break
  fi
  if [[ $attempt -eq 30 ]]; then
    echo "Neo4j container did not become healthy in time"
    exit 1
  fi
  sleep 3
done

export NEO4J_BOLT_URL="bolt://localhost:7687"
export NEO4J_USER="neo4j"
export NEO4J_PASSWORD="password"

export NEPTUNE_BOLT_URL="$NEO4J_BOLT_URL"
export NEPTUNE_USER="$NEO4J_USER"
export NEPTUNE_PASSWORD="$NEO4J_PASSWORD"

echo "Running Neo4j integration tests..."
pipenv run test_neo4j

echo "Running Neptune integration tests (using Neo4j container)..."
pipenv run test_neptune
