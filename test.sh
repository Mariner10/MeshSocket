#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

cleanup() {
    echo "Tearing down Docker services..."
    docker compose down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting Python mesh server + echo client..."
docker compose up -d --build

echo "Waiting for server to be ready..."
for i in $(seq 1 30); do
    if nc -z localhost 8765 2>/dev/null; then
        echo "Server is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "Server failed to start within 30 seconds."
        docker compose logs
        exit 1
    fi
    sleep 1
done

# Give echo client a moment to connect
sleep 2

echo "Running Swift tests..."
cd Swift
MESH_AUTH_TOKEN=test-token swift test
echo "All tests passed."
