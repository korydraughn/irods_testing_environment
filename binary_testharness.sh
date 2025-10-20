#!/bin/bash
set -e

TIMESTAMP=$(date -u '+%Y-%m-%d_%H-%M-%S')
RESULTS_BASE_DIR="./test_results_$TIMESTAMP"

mkdir -p "$RESULTS_BASE_DIR/resource_usage"
mkdir -p "$RESULTS_BASE_DIR/performance"

echo "=== Starting iRODS Resource Usage Test ==="
bash ./irods_resource_usage_binary.sh "$RESULTS_BASE_DIR/resource_usage" "$TIMESTAMP"

echo
echo "=== Starting iRODS Performance Test ==="
bash ./irods_binary_testing.sh "$RESULTS_BASE_DIR/performance" "$TIMESTAMP"

echo
echo "=== All tests complete ==="
echo "Results saved under $RESULTS_BASE_DIR"

