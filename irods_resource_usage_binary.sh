#!/bin/bash

# iRODS Client-Side Resource Usage Test Script
# Measures CPU usage (%) and memory usage (RSS) for iput and iget operations

# Configuration
TEST_RUNS=10
TEST_FILE_SIZE="100M"
RESULTS_DIR=${1:-"./resource_usage_results"}
TIMESTAMP=${2:-$(date -u '+%Y-%m-%d_%H-%M-%S')}
CONTAINER_NAME="ubuntu-2204-postgres-14_irods-catalog-provider_1"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== iRODS Client-Side Resource Usage Test ===${NC}"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Test runs: $TEST_RUNS"
echo "Test file size: $TEST_FILE_SIZE"
echo

# Create results directory
mkdir -p "$RESULTS_DIR"

# Check if container is running
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo -e "${RED}Error: iRODS container is not running!${NC}"
    exit 1
fi

# Check iRODS connectivity
if ! docker exec -u irods $CONTAINER_NAME ils >/dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to iRODS inside container!${NC}"
    exit 1
fi

# Generate test file
TEST_FILE="test_file_${TEST_FILE_SIZE}_${RANDOM}.dat"
truncate -s "$TEST_FILE_SIZE" "$TEST_FILE"

# Copy file into container
docker cp "$TEST_FILE" "$CONTAINER_NAME:/tmp/$TEST_FILE"

# Create script inside container for resource measurement
docker exec $CONTAINER_NAME bash -c "cat > /tmp/resource_usage.sh << 'EOF'
#!/bin/bash
FILE=\$1
IRODS_PATH=\$2
ACTION=\$3  # 'iput' or 'iget'

export irodsProt=0  # Binary mode
TIMEFILE=/tmp/\${ACTION}_time_output.txt

if [[ \$ACTION == \"iput\" ]]; then
    /usr/bin/time -v iput \"\$FILE\" \"\$IRODS_PATH\" 2> \"\$TIMEFILE\"
else
    /usr/bin/time -v iget -f \"\$IRODS_PATH\" \"/tmp/downloaded_\$(basename \$FILE)\" 2> \"\$TIMEFILE\"
fi

cat \"\$TIMEFILE\"
EOF

chmod +x /tmp/resource_usage.sh
"

# Main loop
echo -e "${YELLOW}Running $TEST_RUNS iterations...${NC}"

for i in $(seq 1 "$TEST_RUNS"); do
    echo -e "${YELLOW}Run $i/$TEST_RUNS${NC}"
    IRODS_PATH="/tempZone/home/rods/${TEST_FILE}_${i}"

    # Upload (iput)
    echo -e "  ${GREEN}Measuring iput...${NC}"
    docker exec -u irods $CONTAINER_NAME bash /tmp/resource_usage.sh "/tmp/$TEST_FILE" "$IRODS_PATH" "iput" > "${RESULTS_DIR}/iput_run_${i}.txt"
    # Download (iget)
    echo -e "  ${GREEN}Measuring iget...${NC}"
    docker exec -u irods $CONTAINER_NAME bash /tmp/resource_usage.sh "/tmp/$TEST_FILE" "$IRODS_PATH" "iget" > "${RESULTS_DIR}/iget_run_${i}.txt"
    # Clean up iRODS file
    docker exec -u irods $CONTAINER_NAME irm -f "$IRODS_PATH"
done
# Clean up local test file
rm -f "$TEST_FILE"

echo
echo -e "${GREEN}Raw resource usage results saved. Processing results...${NC}"

# === CALL TO PROCESSING SCRIPT HERE ===
./process_resource_usage_binary.sh "$RESULTS_DIR" "$TIMESTAMP" "$TEST_RUNS" "$TEST_FILE_SIZE" "$CONTAINER_NAME"

echo
echo -e "${GREEN}Resource usage test completed!${NC}"

