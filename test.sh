#!/bin/bash

# iRODS Performance Test Script (Corrected for supported options)

# Configuration

TEST_RUNS=25
TEST_FILE_SIZE="100M"
RESULTS_DIR=${1:-"./performance_results"}
TIMESTAMP=${2:-$(date -u '+%Y-%m-%d_%H-%M-%S')}
CONTAINER_NAME="irods_provider"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

mkdir -p "$RESULTS_DIR"

# Check container

if ! docker ps | grep -q $CONTAINER_NAME; then
echo -e "${RED}Error: iRODS container not running!${NC}"
exit 1
fi

# Helper functions (timing iput/iget)

time_iput() {
local src="$1"
local dest="$2"
local start end
start=$(date +%s.%N)
iput -f "$src" "$dest"    # -f to overwrite
end=$(date +%s.%N)
echo "$start $end"
}

time_iget() {
local src="$1"
local dest="$2"
local start end
start=$(date +%s.%N)
iget -f "$src" "$dest"    # -f to overwrite
end=$(date +%s.%N)
echo "$start $end"
}

# Main test function

run_performance_test() {
local test_file="test_file_${TEST_FILE_SIZE}_${RANDOM}.dat"
local irods_path="/tempZone/home/rods/${test_file}"

```
echo -e "${YELLOW}Creating test file (${TEST_FILE_SIZE})...${NC}"
truncate -s $TEST_FILE_SIZE "$test_file"
file "$test_file"

declare -a upload_times
declare -a download_times

for i in $(seq 1 $TEST_RUNS); do
    echo -n "Run $i/$TEST_RUNS: "

    # Copy test file into container
    docker cp "$test_file" "$CONTAINER_NAME:/host_data/$test_file"

    # Upload
    echo -n "Upload... "
    timing=$(docker exec -u irods $CONTAINER_NAME bash -c "cd /host_data && $(declare -f time_iput); time_iput $test_file $irods_path")
    upload_start=$(echo $timing | awk '{print $1}')
    upload_end=$(echo $timing | awk '{print $2}')
    upload_time=$(echo "$upload_end - $upload_start" | bc -l)
    upload_times+=($upload_time)

    # Download
    echo -n "Download... "
    timing=$(docker exec -u irods $CONTAINER_NAME bash -c "$(declare -f time_iget); time_iget $irods_path /tmp/downloaded_$test_file")
    download_start=$(echo $timing | awk '{print $1}')
    download_end=$(echo $timing | awk '{print $2}')
    download_time=$(echo "$download_end - $download_start" | bc -l)
    download_times+=($download_time)

    # Cleanup iRODS object
    docker exec -u irods $CONTAINER_NAME irm -f $irods_path

    echo -e "${GREEN}Done${NC} (Upload: ${upload_time}s, Download: ${download_time}s)"
done

rm -f "$test_file"

# Save results
echo "${upload_times[*]}" > "${RESULTS_DIR}/upload_times_${TIMESTAMP}.txt"
echo "${download_times[*]}" > "${RESULTS_DIR}/download_times_${TIMESTAMP}.txt"

echo -e "${GREEN}Test complete. Results saved in $RESULTS_DIR${NC}"
```

}

run_performance_test

