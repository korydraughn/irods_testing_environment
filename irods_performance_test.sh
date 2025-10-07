#!/bin/bash

# iRODS Performance Test Script
# Tests file upload/download times and calculates averages

# Configuration
TEST_RUNS=25
TEST_FILE_SIZE="100M"  # Size of test file to create
RESULTS_DIR="./performance_results"
TIMESTAMP=$(date -u '+%Y-%m-%d_%H-%M-%S')
RESULTS_FILE="${RESULTS_DIR}/irods_performance_${TIMESTAMP}.txt"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== iRODS Performance Test ===${NC}"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Test runs: $TEST_RUNS"
echo "Test file size: $TEST_FILE_SIZE"
echo

# Create results directory
mkdir -p "$RESULTS_DIR"

# Initialize results file
cat > "$RESULTS_FILE" << EOF
iRODS Performance Test Results
==============================
Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
User: $(whoami)
Test runs: $TEST_RUNS
Test file size: $TEST_FILE_SIZE
Container: ubuntu-2204-postgres-14_irods-catalog-provider_1

EOF

# Function to run performance test
run_performance_test() {
    local test_file="test_file_${TEST_FILE_SIZE}_${RANDOM}.dat"
    local irods_path="/tempZone/home/rods/${test_file}"
    
    echo -e "${YELLOW}Creating test file (${TEST_FILE_SIZE})...${NC}"
    dd if=/dev/zero of="$test_file" bs=1M count=${TEST_FILE_SIZE%M} 2>/dev/null
    
    declare -a upload_times
    declare -a download_times
    
    echo -e "${YELLOW}Running $TEST_RUNS test iterations...${NC}"
    
    for i in $(seq 1 $TEST_RUNS); do
        echo -n "  Run $i/$TEST_RUNS: "
        
        # Test upload (iput)
        echo -n "Upload... "
        upload_start=$(date +%s.%N)
        docker exec -u irods ubuntu-2204-postgres-14_irods-catalog-provider_1 \
            bash -c "cd /tmp && iput /host_data/$test_file $irods_path" 2>/dev/null
        upload_end=$(date +%s.%N)
        upload_time=$(echo "$upload_end - $upload_start" | bc -l)
        upload_times+=($upload_time)
        
        # Test download (iget)
        echo -n "Download... "
        download_start=$(date +%s.%N)
        docker exec -u irods ubuntu-2204-postgres-14_irods-catalog-provider_1 \
            bash -c "cd /tmp && iget -f $irods_path /tmp/downloaded_$test_file" 2>/dev/null
        download_end=$(date +%s.%N)
        download_time=$(echo "$download_end - $download_start" | bc -l)
        download_times+=($download_time)
        
        # Clean up iRODS file for next iteration
        docker exec -u irods ubuntu-2204-postgres-14_irods-catalog-provider_1 \
            bash -c "irm -f $irods_path" 2>/dev/null
        
        echo -e "${GREEN}Done${NC} (Upload: ${upload_time}s, Download: ${download_time}s)"
    done
    
    # Calculate averages
    upload_sum=0
    download_sum=0
    
    for time in "${upload_times[@]}"; do
        upload_sum=$(echo "$upload_sum + $time" | bc -l)
    done
    
    for time in "${download_times[@]}"; do
        download_sum=$(echo "$download_sum + $time" | bc -l)
    done
    
    upload_avg=$(echo "scale=3; $upload_sum / $TEST_RUNS" | bc -l)
    download_avg=$(echo "scale=3; $download_sum / $TEST_RUNS" | bc -l)
    
    # Calculate throughput (MB/s)
    file_size_mb=${TEST_FILE_SIZE%M}
    upload_throughput=$(echo "scale=2; $file_size_mb / $upload_avg" | bc -l)
    download_throughput=$(echo "scale=2; $file_size_mb / $download_avg" | bc -l)
    
    # Display results
    echo
    echo -e "${GREEN}=== Results ===${NC}"
    echo -e "Upload times:   ${upload_times[*]}"
    echo -e "Download times: ${download_times[*]}"
    echo -e "Average upload time:      ${upload_avg}s (${upload_throughput} MB/s)"
    echo -e "Average download time:    ${download_avg}s (${download_throughput} MB/s)"
    
    # Append to results file
    cat >> "$RESULTS_FILE" << EOF

Test Results:
-------------
Upload times (seconds):   ${upload_times[*]}
Download times (seconds): ${download_times[*]}
Average upload time:      ${upload_avg}s
Average download time:    ${download_avg}s
Upload throughput:        ${upload_throughput} MB/s
Download throughput:      ${download_throughput} MB/s

EOF
    
    # Clean up
    rm -f "$test_file"
    
    echo
    echo -e "${GREEN}Results saved to: $RESULTS_FILE${NC}"
}

# Check if iRODS container is running
if ! docker ps | grep -q ubuntu-2204-postgres-14_irods-catalog-provider_1; then
    echo -e "${RED}Error: iRODS container is not running!${NC}"
    echo "Please start it first with:"
    echo "python stand_it_up.py --project-directory ./projects/ubuntu-22.04/ubuntu-22.04-postgres-14"
    exit 1
fi

# Check if we can connect to iRODS
if ! docker exec -u irods ubuntu-2204-postgres-14_irods-catalog-provider_1 ils >/dev/null 2>&1; then
    echo -e "${RED}Error: Cannot connect to iRODS!${NC}"
    exit 1
fi

# Create a shared directory for file exchange (if it doesn't exist)
docker exec ubuntu-2204-postgres-14_irods-catalog-provider_1 mkdir -p /host_data 2>/dev/null

# Run the test
run_performance_test

echo -e "${GREEN}Performance test completed!${NC}"
