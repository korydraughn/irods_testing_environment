#!/bin/bash

# Process Results Script
# Calculates averages and saves formatted results

# Arguments
RESULTS_DIR="$1"
TIMESTAMP="$2"
TEST_RUNS="$3"
TEST_FILE_SIZE="$4"
CONTAINER_NAME="$5"

# Colors for output
GREEN='\033[0;32m'
NC='\033[0m' # No Color

RESULTS_FILE="${RESULTS_DIR}/irods_performance_${TIMESTAMP}.txt"

# Read timing data
read -ra upload_times < "${RESULTS_DIR}/upload_times_prot0_${TIMESTAMP}.txt"
read -ra download_times < "${RESULTS_DIR}/download_times_prot0_${TIMESTAMP}.txt"

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

# Write results file
cat > "$RESULTS_FILE" << EOF
iRODS Performance Test Results
==============================
Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
Timestamp: $TIMESTAMP
User: $(whoami)
Test runs: $TEST_RUNS
Test file size: $TEST_FILE_SIZE
Container: $CONTAINER_NAME

Test Results:
-------------
Upload times (seconds):   ${upload_times[*]}
Download times (seconds): ${download_times[*]}
Average upload time:      ${upload_avg}s
Average download time:    ${download_avg}s
Upload throughput:        ${upload_throughput} MB/s
Download throughput:      ${download_throughput} MB/s

EOF

# Clean up temporary timing files
rm -f "${RESULTS_DIR}/upload_times_${TIMESTAMP}.txt" "${RESULTS_DIR}/download_times_${TIMESTAMP}.txt"

echo
echo -e "${GREEN}Results saved to: $RESULTS_FILE${NC}"
