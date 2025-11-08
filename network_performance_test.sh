#!/bin/bash

# iRODS Network Performance Test
# Upload/download test against a remote iRODS server (EC2)
# Supports optional compression

# --- CONFIGURATION ---
TEST_RUNS=10
TEST_FILE_SIZE="100M"           # size of generated test file
RESULTS_DIR=${1:-"./performance_results"}
TIMESTAMP=${2:-$(date -u '+%Y-%m-%d_%H-%M-%S')}
ENABLE_COMPRESSION=false          # true/false
BUFFER_SIZE_MB=4                 # buffer size for info only

# iRODS server info
IRODS_HOST="98.93.165.152"
IRODS_PORT=1247
IRODS_USER="rods"
IRODS_PASS="usfusf"
IRODS_ZONE="tempZone"
IRODS_HOME="/${IRODS_ZONE}/home/${IRODS_USER}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# --- SETUP ---
mkdir -p "$RESULTS_DIR"

echo -e "${GREEN}=== iRODS Network Performance Test ===${NC}"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Test runs: $TEST_RUNS"
echo "Test file size: $TEST_FILE_SIZE"
echo "Compression: $ENABLE_COMPRESSION"
echo

# Generate a test file
TEST_FILE="testfile_${TEST_FILE_SIZE}.dat"
echo -e "${YELLOW}Creating test file ($TEST_FILE_SIZE)...${NC}"
truncate -s $TEST_FILE_SIZE "$TEST_FILE"

# Optional compression
if [ "$ENABLE_COMPRESSION" = true ]; then
    COMPRESSED_FILE="${TEST_FILE}.zst"
    echo -e "${YELLOW}Compressing test file...${NC}"
    zstd -19 "$TEST_FILE" -o "$COMPRESSED_FILE"
    UPLOAD_FILE="$COMPRESSED_FILE"
else
    UPLOAD_FILE="$TEST_FILE"
fi

# --- RUN TESTS ---
declare -a upload_times
declare -a download_times

for run in $(seq 1 $TEST_RUNS); do
    echo -e "${YELLOW}Run $run/$TEST_RUNS${NC}"

    REMOTE_PATH="${IRODS_HOME}/$(basename $UPLOAD_FILE)_$RANDOM"

    # --- Upload ---
    start=$(date +%s.%N)
    iput -f "$UPLOAD_FILE" "$REMOTE_PATH"
    end=$(date +%s.%N)
    upload_time=$(echo "$end - $start" | bc -l)
    upload_times+=($upload_time)
    echo -e "${GREEN}Upload completed in $upload_time s${NC}"

    # --- Download ---
    TMP_DOWNLOAD="/tmp/download_$(basename $UPLOAD_FILE)_$RANDOM"
    start=$(date +%s.%N)
    iget -f "$REMOTE_PATH" "$TMP_DOWNLOAD"
    end=$(date +%s.%N)
    download_time=$(echo "$end - $start" | bc -l)
    download_times+=($download_time)
    echo -e "${GREEN}Download completed in $download_time s${NC}"

    # --- Cleanup remote file ---
    irm -f "$REMOTE_PATH"

    # --- Cleanup local downloaded file ---
    rm -f "$TMP_DOWNLOAD"
done

# --- Save results ---
echo "${upload_times[*]}" > "${RESULTS_DIR}/upload_times_${TIMESTAMP}.txt"
echo "${download_times[*]}" > "${RESULTS_DIR}/download_times_${TIMESTAMP}.txt"

# --- Compute averages ---
avg_upload=$(awk '{sum+=$1} END {print sum/NR}' "${RESULTS_DIR}/upload_times_${TIMESTAMP}.txt")
avg_download=$(awk '{sum+=$1} END {print sum/NR}' "${RESULTS_DIR}/download_times_${TIMESTAMP}.txt")

echo
echo -e "${GREEN}=== TEST RESULTS ===${NC}"
echo "Average upload time  : $avg_upload s"
echo "Average download time: $avg_download s"
echo "Results saved in: $RESULTS_DIR"

# --- Cleanup ---
rm -f "$TEST_FILE"
[ "$ENABLE_COMPRESSION" = true ] && rm -f "$COMPRESSED_FILE"

