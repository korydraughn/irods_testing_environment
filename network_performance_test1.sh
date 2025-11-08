#!/bin/bash

# === iRODS Network Performance Test ===

# Measures upload/download times and effective network speed in MB/s

# Supports optional compression for multiple files.

# --- CONFIGURATION ---

TEST_RUNS=10
COMPRESSION=false               # true or false
TEST_FILES_DIR="/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles"
RESULTS_DIR="./performance_results"
TIMESTAMP=$(date -u '+%Y-%m-%d_%H-%M-%S')
IRODS_PATH="/tempZone/home/rods"

# Colors

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

mkdir -p "$RESULTS_DIR"

echo -e "${GREEN}=== iRODS Network Performance Test ===${NC}"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Test runs: $TEST_RUNS"
echo "Compression: $COMPRESSION"
echo "Test files dir: $TEST_FILES_DIR"
echo

# Check zstd if compression enabled

if [ "$COMPRESSION" = true ]; then
if ! command -v zstd >/dev/null 2>&1; then
echo -e "${RED}Error: zstd is required for compression but not found.${NC}"
exit 1
fi
fi

# --- FUNCTIONS ---

run_test() {
local file="$1"
local filename=$(basename "$file")
local upload_file="$file"

```
# Optional compression
if [ "$COMPRESSION" = true ]; then
    upload_file="$file.zst"
    if [ ! -f "$upload_file" ]; then
        echo -e "${YELLOW}Compressing $filename...${NC}"
        zstd -f "$file" -o "$upload_file"
    fi
fi

filesize_bytes=$(stat -c%s "$upload_file")
filesize_mb=$(echo "scale=3; $filesize_bytes / 1024 / 1024" | bc)

echo -e "${GREEN}Testing file: $filename (Size: ${filesize_mb} MB)${NC}"

local total_upload=0
local total_download=0

for i in $(seq 1 $TEST_RUNS); do
    echo "Run $i/$TEST_RUNS"

    # Upload
    start=$(date +%s.%N)
    if ! iput -f "$upload_file" "$IRODS_PATH/$filename"; then
        echo -e "${RED}Upload failed for $filename. Skipping this run.${NC}"
        continue
    fi
    end=$(date +%s.%N)
    upload_time=$(echo "$end - $start" | bc -l)
    upload_speed=$(echo "scale=2; $filesize_mb / $upload_time" | bc)
    total_upload=$(echo "$total_upload + $upload_time" | bc -l)
    echo "  Upload time: $upload_time s, Speed: $upload_speed MB/s"

    # Download
    download_target="/tmp/downloaded_$filename"
    start=$(date +%s.%N)
    if ! iget -f "$IRODS_PATH/$filename" "$download_target"; then
        echo -e "${RED}Download failed for $filename. Skipping this run.${NC}"
        continue
    fi
    end=$(date +%s.%N)
    download_time=$(echo "$end - $start" | bc -l)
    download_speed=$(echo "scale=2; $filesize_mb / $download_time" | bc)
    total_download=$(echo "$total_download + $download_time" | bc -l)
    echo "  Download time: $download_time s, Speed: $download_speed MB/s"

    # Clean up iRODS file and local download
    irm -f "$IRODS_PATH/$filename"
    rm -f "$download_target"
done

# Compute averages safely
if (( $(echo "$total_upload > 0" | bc -l) )); then
    avg_upload=$(echo "scale=3; $total_upload / $TEST_RUNS" | bc)
    avg_upload_speed=$(echo "scale=2; $filesize_mb / ($total_upload / $TEST_RUNS)" | bc)
else
    avg_upload=0
    avg_upload_speed=0
fi

if (( $(echo "$total_download > 0" | bc -l) )); then
    avg_download=$(echo "scale=3; $total_download / $TEST_RUNS" | bc)
    avg_download_speed=$(echo "scale=2; $filesize_mb / ($total_download / $TEST_RUNS)" | bc)
else
    avg_download=0
    avg_download_speed=0
fi

echo -e "${YELLOW}Average upload: $avg_upload s, $avg_upload_speed MB/s | Average download: $avg_download s, $avg_download_speed MB/s${NC}"

# Save results to CSV
echo "$filename,$filesize_mb,$avg_upload,$avg_upload_speed,$avg_download,$avg_download_speed" >> "$RESULTS_DIR/network_results_$TIMESTAMP.csv"
echo
```

}

# --- MAIN LOOP ---

for file in "$TEST_FILES_DIR"/*; do
if [ -f "$file" ]; then
run_test "$file"
fi
done

echo -e "${GREEN}All tests completed. Results saved in $RESULTS_DIR/network_results_$TIMESTAMP.csv${NC}"

