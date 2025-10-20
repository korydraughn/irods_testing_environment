#!/bin/bash

# Process Resource Usage Results Script
# Extracts and averages CPU and memory usage from time output files

# === INPUT ARGUMENTS ===
RESULTS_DIR="$1"
TIMESTAMP="$2"
TEST_RUNS="$3"
TEST_FILE_SIZE="$4"
CONTAINER_NAME="$5"

# === COLORS ===
GREEN='\033[0;32m'
NC='\033[0m'

# === VALIDATION ===
if [[ -z "$RESULTS_DIR" || -z "$TIMESTAMP" || -z "$TEST_RUNS" || -z "$TEST_FILE_SIZE" || -z "$CONTAINER_NAME" ]]; then
    echo "Usage: $0 <results_dir> <timestamp> <test_runs> <file_size> <container_name>"
    exit 1
fi

# Output file in writable directory
RESULTS_FILE="${RESULTS_DIR}/irods_resource_usage_${TIMESTAMP}.txt"

# === INITIALIZE ARRAYS ===
declare -a iput_cpu_usage
declare -a iput_mem_usage
declare -a iget_cpu_usage
declare -a iget_mem_usage

# === FUNCTION: Extract numeric field ===
extract_value() {
    local file="$1"
    local label="$2"
    grep -F "$label" "$file" | awk -F: '{gsub(/^[ \t]+/, "", $2); print $2}' | tr -d '%' | tr -d ' '
}

# === LOOP THROUGH FILES ===
for i in $(seq 1 "$TEST_RUNS"); do
    iput_file="${RESULTS_DIR}/iput_run_${i}.txt"
    iget_file="${RESULTS_DIR}/iget_run_${i}.txt"

    # Parse CPU (%) and Memory (KB)
    iput_cpu=$(extract_value "$iput_file" "Percent of CPU this job got")
    iput_mem=$(extract_value "$iput_file" "Maximum resident set size")
    iget_cpu=$(extract_value "$iget_file" "Percent of CPU this job got")
    iget_mem=$(extract_value "$iget_file" "Maximum resident set size")

    # Set defaults to 0 if empty to prevent bc errors
    iput_cpu=${iput_cpu:-0}
    iput_mem=${iput_mem:-0}
    iget_cpu=${iget_cpu:-0}
    iget_mem=${iget_mem:-0}

    iput_cpu_usage+=("$iput_cpu")
    iput_mem_usage+=("$iput_mem")
    iget_cpu_usage+=("$iget_cpu")
    iget_mem_usage+=("$iget_mem")

    # Optional: Print per-run summary (uncomment if desired)
    echo "Run $i:"
    echo "  iput CPU: ${iput_cpu}%, Memory: ${iput_mem} KB"
    echo "  iget CPU: ${iget_cpu}%, Memory: ${iget_mem} KB"
done

# === FUNCTION: Sum array ===
sum_array() {
    local arr=("$@")
    local sum=0
    for val in "${arr[@]}"; do
        if [[ "$val" =~ ^[0-9.]+$ ]]; then
            sum=$(echo "$sum + $val" | bc -l)
        fi
    done
    echo "$sum"
}

# === AVERAGE CALCULATIONS ===
iput_cpu_avg=$(echo "scale=2; $(sum_array "${iput_cpu_usage[@]}") / $TEST_RUNS" | bc)
iput_mem_avg=$(echo "scale=0; $(sum_array "${iput_mem_usage[@]}") / $TEST_RUNS" | bc)
iget_cpu_avg=$(echo "scale=2; $(sum_array "${iget_cpu_usage[@]}") / $TEST_RUNS" | bc)
iget_mem_avg=$(echo "scale=0; $(sum_array "${iget_mem_usage[@]}") / $TEST_RUNS" | bc)

# === DISPLAY RESULTS TO CONSOLE ===
echo
echo -e "${GREEN}=== iRODS Client-Side Resource Usage Results ===${NC}"
echo "Test runs:         $TEST_RUNS"
echo "File size:         $TEST_FILE_SIZE"
echo "Container:         $CONTAINER_NAME"
echo
echo "Average iput CPU usage:     ${iput_cpu_avg}%"
echo "Average iput memory usage:  ${iput_mem_avg} KB"
echo
echo "Average iget CPU usage:     ${iget_cpu_avg}%"
echo "Average iget memory usage:  ${iget_mem_avg} KB"
echo

# === WRITE TO OUTPUT FILE ===
cat > "$RESULTS_FILE" << EOF
iRODS Client-Side Resource Usage Results
========================================
Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
Timestamp: $TIMESTAMP
User: $(whoami)
Test runs: $TEST_RUNS
Test file size: $TEST_FILE_SIZE
Container: $CONTAINER_NAME

Average CPU and Memory Usage:
-----------------------------
Average iput CPU usage:     ${iput_cpu_avg}%
Average iput memory usage:  ${iput_mem_avg} KB

Average iget CPU usage:     ${iget_cpu_avg}%
Average iget memory usage:  ${iget_mem_avg} KB
EOF

# === FINAL SUCCESS MESSAGE ===
echo -e "${GREEN}Resource usage results saved to: ${RESULTS_FILE}${NC}"

