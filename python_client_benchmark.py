#!/usr/bin/env python3

"""
iRODS Adaptive Compression Benchmark
Tests network speed first, then adjusts compression level automatically

ADAPTIVE FEATURES:
- Pre-test network speed measurement
- Automatic compression level selection based on network speed
- Compression speed vs network speed tradeoff analysis
- Optimal settings for your specific connection
"""

import os
import sys
import time
import hashlib
import tempfile
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
from irods.session import iRODSSession
from irods.exception import iRODSException
from irods.models import DataObjectMeta

try:
    import zstandard as zstd
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False
    print("Warning: zstandard not installed. Install with: pip install zstandard")

from resource_monitor import ResourceMonitor, format_stats, save_stats_to_file


# ==================== CONFIGURATION ====================
TEST_RUNS = 5
TEST_FILES_DIR = "/home/demetrius/Desktop/testFiles"
ENABLE_ADAPTIVE_COMPRESSION = True  # Auto-adjust compression based on network speed
ENABLE_CLEANUP = True
ENABLE_FILE_VERIFICATION = True
ENABLE_METADATA = True
RESULTS_DIR = "./performance_results"

# Bandwidth limiting configuration (using wondershaper)
# Set to False for no limit (default speed), or specify a speed limit:
# Options: False, "100mbps", "50mbps", "10mbps", "1mbps"
# Note: Requires wondershaper (sudo apt install wondershaper)
# Format: {"download_kbps": 1000, "upload_kbps": 500, "interface": "eth0"}
# Or use string shortcuts: "1mbps", "10mbps", "100mbps"
BANDWIDTH_LIMIT = False  # No bandwidth limiting by default
NETWORK_INTERFACE = "eth0"  # Change to your network interface (use 'ip link show')

# Network speed test configuration
NETWORK_TEST_SIZE_MB = 5  # Size of test file for speed measurement
NETWORK_TEST_SAMPLES = 3  # Number of samples to average
# =======================================================

# Compression level recommendations based on network speed
# Logic: Faster network = lower compression (CPU less important)
#        Slower network = higher compression (worth the CPU cost)
COMPRESSION_STRATEGY = {
    'very_fast': {'min_mbps': 100, 'level': 1, 'description': 'Very fast network (>100 MB/s): minimal compression'},
    'fast': {'min_mbps': 50, 'level': 3, 'description': 'Fast network (50-100 MB/s): light compression'},
    'medium': {'min_mbps': 10, 'level': 6, 'description': 'Medium network (10-50 MB/s): balanced compression'},
    'slow': {'min_mbps': 1, 'level': 9, 'description': 'Slow network (1-10 MB/s): high compression'},
    'very_slow': {'min_mbps': 0, 'level': 15, 'description': 'Very slow network (<1 MB/s): maximum compression'},
}

# Metadata schema constants
METADATA_COMPRESSION_ALGORITHM = "compression_algorithm"
METADATA_ORIGINAL_SIZE = "original_size_bytes"
METADATA_COMPRESSION_RATIO = "compression_ratio_percent"
METADATA_COMPRESSION_LEVEL = "compression_level"

class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    MAGENTA = '\033[0;35m'
    NC = '\033[0m'

def print_color(message, color):
    """Print colored message"""
    print(f"{color}{message}{Colors.NC}")

def format_size(size_bytes):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def create_test_file(size_mb):
    """Create a temporary test file of specified size with random data"""
    test_file = tempfile.mktemp(suffix='.testdata')
    size_bytes = size_mb * 1024 * 1024
    
    with open(test_file, 'wb') as f:
        # Write random data in chunks
        chunk_size = 1024 * 1024  # 1 MB chunks
        written = 0
        while written < size_bytes:
            to_write = min(chunk_size, size_bytes - written)
            f.write(os.urandom(to_write))
            written += to_write
    
    return test_file

def test_network_speed(session, test_size_mb=NETWORK_TEST_SIZE_MB, samples=NETWORK_TEST_SAMPLES):
    """
    Test network speed by uploading and downloading a test file
    
    Returns: (upload_mbps, download_mbps, avg_latency_ms)
    """
    print_color(f"\n{'='*60}", Colors.CYAN)
    print_color("NETWORK SPEED TEST", Colors.CYAN)
    print_color(f"{'='*60}", Colors.CYAN)
    print(f"Test file size: {test_size_mb} MB")
    print(f"Test samples: {samples}")
    print()
    
    zone = session.zone
    username = session.username
    
    upload_speeds = []
    download_speeds = []
    latencies = []
    
    for i in range(samples):
        print(f"Sample {i+1}/{samples}: ", end='', flush=True)
        
        try:
            # Create test file
            test_file = create_test_file(test_size_mb)
            file_size = os.path.getsize(test_file)
            irods_path = f"/{zone}/home/{username}/.speedtest_{int(time.time()*1000)}.tmp"
            
            # Measure upload speed
            upload_start = time.time()
            session.data_objects.put(test_file, irods_path, force=True)
            upload_time = time.time() - upload_start
            upload_mbps = (file_size / (1024 * 1024)) / upload_time
            
            # Measure download speed
            download_file = tempfile.mktemp(suffix='.tmp')
            download_start = time.time()
            session.data_objects.get(irods_path, download_file, force=True)
            download_time = time.time() - download_start
            download_mbps = (file_size / (1024 * 1024)) / download_time
            
            # Calculate round-trip latency
            latency_ms = (upload_time + download_time) * 1000 / 2
            
            upload_speeds.append(upload_mbps)
            download_speeds.append(download_mbps)
            latencies.append(latency_ms)
            
            print(f"Up: {upload_mbps:.2f} MB/s, Down: {download_mbps:.2f} MB/s, Latency: {latency_ms:.0f}ms")
            
            # Cleanup
            os.unlink(test_file)
            os.unlink(download_file)
            session.data_objects.unlink(irods_path, force=True)
            
        except Exception as e:
            print_color(f"Failed: {e}", Colors.RED)
            continue
    
    if not upload_speeds:
        print_color("\nNetwork speed test failed!", Colors.RED)
        return None, None, None
    
    avg_upload = sum(upload_speeds) / len(upload_speeds)
    avg_download = sum(download_speeds) / len(download_speeds)
    avg_latency = sum(latencies) / len(latencies)
    
    print()
    print_color("Network Speed Test Results:", Colors.GREEN)
    print(f"  Average upload speed:   {avg_upload:.2f} MB/s")
    print(f"  Average download speed: {avg_download:.2f} MB/s")
    print(f"  Average latency:        {avg_latency:.0f} ms")
    
    return avg_upload, avg_download, avg_latency

def select_compression_level(network_speed_mbps):
    """
    Select optimal compression level based on network speed

    Logic:
    - Fast network: Lower compression (less CPU, network isn't bottleneck)
    - Slow network: Higher compression (more CPU, but saves transfer time)
    """
    selected_strategy = None
    selected_level = 3  # Default

    # Find matching strategy
    for name, config in sorted(COMPRESSION_STRATEGY.items(),
                               key=lambda x: x[1]['min_mbps'],
                               reverse=True):
        if network_speed_mbps >= config['min_mbps']:
            selected_strategy = name
            selected_level = config['level']
            description = config['description']
            break

    print()
    print_color(f"{'='*60}", Colors.CYAN)
    print_color("ADAPTIVE COMPRESSION SELECTION", Colors.CYAN)
    print_color(f"{'='*60}", Colors.CYAN)
    print(f"Network speed: {network_speed_mbps:.2f} MB/s")
    print(f"Strategy: {selected_strategy}")
    print(f"Selected compression level: {selected_level}")
    print(f"Rationale: {description}")
    print()

    # Show compression speed estimates
    print("Compression level characteristics (zstd):")
    print("  Level 1:  ~500 MB/s compression, ~2000 MB/s decompression")
    print("  Level 3:  ~200 MB/s compression, ~2000 MB/s decompression")
    print("  Level 6:  ~100 MB/s compression, ~1500 MB/s decompression")
    print("  Level 9:   ~40 MB/s compression, ~1200 MB/s decompression")
    print("  Level 15:  ~10 MB/s compression, ~1000 MB/s decompression")
    print()

    if selected_level == 1:
        print_color("→ Network is fast! Using minimal compression to save CPU.", Colors.GREEN)
    elif selected_level <= 3:
        print_color("→ Network is fairly fast. Using light compression.", Colors.GREEN)
    elif selected_level <= 6:
        print_color("→ Network speed is moderate. Balanced compression/speed tradeoff.", Colors.YELLOW)
    elif selected_level <= 9:
        print_color("→ Network is slow. Using higher compression to reduce transfer time.", Colors.YELLOW)
    else:
        print_color("→ Network is very slow! Using maximum compression.", Colors.RED)

    return selected_level

def select_compression_level_for_file(file_size_bytes, network_speed_mbps, base_level):
    """
    Select optimal compression level for a specific file based on size and network speed

    Logic:
    - Small files (< 1 MB): Use lower compression (overhead not worth it)
    - Medium files (1-100 MB): Use base level from network speed
    - Large files (> 100 MB): Potentially increase compression (more savings)

    Also considers compression speed vs network speed tradeoff:
    - If compression would be slower than network transfer, reduce level
    - If file is large and network is slow, increase level
    """
    file_size_mb = file_size_bytes / (1024 * 1024)

    # Compression speeds (MB/s) for different levels
    compression_speeds = {1: 500, 3: 200, 6: 100, 9: 40, 15: 10}

    # Start with base level
    selected_level = base_level

    # Adjust based on file size
    if file_size_mb < 1:
        # Small files: use minimal compression (overhead dominates)
        selected_level = min(selected_level, 1)
    elif file_size_mb < 10:
        # Medium-small files: slightly reduce compression
        selected_level = max(1, selected_level - 2)
    elif file_size_mb > 100:
        # Large files: can benefit from higher compression if network is slow
        if network_speed_mbps < 10:
            selected_level = min(15, selected_level + 2)
        elif network_speed_mbps < 50:
            selected_level = min(9, selected_level + 1)

    # Ensure compression won't bottleneck the transfer
    # If compression speed is much slower than network, reduce level
    estimated_comp_speed = compression_speeds.get(selected_level, 100)
    if estimated_comp_speed < network_speed_mbps * 0.5:
        # Compression is too slow, drop a few levels
        if selected_level > 6:
            selected_level = 6
        elif selected_level > 3:
            selected_level = 3

    # Clamp to valid range
    selected_level = max(1, min(22, selected_level))

    return selected_level

def calculate_compression_benefit(network_speed_mbps, file_size_mb, compression_ratio):
    """
    Calculate if compression is beneficial given network speed and compression ratio
    
    Returns: (time_saved_seconds, benefit_description)
    """
    # Estimate compression/decompression speeds (MB/s) - these are rough estimates
    compression_speeds = {1: 500, 3: 200, 6: 100, 9: 40, 15: 10}
    decompression_speed = 1500  # MB/s (decompression is usually fast)
    
    # Time without compression
    transfer_time_uncompressed = (file_size_mb * 2) / network_speed_mbps  # upload + download
    
    # Time with compression (for each level)
    benefits = {}
    for level, comp_speed in compression_speeds.items():
        compressed_size = file_size_mb * (1 - compression_ratio / 100)
        
        compress_time = file_size_mb / comp_speed
        decompress_time = compressed_size / decompression_speed
        transfer_time = (compressed_size * 2) / network_speed_mbps
        
        total_time_compressed = compress_time + transfer_time + decompress_time
        time_saved = transfer_time_uncompressed - total_time_compressed
        
        benefits[level] = {
            'time_saved': time_saved,
            'total_time': total_time_compressed,
            'worthwhile': time_saved > 0
        }
    
    return benefits

# Import all the verification functions from the fully verified version
def verify_file_exists(filepath, description="File"):
    """Verify that a file exists and is readable"""
    if not os.path.exists(filepath):
        return False
    if not os.path.isfile(filepath):
        return False
    if not os.access(filepath, os.R_OK):
        return False
    return True

def get_irods_file_size(session, irods_path):
    """Get the actual size of a file in iRODS"""
    try:
        obj = session.data_objects.get(irods_path)
        return obj.size
    except:
        return None

def verify_irods_upload(session, irods_path, expected_size):
    """Verify that a file was actually uploaded to iRODS with correct size"""
    try:
        obj = session.data_objects.get(irods_path)
        actual_size = obj.size
        
        if actual_size is None or actual_size == 0:
            return False, 0
        
        if actual_size != expected_size:
            return False, actual_size
        
        return True, actual_size
    except:
        return False, 0

def verify_local_download(filepath, expected_size):
    """Verify that a file was actually downloaded with correct size"""
    if not os.path.exists(filepath):
        return False, 0
    
    actual_size = os.path.getsize(filepath)
    
    if actual_size == 0 or actual_size != expected_size:
        return False, actual_size
    
    return True, actual_size

def get_test_files(test_files_dir):
    """Get list of test files from directory"""
    if not os.path.exists(test_files_dir):
        print_color(f"Error: Test files directory not found: {test_files_dir}", Colors.RED)
        sys.exit(1)
    
    files = []
    for item in os.listdir(test_files_dir):
        filepath = os.path.join(test_files_dir, item)
        if os.path.isfile(filepath) and verify_file_exists(filepath):
            files.append(filepath)
    
    if not files:
        print_color(f"Error: No valid files found in {test_files_dir}", Colors.RED)
        sys.exit(1)
    
    return files

def compress_file_zstd(input_file, compression_level=3):
    """Compress file using Zstandard with specified level, or return original if level is 0"""
    if compression_level == 0:
        # No compression - just return the original file
        return input_file

    if not ZSTD_AVAILABLE:
        print_color("Error: zstandard library not available", Colors.RED)
        sys.exit(1)

    compressed_file = tempfile.mktemp(suffix='.zst')
    cctx = zstd.ZstdCompressor(level=compression_level, threads=-1)

    with open(input_file, 'rb') as f_in:
        with open(compressed_file, 'wb') as f_out:
            cctx.copy_stream(f_in, f_out)

    return compressed_file

def decompress_file_zstd(compressed_file, output_file):
    """Decompress Zstandard file"""
    if not ZSTD_AVAILABLE:
        print_color("Error: zstandard library not available", Colors.RED)
        sys.exit(1)
    
    dctx = zstd.ZstdDecompressor()
    
    with open(compressed_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            dctx.copy_stream(f_in, f_out)

def add_compression_metadata(session, irods_path, compression_algorithm, original_size,
                            compression_ratio, compression_level=None):
    """Add compression metadata to iRODS object"""
    try:
        obj = session.data_objects.get(irods_path)
        obj.metadata.add(METADATA_COMPRESSION_ALGORITHM, compression_algorithm)
        obj.metadata.add(METADATA_ORIGINAL_SIZE, str(original_size))
        obj.metadata.add(METADATA_COMPRESSION_RATIO, f"{compression_ratio:.2f}")
        if compression_level is not None:
            obj.metadata.add(METADATA_COMPRESSION_LEVEL, str(compression_level))
        return True, None
    except Exception as e:
        return False, str(e)

def verify_metadata_written(session, irods_path, expected_algorithm, expected_size):
    """Verify that metadata was actually written"""
    try:
        obj = session.data_objects.get(irods_path)
        metadata = {}

        for item in obj.metadata.items():
            if item.name == METADATA_COMPRESSION_ALGORITHM:
                metadata['compression_algorithm'] = item.value
            elif item.name == METADATA_ORIGINAL_SIZE:
                metadata['original_size_bytes'] = int(item.value)

        required_fields = ['compression_algorithm', 'original_size_bytes']
        missing = [f for f in required_fields if f not in metadata]
        if missing:
            return False, metadata, f"Missing fields: {missing}"

        if (metadata['compression_algorithm'] != expected_algorithm or
            metadata['original_size_bytes'] != expected_size):
            return False, metadata, "Value mismatch"

        return True, metadata, None
    except Exception as e:
        return False, {}, str(e)

def read_compression_metadata(session, irods_path):
    """Read compression metadata from iRODS object"""
    try:
        obj = session.data_objects.get(irods_path)
        metadata = {}

        for item in obj.metadata.items():
            if item.name == METADATA_COMPRESSION_ALGORITHM:
                metadata['compression_algorithm'] = item.value
            elif item.name == METADATA_ORIGINAL_SIZE:
                metadata['original_size_bytes'] = int(item.value)
            elif item.name == METADATA_COMPRESSION_RATIO:
                metadata['compression_ratio_percent'] = float(item.value)
            elif item.name == METADATA_COMPRESSION_LEVEL:
                metadata['compression_level'] = int(item.value)

        if not metadata:
            return False, {}, "No metadata found"

        return True, metadata, None
    except Exception as e:
        return False, {}, str(e)

def decompress_based_on_metadata(compressed_file, output_file, metadata):
    """Decompress file based on metadata"""
    algorithm = metadata.get('compression_algorithm', 'unknown')
    
    if algorithm == 'zstd':
        decompress_file_zstd(compressed_file, output_file)
    elif algorithm == 'none':
        import shutil
        shutil.copy(compressed_file, output_file)
    else:
        raise ValueError(f"Unknown compression algorithm: {algorithm}")

def calculate_file_checksum(filepath, algorithm='sha256'):
    """Calculate checksum of a file"""
    hash_obj = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def cleanup_irods_directory(session, path_pattern="benchmark_"):
    """Clean up old benchmark files"""
    try:
        zone = session.zone
        username = session.username
        home_path = f"/{zone}/home/{username}"
        
        print_color(f"\nCleaning up iRODS directory: {home_path}", Colors.YELLOW)
        
        coll = session.collections.get(home_path)
        objects_removed = 0
        
        for obj in coll.data_objects:
            if path_pattern in obj.name or ".speedtest_" in obj.name:
                try:
                    obj_path = f"{home_path}/{obj.name}"
                    session.data_objects.unlink(obj_path, force=True)
                    objects_removed += 1
                except:
                    pass
        
        if objects_removed > 0:
            print_color(f"✓ Cleaned up {objects_removed} file(s)", Colors.GREEN)
        else:
            print_color(f"✓ Directory already clean", Colors.GREEN)
        
        return objects_removed
    except Exception as e:
        print_color(f"Warning: Cleanup failed: {e}", Colors.YELLOW)
        return 0

def create_session():
    """Create iRODS session"""
    try:
        env_file = os.environ.get('IRODS_ENVIRONMENT_FILE', 
                                   os.path.expanduser('~/.irods/irods_environment.json'))
        
        if not os.path.exists(env_file):
            print_color(f"Error: Environment file not found: {env_file}", Colors.RED)
            sys.exit(1)
        
        session = iRODSSession(irods_env_file=env_file)
        return session
    except Exception as e:
        print_color(f"Error: Cannot connect to iRODS", Colors.RED)
        sys.exit(1)

def verify_connection(session):
    """Verify iRODS connection"""
    print_color("\nVerifying iRODS connection...", Colors.YELLOW)
    
    try:
        zone = session.zone
        username = session.username
        home_path = f"/{zone}/home/{username}"
        
        print_color(f"  Zone: {zone}", Colors.BLUE)
        print_color(f"  User: {username}", Colors.BLUE)
        print_color(f"  Host: {session.host}:{session.port}", Colors.BLUE)
        
        coll = session.collections.get(home_path)
        print_color(f"  ✓ Connection verified", Colors.GREEN)
        return True
    except Exception as e:
        print_color(f"  ✗ Connection failed: {e}", Colors.RED)
        return False

def run_performance_test(session, test_files, test_runs, base_compression_level,
                        enable_verification, enable_metadata, network_speed_mbps=None):
    """Run performance test with per-file compression level selection"""
    results = []
    zone = session.zone
    username = session.username

    print_color(f"\nRunning {test_runs} test iterations...", Colors.YELLOW)
    print_color(f"  Base compression level: {base_compression_level}", Colors.CYAN)
    if ENABLE_ADAPTIVE_COMPRESSION and network_speed_mbps:
        print_color(f"  Adaptive compression: ENABLED (per-file)", Colors.CYAN)
    else:
        print_color(f"  Compression level: FIXED at {base_compression_level}", Colors.CYAN)
    print_color(f"  Verification: {'ENABLED' if enable_verification else 'DISABLED'}", Colors.CYAN)
    print_color(f"  Metadata: {'ENABLED' if enable_metadata else 'DISABLED'}", Colors.CYAN)

    failed_runs = 0

    for run_num in range(1, test_runs + 1):
        for test_file in test_files:
            filename = os.path.basename(test_file)

            if not verify_file_exists(test_file):
                failed_runs += 1
                continue

            original_size = os.path.getsize(test_file)

            if enable_verification:
                original_checksum = calculate_file_checksum(test_file)

            # Select compression level for this specific file
            if ENABLE_ADAPTIVE_COMPRESSION and network_speed_mbps:
                compression_level = select_compression_level_for_file(
                    original_size, network_speed_mbps, base_compression_level
                )
            else:
                compression_level = 0  # No compression

            # Compress with selected level (or skip if level is 0)
            if compression_level == 0:
                print(f"  Run {run_num}/{test_runs} [{filename}]: No compression... ", end='', flush=True)
                compress_start = time.time()
                upload_file = test_file  # Use original file
                compress_time = time.time() - compress_start
                transfer_size = original_size
                compression_ratio = 0
                print(f"✓ ({format_size(transfer_size)}) ", end='', flush=True)
            else:
                print(f"  Run {run_num}/{test_runs} [{filename}]: Compress(L{compression_level})... ", end='', flush=True)
                compress_start = time.time()
                upload_file = compress_file_zstd(test_file, compression_level)
                compress_time = time.time() - compress_start

                if not verify_file_exists(upload_file):
                    print_color(f"✗ FAILED", Colors.RED)
                    failed_runs += 1
                    continue

                transfer_size = os.path.getsize(upload_file)
                compression_ratio = (1 - transfer_size / original_size) * 100 if original_size > 0 else 0
                print(f"✓ ({format_size(transfer_size)}, {compression_ratio:.1f}%, {compress_time:.3f}s) ", end='', flush=True)

            # Use original filename in iRODS
            irods_path = f"/{zone}/home/{username}/{filename}"
            
            try:
                # Upload
                print("Up... ", end='', flush=True)
                upload_start = time.time()
                try:
                    session.data_objects.put(upload_file, irods_path, force=True) # might switch to parallel threading depending on client, flag for the put call set to 1
                    upload_time = time.time() - upload_start
                except iRODSException as irods_err:
                    print_color(f"✗ iRODS ERROR: {type(irods_err).__name__}", Colors.RED)
                    print_color(f"  Message: {str(irods_err)}", Colors.RED)
                    if hasattr(irods_err, 'error_code'):
                        print_color(f"  Error code: {irods_err.error_code}", Colors.RED)
                    raise
                
                upload_verified, irods_size = verify_irods_upload(session, irods_path, transfer_size)
                if not upload_verified:
                    print_color(f"✗ FAILED", Colors.RED)
                    failed_runs += 1
                    if compression_level > 0:  # Only delete temp file if compressed
                        os.unlink(upload_file)
                    continue

                print(f"✓ ", end='', flush=True)

                # Metadata
                if enable_metadata and compression_level > 0:
                    print("Meta... ", end='', flush=True)
                    add_compression_metadata(session, irods_path, "zstd", original_size,
                                           compression_ratio, compression_level)
                    print(f"✓ ", end='', flush=True)
                elif enable_metadata and compression_level == 0:
                    print("Meta... ", end='', flush=True)
                    add_compression_metadata(session, irods_path, "none", original_size, 0, 0)
                    print(f"✓ ", end='', flush=True)

                # Download
                print("Down... ", end='', flush=True)
                download_file = tempfile.mktemp(suffix='.tmp')
                download_start = time.time()
                session.data_objects.get(irods_path, download_file, force=True)
                download_time = time.time() - download_start

                download_verified, local_size = verify_local_download(download_file, transfer_size)
                if not download_verified:
                    print_color(f"✗ FAILED", Colors.RED)
                    failed_runs += 1
                    if compression_level > 0:  # Only delete temp file if compressed
                        os.unlink(upload_file)
                    session.data_objects.unlink(irods_path, force=True)
                    continue

                # Decompress (or skip if no compression)
                if compression_level == 0:
                    print(f"✓ No decomp... ", end='', flush=True)
                    decompressed_file = download_file  # Use downloaded file directly
                    decompress_time = 0
                else:
                    print(f"✓ Decomp... ", end='', flush=True)
                    decompress_start = time.time()
                    decompressed_file = tempfile.mktemp(suffix='.dat')

                    if enable_metadata:
                        meta_success, metadata, _ = read_compression_metadata(session, irods_path)
                        if meta_success:
                            decompress_based_on_metadata(download_file, decompressed_file, metadata)
                    else:
                        decompress_file_zstd(download_file, decompressed_file)

                    decompress_time = time.time() - decompress_start
                
                if not verify_file_exists(decompressed_file):
                    print_color(f"✗ FAILED", Colors.RED)
                    failed_runs += 1
                    if compression_level > 0:
                        os.unlink(upload_file)
                    if compression_level > 0 or decompressed_file != download_file:
                        os.unlink(download_file)
                    session.data_objects.unlink(irods_path, force=True)
                    continue

                final_size = os.path.getsize(decompressed_file)
                if enable_verification:
                    final_checksum = calculate_file_checksum(decompressed_file)

                # Only delete decompressed file if it's different from download file
                if compression_level > 0 and decompressed_file != download_file:
                    os.unlink(decompressed_file)

                # Verify
                verification_passed = True
                if final_size != original_size:
                    verification_passed = False

                if enable_verification and original_checksum != final_checksum:
                    verification_passed = False

                if not verification_passed:
                    print_color(f"✗ FAILED verification", Colors.RED)
                    failed_runs += 1
                    if compression_level > 0:
                        os.unlink(upload_file)
                    os.unlink(download_file)
                    session.data_objects.unlink(irods_path, force=True)
                    continue

                # Cleanup
                os.unlink(download_file)
                if compression_level > 0:  # Only delete if it's a temp compressed file
                    os.unlink(upload_file)
                session.data_objects.unlink(irods_path, force=True)
                
                # Calculate throughput
                upload_throughput = (transfer_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0
                download_throughput = (transfer_size / (1024 * 1024)) / download_time if download_time > 0 else 0
                
                print_color(f"✓ VERIFIED (Up:{upload_time:.2f}s Down:{download_time:.2f}s)", Colors.GREEN)
                
                # Store results
                results.append({
                    'run': run_num,
                    'filename': filename,
                    'original_size_bytes': original_size,
                    'transfer_size_bytes': transfer_size,
                    'compression_level': compression_level,
                    'compression_ratio': compression_ratio,
                    'compress_time': compress_time,
                    'upload_time': upload_time,
                    'download_time': download_time,
                    'decompress_time': decompress_time,
                    'upload_throughput_mbps': upload_throughput,
                    'download_throughput_mbps': download_throughput,
                })
                
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}" if e else "Unknown error"
                print_color(f"✗ FAILED: {error_msg}", Colors.RED)
                failed_runs += 1
                try:
                    if os.path.exists(upload_file):
                        os.unlink(upload_file)
                    if 'download_file' in locals() and os.path.exists(download_file):
                        os.unlink(download_file)
                    session.data_objects.unlink(irods_path, force=True)
                except:
                    pass
                continue
    
    if failed_runs > 0:
        print_color(f"\nWarning: {failed_runs} runs failed", Colors.YELLOW)
    
    return results

def calculate_aggregate_statistics(results):
    """Calculate aggregate statistics from results"""
    if not results:
        return None
    
    total_runs = len(results)
    
    files_data = {}
    for r in results:
        fname = r['filename']
        if fname not in files_data:
            files_data[fname] = []
        files_data[fname].append(r)
    
    total_upload_time = sum(r['upload_time'] for r in results)
    total_download_time = sum(r['download_time'] for r in results)
    total_compress_time = sum(r['compress_time'] for r in results)
    total_decompress_time = sum(r['decompress_time'] for r in results)
    
    avg_upload_time = total_upload_time / total_runs
    avg_download_time = total_download_time / total_runs
    avg_upload_throughput = sum(r['upload_throughput_mbps'] for r in results) / total_runs
    avg_download_throughput = sum(r['download_throughput_mbps'] for r in results) / total_runs
    
    return {
        'total_runs': total_runs,
        'files_tested': list(files_data.keys()),
        'total_upload_time': total_upload_time,
        'total_download_time': total_download_time,
        'total_compress_time': total_compress_time,
        'total_decompress_time': total_decompress_time,
        'avg_upload_time': avg_upload_time,
        'avg_download_time': avg_download_time,
        'avg_upload_throughput': avg_upload_throughput,
        'avg_download_throughput': avg_download_throughput,
        'files_data': files_data,
    }

def save_results(results_dir, timestamp, results, compression_level, session, 
                network_speed_info=None, resource_stats=None):
    """Save detailed results to file including resource usage and network info"""
    if not results:
        return None
    
    stats = calculate_aggregate_statistics(results)
    if not stats:
        return None
    
    Path(results_dir).mkdir(parents=True, exist_ok=True)
    
    results_file = os.path.join(results_dir, 
        f"adaptive_benchmark_L{compression_level}_{timestamp}.txt")
    
    with open(results_file, 'w') as f:
        f.write("iRODS Adaptive Compression Benchmark Results\n")
        f.write("=" * 80 + "\n")
        f.write(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"User: {os.getenv('USER', 'unknown')}\n")
        f.write(f"iRODS User: {session.username}\n")
        f.write(f"iRODS Zone: {session.zone}\n")
        f.write(f"iRODS Host: {session.host}:{session.port}\n")
        f.write(f"Method: Python iRODS Client (PRC) with Adaptive Compression\n")
        f.write(f"Compression Algorithm: Zstandard\n")
        f.write(f"Compression Level: {compression_level}\n")
        f.write(f"Total test runs: {stats['total_runs']}\n")
        f.write(f"Files tested: {', '.join(stats['files_tested'])}\n")
        f.write("\n")
        
        # Network speed info
        if network_speed_info:
            f.write("Network Speed Test Results:\n")
            f.write("-" * 80 + "\n")
            f.write(f"Test file size: {NETWORK_TEST_SIZE_MB} MB\n")
            f.write(f"Test samples: {NETWORK_TEST_SAMPLES}\n")
            f.write(f"Upload speed:   {network_speed_info['upload_mbps']:.2f} MB/s\n")
            f.write(f"Download speed: {network_speed_info['download_mbps']:.2f} MB/s\n")
            f.write(f"Average speed:  {network_speed_info['avg_mbps']:.2f} MB/s\n")
            f.write(f"Average latency: {network_speed_info['latency_ms']:.0f} ms\n")
            f.write(f"Strategy selected: {network_speed_info['strategy']}\n")
            f.write(f"Rationale: {network_speed_info['rationale']}\n")
            f.write("\n")
        
        f.write("Aggregate Statistics:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total upload time:        {stats['total_upload_time']:.3f}s\n")
        f.write(f"Total download time:      {stats['total_download_time']:.3f}s\n")
        f.write(f"Total compression time:   {stats['total_compress_time']:.3f}s\n")
        f.write(f"Total decompression time: {stats['total_decompress_time']:.3f}s\n")
        f.write(f"Average upload time:      {stats['avg_upload_time']:.3f}s\n")
        f.write(f"Average download time:    {stats['avg_download_time']:.3f}s\n")
        f.write(f"Average upload throughput:   {stats['avg_upload_throughput']:.2f} MB/s\n")
        f.write(f"Average download throughput: {stats['avg_download_throughput']:.2f} MB/s\n")
        f.write("\n")
        
        # Per-file statistics
        f.write("Per-File Statistics:\n")
        f.write("-" * 80 + "\n")
        for filename, file_results in stats['files_data'].items():
            f.write(f"\nFile: {filename}\n")
            f.write(f"  Original size: {format_size(file_results[0]['original_size_bytes'])}\n")
            avg_transfer_size = sum(r['transfer_size_bytes'] for r in file_results) / len(file_results)
            avg_compression_ratio = sum(r['compression_ratio'] for r in file_results) / len(file_results)
            f.write(f"  Avg compressed size: {format_size(avg_transfer_size)}\n")
            f.write(f"  Avg compression ratio: {avg_compression_ratio:.1f}%\n")
            f.write(f"  Runs: {len(file_results)}\n")
            avg_up = sum(r['upload_time'] for r in file_results) / len(file_results)
            avg_down = sum(r['download_time'] for r in file_results) / len(file_results)
            f.write(f"  Avg upload time: {avg_up:.3f}s\n")
            f.write(f"  Avg download time: {avg_down:.3f}s\n")
        
        f.write("\n")
        f.write("Detailed Results:\n")
        f.write("-" * 80 + "\n")
        f.write("Run | File | Size | Compressed | Upload | Download | Ratio\n")
        f.write("-" * 80 + "\n")
        for r in results:
            f.write(f"{r['run']:3d} | {r['filename'][:20]:20s} | ")
            f.write(f"{format_size(r['original_size_bytes']):>10s} | ")
            f.write(f"{format_size(r['transfer_size_bytes']):>10s} | ")
            f.write(f"{r['upload_time']:6.3f}s | {r['download_time']:6.3f}s | ")
            f.write(f"{r['compression_ratio']:5.1f}%\n")
        
        # Add resource usage statistics if provided
        if resource_stats:
            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("CLIENT-SIDE RESOURCE USAGE\n")
            f.write("=" * 80 + "\n")
            f.write(f"Test Duration:         {resource_stats['duration_seconds']:.2f} seconds\n")
            f.write(f"Samples Collected:     {resource_stats['samples_collected']}\n")
            f.write("\n")
            f.write("CPU Usage:\n")
            f.write(f"  Average CPU:         {resource_stats['avg_cpu_percent']:.2f}%\n")
            f.write(f"  Peak CPU:            {resource_stats['max_cpu_percent']:.2f}%\n")
            f.write("\n")
            f.write("Memory Usage:\n")
            f.write(f"  Start memory (RSS):  {resource_stats['start_memory_mb']:.2f} MB\n")
            f.write(f"  Average memory:      {resource_stats['avg_memory_mb']:.2f} MB\n")
            f.write(f"  Peak memory:         {resource_stats['peak_memory_mb']:.2f} MB\n")
            f.write(f"  Final memory:        {resource_stats['final_memory_mb']:.2f} MB\n")
            f.write(f"  Memory delta:        {resource_stats['memory_delta_mb']:+.2f} MB\n")
    
    return results_file, stats

def main():
    """Main execution with adaptive compression"""
    print_color("=" * 80, Colors.GREEN)
    print_color("iRODS ADAPTIVE COMPRESSION BENCHMARK", Colors.GREEN)
    print_color("Network-aware compression level selection", Colors.GREEN)
    print_color("=" * 80, Colors.GREEN)
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    print(f"\nDate: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Test runs: {TEST_RUNS}")
    print(f"Adaptive compression: {'ENABLED' if ENABLE_ADAPTIVE_COMPRESSION else 'DISABLED (no compression)'}")

    if ENABLE_ADAPTIVE_COMPRESSION and not ZSTD_AVAILABLE:
        print_color("\nError: zstandard not installed! Install with: pip install zstandard", Colors.RED)
        sys.exit(1)
    
    test_files = get_test_files(TEST_FILES_DIR)
    print_color(f"\nFound {len(test_files)} test file(s):", Colors.CYAN)
    for f in test_files:
        size = os.path.getsize(f)
        print(f"  - {os.path.basename(f):30s} ({format_size(size)})")
    
    monitor = ResourceMonitor(interval=0.1)
    monitor.start()
    
    print_color("\nConnecting to iRODS...", Colors.YELLOW)
    session = create_session()
    print_color(f"✓ Connected as {session.username}@{session.zone}", Colors.GREEN)
    
    if not verify_connection(session):
        session.cleanup()
        sys.exit(1)
    
    try:
        if ENABLE_CLEANUP:
            cleanup_irods_directory(session)
        
        # Test network speed
        if ENABLE_ADAPTIVE_COMPRESSION:
            upload_speed, download_speed, latency = test_network_speed(
                session, NETWORK_TEST_SIZE_MB, NETWORK_TEST_SAMPLES
            )

            if upload_speed is None:
                print_color("\nNetwork test failed, disabling compression", Colors.YELLOW)
                compression_level = 0
            else:
                # Use average of upload/download for compression decision
                avg_speed = (upload_speed + download_speed) / 2
                compression_level = select_compression_level(avg_speed)
        else:
            compression_level = 0
            print_color(f"\nAdaptive compression disabled - No compression will be used", Colors.CYAN)
        
        # Run benchmark
        # Pass network speed for per-file adaptive compression
        avg_speed = None
        if ENABLE_ADAPTIVE_COMPRESSION and 'upload_speed' in locals() and upload_speed is not None:
            avg_speed = (upload_speed + download_speed) / 2

        results = run_performance_test(
            session, test_files, TEST_RUNS, compression_level,
            ENABLE_FILE_VERIFICATION, ENABLE_METADATA, avg_speed
        )
        
        if not results:
            print_color("\nNo successful test runs", Colors.RED)
            return
        
        if ENABLE_CLEANUP:
            cleanup_irods_directory(session)
        
        resource_stats = monitor.stop()
        
        # Prepare network speed info for results file
        network_speed_info = None
        if ENABLE_ADAPTIVE_COMPRESSION and 'upload_speed' in locals() and upload_speed is not None:
            # Determine strategy used
            avg_speed = (upload_speed + download_speed) / 2
            strategy_name = None
            rationale = None
            for name, config in sorted(COMPRESSION_STRATEGY.items(), 
                                      key=lambda x: x[1]['min_mbps'], 
                                      reverse=True):
                if avg_speed >= config['min_mbps']:
                    strategy_name = name
                    rationale = config['description']
                    break
            
            network_speed_info = {
                'upload_mbps': upload_speed,
                'download_mbps': download_speed,
                'avg_mbps': avg_speed,
                'latency_ms': latency,
                'strategy': strategy_name,
                'rationale': rationale
            }
        
        # Save results to file
        print_color("\nSaving results...", Colors.GREEN)
        results_file, stats = save_results(
            RESULTS_DIR, timestamp, results, compression_level, 
            session, network_speed_info, resource_stats
        )
        
        # Display results summary
        print()
        print_color("=" * 80, Colors.GREEN)
        print_color("BENCHMARK RESULTS SUMMARY", Colors.GREEN)
        print_color("=" * 80, Colors.GREEN)
        
        total_runs = len(results)
        avg_compress_time = sum(r['compress_time'] for r in results) / total_runs
        avg_upload_time = sum(r['upload_time'] for r in results) / total_runs
        avg_download_time = sum(r['download_time'] for r in results) / total_runs
        avg_decompress_time = sum(r['decompress_time'] for r in results) / total_runs
        avg_compression_ratio = sum(r['compression_ratio'] for r in results) / total_runs
        
        print(f"Compression level used: {compression_level}")
        print(f"Average compression ratio: {avg_compression_ratio:.1f}%")
        print(f"Average compression time: {avg_compress_time:.3f}s")
        print(f"Average upload time: {avg_upload_time:.3f}s")
        print(f"Average download time: {avg_download_time:.3f}s")
        print(f"Average decompression time: {avg_decompress_time:.3f}s")
        print(f"Total round-trip time: {avg_compress_time + avg_upload_time + avg_download_time + avg_decompress_time:.3f}s")
        print()
        print_color(f"Results saved to: {results_file}", Colors.GREEN)
        
    except KeyboardInterrupt:
        print_color("\n\nInterrupted", Colors.YELLOW)
        monitor.stop()
    except Exception as e:
        print_color(f"\nError: {e}", Colors.RED)
        import traceback
        traceback.print_exc()
        monitor.stop()
    finally:
        session.cleanup()
    
    print_color("\n" + "=" * 80, Colors.GREEN)
    print_color("Benchmark completed!", Colors.GREEN)
    print_color("=" * 80, Colors.GREEN)

def parse_bandwidth_limit(limit_str):
    """Parse bandwidth limit string to kbps for wondershaper

    Args:
        limit_str: Either False, or string like "1mbps", "10mbps", "100mbps"

    Returns:
        kbps value or None if no limit
    """
    if not limit_str or limit_str == False:
        return None

    limit_str = str(limit_str).lower()
    if limit_str.endswith('mbps'):
        mbps = int(limit_str.replace('mbps', ''))
        # Convert Mbps to kbps (1 Mbps = 1000 kbps)
        return mbps * 1000

    return None

def apply_bandwidth_limit():
    """Apply bandwidth limiting using wondershaper

    Returns:
        True if limit was applied, False otherwise
    """
    kbps = parse_bandwidth_limit(BANDWIDTH_LIMIT)

    if kbps is None:
        # No bandwidth limit requested
        return False

    # Check if wondershaper is available
    if not shutil.which('wondershaper'):
        print_color("Warning: wondershaper not found. Install with: sudo apt install wondershaper", Colors.YELLOW)
        print_color(f"Continuing without bandwidth limit of {BANDWIDTH_LIMIT}", Colors.YELLOW)
        return False

    try:
        print_color(f"\nApplying bandwidth limit: {BANDWIDTH_LIMIT} ({kbps} kbps)", Colors.CYAN)
        print_color(f"Interface: {NETWORK_INTERFACE}", Colors.CYAN)

        # Apply bandwidth limit
        # wondershaper <interface> <download_kbps> <upload_kbps>
        cmd = ['sudo', 'wondershaper', NETWORK_INTERFACE, str(kbps), str(kbps)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            print_color(f"Warning: Failed to apply bandwidth limit: {result.stderr}", Colors.YELLOW)
            print_color("Continuing without bandwidth limit...", Colors.YELLOW)
            return False

        print_color(f"✓ Bandwidth limit applied successfully", Colors.GREEN)
        print_color(f"  Download: {kbps} kbps (~{kbps/8:.1f} KB/s)", Colors.GREEN)
        print_color(f"  Upload:   {kbps} kbps (~{kbps/8:.1f} KB/s)", Colors.GREEN)
        return True

    except subprocess.TimeoutExpired:
        print_color("Warning: wondershaper command timed out", Colors.YELLOW)
        return False
    except Exception as e:
        print_color(f"Warning: Failed to apply bandwidth limit: {e}", Colors.YELLOW)
        return False

def remove_bandwidth_limit():
    """Remove bandwidth limiting using wondershaper"""
    # Only try to remove if bandwidth limiting was requested
    if not BANDWIDTH_LIMIT or BANDWIDTH_LIMIT == False:
        return

    if not shutil.which('wondershaper'):
        return

    try:
        print_color(f"\nRemoving bandwidth limit from {NETWORK_INTERFACE}...", Colors.CYAN)
        cmd = ['sudo', 'wondershaper', 'clear', NETWORK_INTERFACE]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            print_color(f"✓ Bandwidth limit removed", Colors.GREEN)
        else:
            print_color(f"Warning: Failed to remove bandwidth limit: {result.stderr}", Colors.YELLOW)

    except Exception as e:
        print_color(f"Warning: Failed to remove bandwidth limit: {e}", Colors.YELLOW)

if __name__ == "__main__":
    # Apply bandwidth limiting if requested
    bandwidth_applied = apply_bandwidth_limit()

    try:
        main()
    finally:
        # Always try to remove bandwidth limit when done
        if bandwidth_applied:
            remove_bandwidth_limit()
