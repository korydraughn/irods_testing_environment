#!/usr/bin/env python3

"""
iRODS Performance Test Script (Python iRODS Client)
Tests file upload/download times with real files and optional compression
"""

import os
import sys
import time
import gzip
import tempfile
from datetime import datetime
from pathlib import Path
from irods.session import iRODSSession
from irods.exception import iRODSException
from resource_monitor import ResourceMonitor, format_stats, save_stats_to_file


# ==================== CONFIGURATION ====================
TEST_RUNS = 25
TEST_FILES_DIR = "/home/demetrius/Desktop/testFiles"  # Directory containing test files
ENABLE_COMPRESSION = False  # Set to True to enable compression before upload
RESULTS_DIR = "./performance_results"
# =======================================================

# ANSI color codes
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    CYAN = '\033[0;36m'
    NC = '\033[0m'  # No Color

def print_color(message, color):
    """Print colored message"""
    print(f"{color}{message}{Colors.NC}")

def print_error_details(e):
    """Print detailed iRODS exception information"""
    print_color(f"Error: {str(e)}", Colors.RED)
    if hasattr(e, 'code'):
        print_color(f"  Error code: {e.code}", Colors.RED)
    if hasattr(e, 'msg'):
        print_color(f"  Message: {e.msg}", Colors.RED)
    if hasattr(e, 'args') and e.args:
        print_color(f"  Args: {e.args}", Colors.RED)

def format_size(size_bytes):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def get_test_files(test_files_dir):
    """Get list of test files from directory"""
    if not os.path.exists(test_files_dir):
        print_color(f"Error: Test files directory not found: {test_files_dir}", Colors.RED)
        sys.exit(1)
    
    # Get all files (not directories) from the test files directory
    files = []
    for item in os.listdir(test_files_dir):
        filepath = os.path.join(test_files_dir, item)
        if os.path.isfile(filepath):
            files.append(filepath)
    
    if not files:
        print_color(f"Error: No files found in {test_files_dir}", Colors.RED)
        sys.exit(1)
    
    return files

def compress_file(input_file):
    """Compress file using gzip and return path to compressed file"""
    compressed_file = tempfile.mktemp(suffix='.gz')
    
    with open(input_file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb', compresslevel=6) as f_out:
            # Compress in chunks to handle large files
            chunk_size = 1024 * 1024  # 1MB chunks
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
    
    return compressed_file

def decompress_file(compressed_file, output_file):
    """Decompress gzip file"""
    with gzip.open(compressed_file, 'rb') as f_in:
        with open(output_file, 'wb') as f_out:
            # Decompress in chunks
            chunk_size = 1024 * 1024  # 1MB chunks
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)

def create_session():
    """Create iRODS session from environment file"""
    try:
        env_file = os.environ.get('IRODS_ENVIRONMENT_FILE', 
                                   os.path.expanduser('~/.irods/irods_environment.json'))
        
        if not os.path.exists(env_file):
            print_color(f"Error: Environment file not found: {env_file}", Colors.RED)
            print_color("Please run 'iinit' to configure iRODS", Colors.YELLOW)
            sys.exit(1)
        
        session = iRODSSession(irods_env_file=env_file)
        return session
    except Exception as e:
        print_color(f"Error: Cannot connect to iRODS", Colors.RED)
        print_error_details(e)
        sys.exit(1)

def verify_connection(session):
    """Verify iRODS connection and permissions"""
    print_color("\nVerifying iRODS connection...", Colors.YELLOW)
    
    try:
        zone = session.zone
        username = session.username
        home_path = f"/{zone}/home/{username}"
        
        print_color(f"  Zone: {zone}", Colors.BLUE)
        print_color(f"  User: {username}", Colors.BLUE)
        print_color(f"  Host: {session.host}:{session.port}", Colors.BLUE)
        print_color(f"  Home: {home_path}", Colors.BLUE)
        
        # Test if home directory exists and is accessible
        try:
            coll = session.collections.get(home_path)
            print_color(f"  ✓ Home directory accessible", Colors.GREEN)
        except iRODSException as e:
            print_color(f"  ✗ Cannot access home directory", Colors.RED)
            print_error_details(e)
            return False
        
        # Test write/read permissions
        test_path = f"{home_path}/.performance_test_{int(time.time())}.tmp"
        test_data = b"test"
        
        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(test_data)
                tmp_file = tmp.name
            
            session.data_objects.put(tmp_file, test_path)
            print_color(f"  ✓ Write test successful", Colors.GREEN)
            
            download_file = tempfile.mktemp(suffix='.tmp')
            session.data_objects.get(test_path, download_file)
            print_color(f"  ✓ Read test successful", Colors.GREEN)
            
            os.unlink(tmp_file)
            os.unlink(download_file)
            session.data_objects.unlink(test_path, force=True)
            
            return True
            
        except iRODSException as e:
            print_color(f"  ✗ Permission test failed", Colors.RED)
            print_error_details(e)
            return False
            
    except Exception as e:
        print_color(f"  ✗ Connection verification failed", Colors.RED)
        print_error_details(e)
        return False

def run_performance_test(session, test_files, test_runs, enable_compression):
    """Run the performance test with real files"""
    results = []
    
    zone = session.zone
    username = session.username
    
    print_color(f"\nRunning {test_runs} test iterations with {len(test_files)} file(s)...", Colors.YELLOW)
    if enable_compression:
        print_color("  Compression: ENABLED", Colors.CYAN)
    else:
        print_color("  Compression: DISABLED", Colors.CYAN)
    
    failed_runs = 0
    
    for run_num in range(1, test_runs + 1):
        for test_file in test_files:
            filename = os.path.basename(test_file)
            original_size = os.path.getsize(test_file)
            
            # Determine what file to upload
            if enable_compression:
                print(f"  Run {run_num}/{test_runs} [{filename}]: Compressing... ", end='', flush=True)
                compress_start = time.time()
                upload_file = compress_file(test_file)
                compress_time = time.time() - compress_start
                transfer_size = os.path.getsize(upload_file)
                compression_ratio = (1 - transfer_size / original_size) * 100 if original_size > 0 else 0
                print(f"({format_size(transfer_size)}, {compression_ratio:.1f}% saved, {compress_time:.3f}s) ", end='', flush=True)
                irods_filename = f"{filename}.gz"
            else:
                upload_file = test_file
                transfer_size = original_size
                compression_ratio = 0
                compress_time = 0
                print(f"  Run {run_num}/{test_runs} [{filename}] ({format_size(original_size)}): ", end='', flush=True)
                irods_filename = filename
            
            irods_path = f"/{zone}/home/{username}/benchmark_{irods_filename}_{int(time.time())}"
            
            try:
                # Test upload (put)
                print("Upload... ", end='', flush=True)
                upload_start = time.time()
                session.data_objects.put(upload_file, irods_path, force=True)
                upload_time = time.time() - upload_start
                
                # Test download (get)
                print("Download... ", end='', flush=True)
                download_file = tempfile.mktemp(suffix='.tmp')
                
                download_start = time.time()
                session.data_objects.get(irods_path, download_file, force=True)
                download_time = time.time() - download_start
                
                # If compressed, decompress for verification
                if enable_compression:
                    print("Decompress... ", end='', flush=True)
                    decompress_start = time.time()
                    decompressed_file = tempfile.mktemp(suffix='.dat')
                    decompress_file(download_file, decompressed_file)
                    decompress_time = time.time() - decompress_start
                    
                    # Verify size matches original
                    final_size = os.path.getsize(decompressed_file)
                    os.unlink(decompressed_file)
                else:
                    decompress_time = 0
                    final_size = os.path.getsize(download_file)
                
                # Verify size
                if final_size != original_size:
                    print_color(f"Warning: Size mismatch! {final_size} != {original_size}", Colors.YELLOW)
                
                # Cleanup
                os.unlink(download_file)
                if enable_compression and upload_file != test_file:
                    os.unlink(upload_file)
                session.data_objects.unlink(irods_path, force=True)
                
                # Calculate throughput
                upload_throughput = (transfer_size / (1024 * 1024)) / upload_time if upload_time > 0 else 0
                download_throughput = (transfer_size / (1024 * 1024)) / download_time if download_time > 0 else 0
                
                print_color(f"Done (U: {upload_time:.3f}s, D: {download_time:.3f}s)", Colors.GREEN)
                
                # Store results
                results.append({
                    'run': run_num,
                    'filename': filename,
                    'original_size_bytes': original_size,
                    'transfer_size_bytes': transfer_size,
                    'compressed': enable_compression,
                    'compression_ratio': compression_ratio,
                    'compress_time': compress_time,
                    'upload_time': upload_time,
                    'download_time': download_time,
                    'decompress_time': decompress_time,
                    'upload_throughput_mbps': upload_throughput,
                    'download_throughput_mbps': download_throughput,
                })
                
            except iRODSException as e:
                print_color(f"FAILED", Colors.RED)
                print_error_details(e)
                failed_runs += 1
                
                # Cleanup
                try:
                    if enable_compression and upload_file != test_file and os.path.exists(upload_file):
                        os.unlink(upload_file)
                    session.data_objects.unlink(irods_path, force=True)
                except:
                    pass
                
                if failed_runs > (test_runs * len(test_files)) // 2:
                    print_color(f"\nToo many failures ({failed_runs}), stopping test.", Colors.RED)
                    return results
                    
                continue
                
            except Exception as e:
                print_color(f"FAILED - {type(e).__name__}: {str(e)}", Colors.RED)
                failed_runs += 1
                continue
    
    if failed_runs > 0:
        print_color(f"\nWarning: {failed_runs} runs failed", Colors.YELLOW)
    
    return results

def calculate_aggregate_statistics(results):
    """Calculate aggregate statistics from results"""
    if not results:
        return None
    
    total_runs = len(results)
    
    # Group by filename for per-file statistics
    files_data = {}
    for r in results:
        fname = r['filename']
        if fname not in files_data:
            files_data[fname] = []
        files_data[fname].append(r)
    
    # Calculate aggregates
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

def save_results(results_dir, timestamp, results, enable_compression, session, resource_stats=None):
    """Save detailed results to file including resource usage"""
    if not results:
        return None
    
    stats = calculate_aggregate_statistics(results)
    if not stats:
        return None
    
    Path(results_dir).mkdir(parents=True, exist_ok=True)
    
    # Results filename
    compression_suffix = "_compressed" if enable_compression else "_uncompressed"
    results_file = os.path.join(results_dir, f"python_client_benchmark{compression_suffix}_{timestamp}.txt")
    
    with open(results_file, 'w') as f:
        f.write("iRODS Performance Test Results (Python iRODS Client)\n")
        f.write("=" * 80 + "\n")
        f.write(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"User: {os.getenv('USER', 'unknown')}\n")
        f.write(f"iRODS User: {session.username}\n")
        f.write(f"iRODS Zone: {session.zone}\n")
        f.write(f"iRODS Host: {session.host}:{session.port}\n")
        f.write(f"Method: Python iRODS Client (PRC)\n")
        f.write(f"Compression: {'ENABLED (gzip)' if enable_compression else 'DISABLED'}\n")
        f.write(f"Total test runs: {stats['total_runs']}\n")
        f.write(f"Files tested: {', '.join(stats['files_tested'])}\n")
        f.write("\n")
        
        f.write("Aggregate Statistics:\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total upload time:        {stats['total_upload_time']:.3f}s\n")
        f.write(f"Total download time:      {stats['total_download_time']:.3f}s\n")
        if enable_compression:
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
            if enable_compression:
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
        f.write("Run | File | Size | Transfer | Upload | Download | U-Tput | D-Tput\n")
        f.write("-" * 80 + "\n")
        for r in results:
            f.write(f"{r['run']:3d} | {r['filename'][:20]:20s} | ")
            f.write(f"{format_size(r['original_size_bytes']):>10s} | ")
            f.write(f"{format_size(r['transfer_size_bytes']):>10s} | ")
            f.write(f"{r['upload_time']:6.3f}s | {r['download_time']:6.3f}s | ")
            f.write(f"{r['upload_throughput_mbps']:7.2f} | {r['download_throughput_mbps']:7.2f}\n")
        
        # Add resource usage statistics if provided
        if resource_stats:
            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("CLIENT-SIDE RESOURCE USAGE\n")
            f.write("=" * 80 + "\n")
            f.write(f"Test Duration:         {resource_stats['duration_seconds']:.2f} seconds\n")
            f.write(f"Samples Collected:     {resource_stats['samples_collected']}\n")
            f.write(f"Sample Interval:       ~{resource_stats['duration_seconds']/resource_stats['samples_collected']:.3f} seconds\n")
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
            f.write("\n")
            f.write("Notes:\n")
            f.write(f"  - RSS: Resident Set Size (physical memory actually in RAM)\n")
            f.write(f"  - CPU percentages are per-core (can exceed 100% on multi-core systems)\n")
            f.write(f"  - Memory delta shows net memory increase/decrease during test\n")
            f.write(f"  - These metrics track ONLY the Python client process, not the iRODS server\n")
    
    return results_file, stats

def main():
    """Main execution function with resource monitoring"""
    print_color("=" * 80, Colors.GREEN)
    print_color("iRODS Performance Test (Python iRODS Client)", Colors.GREEN)
    print_color("Real Files with Optional Compression", Colors.GREEN)
    print_color("=" * 80, Colors.GREEN)
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    print(f"\nDate: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Test runs: {TEST_RUNS}")
    print(f"Test files directory: {TEST_FILES_DIR}")
    print(f"Compression: {'ENABLED' if ENABLE_COMPRESSION else 'DISABLED'}")
    
    # Get test files
    test_files = get_test_files(TEST_FILES_DIR)
    print_color(f"\nFound {len(test_files)} test file(s):", Colors.CYAN)
    for f in test_files:
        size = os.path.getsize(f)
        print(f"  - {os.path.basename(f):30s} ({format_size(size)})")
    
    # Start resource monitoring
    monitor = ResourceMonitor(interval=0.1)
    monitor.start()

    # Create iRODS session
    print_color("\nConnecting to iRODS...", Colors.YELLOW)
    session = create_session()
    print_color(f"✓ Connected as {session.username}@{session.zone}", Colors.GREEN)
    
    # Verify connection
    if not verify_connection(session):
        print_color("\nConnection verification failed. Please check your iRODS setup.", Colors.RED)
        session.cleanup()
        sys.exit(1)
    
    try:
        # Run performance test
        results = run_performance_test(session, test_files, TEST_RUNS, ENABLE_COMPRESSION)
        
        if not results:
            print_color("\nNo successful test runs. Cannot generate results.", Colors.RED)
            return
        
        # Stop resource monitoring before saving
        resource_stats = monitor.stop()
        
        # Save results including resource usage
        print_color("\nProcessing results...", Colors.GREEN)
        results_file, stats = save_results(
            RESULTS_DIR, timestamp, results, ENABLE_COMPRESSION, session, resource_stats
        )
        
        # Display summary
        print()
        print_color("=" * 80, Colors.GREEN)
        print_color("Test Results Summary", Colors.GREEN)
        print_color("=" * 80, Colors.GREEN)
        print(f"Successful runs: {stats['total_runs']}")
        print(f"Files tested: {', '.join(stats['files_tested'])}")
        print(f"Average upload time:      {stats['avg_upload_time']:.3f}s ({stats['avg_upload_throughput']:.2f} MB/s)")
        print(f"Average download time:    {stats['avg_download_time']:.3f}s ({stats['avg_download_throughput']:.2f} MB/s)")
        print(f"Total upload time:        {stats['total_upload_time']:.3f}s")
        print(f"Total download time:      {stats['total_download_time']:.3f}s")
        if ENABLE_COMPRESSION:
            print(f"Total compression time:   {stats['total_compress_time']:.3f}s")
            print(f"Total decompression time: {stats['total_decompress_time']:.3f}s")
        print()
        print_color(f"Results saved to: {results_file}", Colors.GREEN)
        
    except KeyboardInterrupt:
        print_color("\n\nTest interrupted by user", Colors.YELLOW)
        resource_stats = monitor.stop()
    except Exception as e:
        print_color(f"\nUnexpected error: {type(e).__name__}", Colors.RED)
        print_color(f"{str(e)}", Colors.RED)
        import traceback
        traceback.print_exc()
        resource_stats = monitor.stop()
    finally:
        session.cleanup()
    
    # Display resource usage summary to console
    if 'resource_stats' in locals():
        print(format_stats(resource_stats))
        print_color(f"\nNote: Resource usage also saved in the results file above", Colors.CYAN)
    
    print_color("\n" + "=" * 80, Colors.GREEN)
    print_color("Performance test completed!", Colors.GREEN)
    print_color("=" * 80, Colors.GREEN)

if __name__ == "__main__":
    main()
