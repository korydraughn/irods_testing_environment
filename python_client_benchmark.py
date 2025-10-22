#!/usr/bin/env python3

"""
iRODS Performance Test Script (Python iRODS Client)
Tests file upload/download times using the Python iRODS Client and calculates averages
"""

import os
import sys
import time
import tempfile
from datetime import datetime
from pathlib import Path
from irods.session import iRODSSession
from irods.exception import iRODSException
from resource_monitor import ResourceMonitor, format_stats, save_stats_to_file


# Configuration
TEST_RUNS = 25
TEST_FILE_SIZE_MB = 100  # Size in megabytes
RESULTS_DIR = "./performance_results"

# ANSI color codes
class Colors:
    RED = '\033[0;31m'
    GREEN = '\033[0;32m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
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

def create_session():
    """Create iRODS session from environment file"""
    try:
        env_file = os.environ.get('IRODS_ENVIRONMENT_FILE', 
                                   os.path.expanduser('~/.irods/irods_environment.json'))
        
        print_color(f"Using environment file: {env_file}", Colors.BLUE)
        
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
        # Test basic connectivity
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
        
        # Test write permissions by creating a small test file
        test_path = f"{home_path}/.performance_test_{int(time.time())}.tmp"
        test_data = b"test"
        
        try:
            # Create a temporary local file
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(test_data)
                tmp_file = tmp.name
            
            # Try to upload
            session.data_objects.put(tmp_file, test_path)
            print_color(f"  ✓ Write test successful", Colors.GREEN)
            
            # Try to download
            download_file = tempfile.mktemp(suffix='.tmp')
            session.data_objects.get(test_path, download_file)
            print_color(f"  ✓ Read test successful", Colors.GREEN)
            
            # Cleanup
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

def create_test_file(size_mb):
    """Create a test file of specified size"""
    print_color(f"\nCreating test file ({size_mb}MB)...", Colors.YELLOW)
    
    # Create temporary file
    fd, filepath = tempfile.mkstemp(suffix='.dat', prefix='irods_test_')
    os.close(fd)
    
    # Write random data
    with open(filepath, 'wb') as f:
        # Write in chunks to avoid memory issues
        chunk_size = 1024 * 1024  # 1MB chunks
        remaining = size_mb * 1024 * 1024
        while remaining > 0:
            chunk = min(chunk_size, remaining)
            f.write(os.urandom(chunk))
            remaining -= chunk
    
    print_color(f"  Test file created: {filepath}", Colors.GREEN)
    return filepath

def run_performance_test(session, test_file, test_runs, file_size_mb):
    """Run the performance test"""
    upload_times = []
    download_times = []
    
    # Get iRODS path
    zone = session.zone
    username = session.username
    test_filename = f"test_file_{file_size_mb}MB_{int(time.time())}.dat"
    irods_path = f"/{zone}/home/{username}/{test_filename}"
    
    print_color(f"\nRunning {test_runs} test iterations...", Colors.YELLOW)
    print_color(f"  iRODS path: {irods_path}", Colors.BLUE)
    
    failed_runs = 0
    
    for i in range(1, test_runs + 1):
        print(f"  Run {i}/{test_runs}: ", end='', flush=True)
        
        try:
            # Test upload (put)
            print("Upload... ", end='', flush=True)
            start_time = time.time()
            session.data_objects.put(test_file, irods_path, force=True)
            end_time = time.time()
            upload_time = end_time - start_time
            upload_times.append(upload_time)
            
            # Verify file was uploaded
            try:
                obj = session.data_objects.get(irods_path)
                if obj.size != file_size_mb * 1024 * 1024:
                    print_color(f"Warning: File size mismatch!", Colors.YELLOW)
            except:
                pass
            
            # Test download (get)
            print("Download... ", end='', flush=True)
            download_file = tempfile.mktemp(suffix='.dat')
            
            start_time = time.time()
            # Use force=True and specify options to avoid permission issues
            session.data_objects.get(irods_path, download_file, force=True, **{})
            end_time = time.time()
            download_time = end_time - start_time
            download_times.append(download_time)
            
            # Verify downloaded file
            download_size = os.path.getsize(download_file)
            expected_size = file_size_mb * 1024 * 1024
            if download_size != expected_size:
                print_color(f"Warning: Downloaded size {download_size} != expected {expected_size}", 
                           Colors.YELLOW)
            
            # Clean up downloaded file
            os.unlink(download_file)
            
            # Clean up iRODS file for next iteration
            session.data_objects.unlink(irods_path, force=True)
            
            print_color(f"Done (Upload: {upload_time:.3f}s, Download: {download_time:.3f}s)", 
                       Colors.GREEN)
            
        except iRODSException as e:
            print_color(f"FAILED", Colors.RED)
            print_error_details(e)
            failed_runs += 1
            
            # Try to cleanup if file exists
            try:
                session.data_objects.unlink(irods_path, force=True)
            except:
                pass
            
            # If too many failures, stop
            if failed_runs > test_runs // 2:
                print_color(f"\nToo many failures ({failed_runs}), stopping test.", Colors.RED)
                break
                
            continue
        except Exception as e:
            print_color(f"FAILED - Unexpected error", Colors.RED)
            print_color(f"  {type(e).__name__}: {str(e)}", Colors.RED)
            failed_runs += 1
            continue
    
    if failed_runs > 0:
        print_color(f"\nWarning: {failed_runs} runs failed out of {test_runs}", Colors.YELLOW)
    
    return upload_times, download_times

def calculate_statistics(times, file_size_mb):
    """Calculate average time and throughput"""
    if not times:
        return 0.0, 0.0
    
    avg_time = sum(times) / len(times)
    throughput = file_size_mb / avg_time if avg_time > 0 else 0
    
    return avg_time, throughput

def save_results(results_dir, timestamp, test_runs, file_size_mb, 
                upload_times, download_times, session):
    """Save results to file"""
    # Calculate statistics
    upload_avg, upload_throughput = calculate_statistics(upload_times, file_size_mb)
    download_avg, download_throughput = calculate_statistics(download_times, file_size_mb)
    
    # Create results directory
    Path(results_dir).mkdir(parents=True, exist_ok=True)
    
    # Results filename
    results_file = os.path.join(results_dir, f"python_client_benchamark{timestamp}.txt")
    
    # Format times for output
    upload_times_str = ' '.join(f"{t:.3f}" for t in upload_times)
    download_times_str = ' '.join(f"{t:.3f}" for t in download_times)
    
    # Write results
    with open(results_file, 'w') as f:
        f.write("iRODS Performance Test Results (Python iRODS Client)\n")
        f.write("=" * 60 + "\n")
        f.write(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write(f"Timestamp: {timestamp}\n")
        f.write(f"User: {os.getenv('USER', 'unknown')}\n")
        f.write(f"iRODS User: {session.username}\n")
        f.write(f"iRODS Zone: {session.zone}\n")
        f.write(f"iRODS Host: {session.host}:{session.port}\n")
        f.write(f"Test runs: {test_runs}\n")
        f.write(f"Successful runs: {len(upload_times)}\n")
        f.write(f"Test file size: {file_size_mb}MB\n")
        f.write(f"Method: Python iRODS Client (PRC)\n")
        f.write("\n")
        f.write("Test Results:\n")
        f.write("-" * 60 + "\n")
        f.write(f"Upload times (seconds):   {upload_times_str}\n")
        f.write(f"Download times (seconds): {download_times_str}\n")
        f.write(f"Average upload time:      {upload_avg:.3f}s\n")
        f.write(f"Average download time:    {download_avg:.3f}s\n")
        f.write(f"Total upload time: {(upload_avg * len(upload_times)):.3f}\n")
        f.write(f"Total download time: {(download_avg * len(download_times)):.3f}\n")
        f.write(f"Upload throughput:        {upload_throughput:.2f} MB/s\n")
        f.write(f"Download throughput:      {download_throughput:.2f} MB/s\n")
        f.write("\n")
        f.write("Note: These results are from the Python iRODS Client (PRC),\n")
        f.write("      not from iCommands directly.\n")
    
    return results_file, upload_avg, download_avg, upload_throughput, download_throughput

def main():
    """Main execution function with resource monitoring"""
    print_color("=== iRODS Performance Test (Python iRODS Client) ===", Colors.GREEN)
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    print(f"Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Test runs: {TEST_RUNS}")
    print(f"Test file size: {TEST_FILE_SIZE_MB}MB")
    print(f"Method: Python iRODS Client\n")
    
    monitor = ResourceMonitor(interval=0.1)  # Sample every 100ms
    monitor.start()

    # Create iRODS session
    print_color("Connecting to iRODS...", Colors.YELLOW)
    session = create_session()
    print_color(f"✓ Connected as {session.username}@{session.zone}", Colors.GREEN)
    
    # Verify connection and permissions
    if not verify_connection(session):
        print_color("\nConnection verification failed. Please check your iRODS setup.", Colors.RED)
        print_color("Run 'iinit' to configure authentication", Colors.YELLOW)
        session.cleanup()
        sys.exit(1)
    
    # Create test file
    test_file = None
    try:
        test_file = create_test_file(TEST_FILE_SIZE_MB)
        
        # Run performance test
        upload_times, download_times = run_performance_test(
            session, test_file, TEST_RUNS, TEST_FILE_SIZE_MB
        )
        
        if not upload_times or not download_times:
            print_color("\nNo successful test runs. Cannot generate results.", Colors.RED)
            return
        
        # Save and display results
        print_color("\nProcessing results...", Colors.GREEN)
        results_file, upload_avg, download_avg, upload_throughput, download_throughput = save_results(
            RESULTS_DIR, timestamp, TEST_RUNS, TEST_FILE_SIZE_MB,
            upload_times, download_times, session
        )
        
        print()
        print_color("=== Results ===", Colors.GREEN)
        print(f"Successful runs: {len(upload_times)}/{TEST_RUNS}")
        print(f"Upload times:   {' '.join(f'{t:.3f}' for t in upload_times)}")
        print(f"Download times: {' '.join(f'{t:.3f}' for t in download_times)}")
        print(f"Average upload time:      {upload_avg:.3f}s ({upload_throughput:.2f} MB/s)")
        print(f"Average download time:    {download_avg:.3f}s ({download_throughput:.2f} MB/s)")
        print(f"Total upload time: {(upload_avg * len(upload_times)):.3f}")
        print(f"Total download time: {(download_avg * len(download_times)):.3f}")        
        print()
        print_color(f"Results saved to: {results_file}", Colors.GREEN)
        print_color("Performance test completed!", Colors.GREEN)
        
    except KeyboardInterrupt:
        print_color("\n\nTest interrupted by user", Colors.YELLOW)
    except Exception as e:
        print_color(f"\nUnexpected error: {type(e).__name__}", Colors.RED)
        print_color(f"{str(e)}", Colors.RED)
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        if test_file and os.path.exists(test_file):
            os.unlink(test_file)
        session.cleanup()
    resource_stats = monitor.stop()

    # Display resource usage
    print(format_stats(resource_stats))

    # Save resource usage to file
    resource_file = os.path.join(RESULTS_DIR, f"resource_usage_{timestamp}.txt")
    save_stats_to_file(resource_stats, resource_file)
    print_color(f"\nResource usage saved to: {resource_file}", Colors.GREEN)

if __name__ == "__main__":
    main()
