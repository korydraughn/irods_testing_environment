#!/usr/bin/env python3

"""
iRODS Compression Demo
Demonstrates the stark differences between running with and without compression
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-GUI backend
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

# Import functions from the benchmark script
from python_client_benchmark import (
    create_session,
    verify_connection,
    get_test_files,
    test_network_speed,
    select_compression_level,
    run_performance_test,
    cleanup_irods_directory,
    calculate_aggregate_statistics,
    format_size,
    print_color,
    Colors,
    ResourceMonitor,
    TEST_FILES_DIR,
    NETWORK_TEST_SIZE_MB,
    NETWORK_TEST_SAMPLES,
    ZSTD_AVAILABLE
)

# Demo configuration
DEMO_RUNS = 1  # Number of runs per test


def print_separator(char='=', length=80, color=Colors.CYAN):
    """Print a separator line"""
    print_color(char * length, color)


def print_header(title, color=Colors.CYAN):
    """Print a section header"""
    print()
    print_separator('=', 80, color)
    print_color(title.center(80), color)
    print_separator('=', 80, color)
    print()


def print_comparison_table(no_compression_stats, compression_stats, network_speed):
    """Print a side-by-side comparison table"""
    print_header("📊 PERFORMANCE COMPARISON", Colors.GREEN)

    # Calculate metrics for no compression
    nc_total_time = (no_compression_stats['total_compress_time'] +
                     no_compression_stats['total_upload_time'] +
                     no_compression_stats['total_download_time'] +
                     no_compression_stats['total_decompress_time'])
    nc_avg_time = nc_total_time / no_compression_stats['total_runs']

    # Calculate metrics for with compression
    c_total_time = (compression_stats['total_compress_time'] +
                    compression_stats['total_upload_time'] +
                    compression_stats['total_download_time'] +
                    compression_stats['total_decompress_time'])
    c_avg_time = c_total_time / compression_stats['total_runs']

    # Calculate data transferred
    nc_total_data = sum(r['original_size_bytes'] for r in no_compression_stats['_raw_results'])
    c_total_data = sum(r['transfer_size_bytes'] for r in compression_stats['_raw_results'])

    # Calculate average compression ratio
    avg_compression_ratio = sum(r['compression_ratio'] for r in compression_stats['_raw_results']) / len(compression_stats['_raw_results'])

    # Time savings
    time_saved = nc_total_time - c_total_time
    time_saved_pct = (time_saved / nc_total_time * 100) if nc_total_time > 0 else 0

    # Data savings
    data_saved = nc_total_data - c_total_data
    data_saved_pct = (data_saved / nc_total_data * 100) if nc_total_data > 0 else 0

    print("╔═══════════════════════════════════════╦═══════════════════╦═══════════════════╗")
    print("║ Metric                                ║ Without Compress. ║ With Compression  ║")
    print("╠═══════════════════════════════════════╬═══════════════════╬═══════════════════╣")

    # Total time
    print(f"║ Total Round-Trip Time                 ║ {nc_total_time:>13.2f}s    ║ {c_total_time:>13.2f}s    ║")
    print(f"║ Average per File                      ║ {nc_avg_time:>13.2f}s    ║ {c_avg_time:>13.2f}s    ║")
    print("╠═══════════════════════════════════════╬═══════════════════╬═══════════════════╣")

    # Upload time
    nc_upload = no_compression_stats['total_upload_time']
    c_upload = compression_stats['total_upload_time']
    print(f"║ Total Upload Time                     ║ {nc_upload:>13.2f}s    ║ {c_upload:>13.2f}s    ║")

    # Download time
    nc_download = no_compression_stats['total_download_time']
    c_download = compression_stats['total_download_time']
    print(f"║ Total Download Time                   ║ {nc_download:>13.2f}s    ║ {c_download:>13.2f}s    ║")

    # Compression time
    nc_compress = no_compression_stats['total_compress_time']
    c_compress = compression_stats['total_compress_time']
    print(f"║ Total Compression Time                ║ {nc_compress:>13.2f}s    ║ {c_compress:>13.2f}s    ║")

    # Decompression time
    nc_decompress = no_compression_stats['total_decompress_time']
    c_decompress = compression_stats['total_decompress_time']
    print(f"║ Total Decompression Time              ║ {nc_decompress:>13.2f}s    ║ {c_decompress:>13.2f}s    ║")

    print("╠═══════════════════════════════════════╬═══════════════════╬═══════════════════╣")

    # Data transferred
    print(f"║ Data Transferred (both ways)          ║ {format_size(nc_total_data * 2):>17s} ║ {format_size(c_total_data * 2):>17s} ║")
    print(f"║ Compression Ratio                     ║             0.0%  ║ {avg_compression_ratio:>14.1f}%  ║")

    print("╠═══════════════════════════════════════╬═══════════════════╬═══════════════════╣")

    # Throughput
    nc_throughput = no_compression_stats['avg_upload_throughput']
    c_throughput = compression_stats['avg_upload_throughput']
    print(f"║ Avg Upload Throughput                 ║ {nc_throughput:>11.2f} MB/s  ║ {c_throughput:>11.2f} MB/s  ║")

    nc_dl_throughput = no_compression_stats['avg_download_throughput']
    c_dl_throughput = compression_stats['avg_download_throughput']
    print(f"║ Avg Download Throughput               ║ {nc_dl_throughput:>11.2f} MB/s  ║ {c_dl_throughput:>11.2f} MB/s  ║")

    print("╚═══════════════════════════════════════╩═══════════════════╩═══════════════════╝")

    # Print improvements
    print()
    print_header("💡 KEY IMPROVEMENTS WITH COMPRESSION", Colors.GREEN)

    if time_saved > 0:
        print_color(f"⏱️  TIME SAVED: {time_saved:.2f} seconds ({time_saved_pct:.1f}% faster)", Colors.GREEN)
    else:
        print_color(f"⏱️  TIME DIFFERENCE: {abs(time_saved):.2f} seconds slower with compression", Colors.YELLOW)

    print_color(f"💾 DATA SAVED: {format_size(data_saved)} ({data_saved_pct:.1f}% reduction)", Colors.GREEN)
    print_color(f"📦 COMPRESSION RATIO: {avg_compression_ratio:.1f}% average", Colors.CYAN)

    # Calculate bandwidth savings
    if network_speed:
        bandwidth_saved_time = data_saved / (network_speed * 1024 * 1024)  # Convert MB/s to bytes/s
        print_color(f"🌐 NETWORK TIME SAVED: ~{bandwidth_saved_time:.2f} seconds of data transfer", Colors.CYAN)

    # Per-file breakdown
    print()
    print_header("📁 PER-FILE BREAKDOWN", Colors.CYAN)

    print("┌────────────────────────────────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ File                                   │ Original     │ Compressed   │ Ratio        │")
    print("├────────────────────────────────────────┼──────────────┼──────────────┼──────────────┤")

    for filename in compression_stats['files_tested']:
        file_results = compression_stats['files_data'][filename]
        original_size = file_results[0]['original_size_bytes']
        avg_compressed = sum(r['transfer_size_bytes'] for r in file_results) / len(file_results)
        avg_ratio = sum(r['compression_ratio'] for r in file_results) / len(file_results)

        # Truncate filename if too long
        display_name = filename[:38] if len(filename) <= 38 else filename[:35] + "..."

        print(f"│ {display_name:38s} │ {format_size(original_size):>12s} │ {format_size(avg_compressed):>12s} │ {avg_ratio:>11.1f}% │")

    print("└────────────────────────────────────────┴──────────────┴──────────────┴──────────────┘")

    # Show speedup per file
    print()
    print("┌────────────────────────────────────────┬──────────────┬──────────────┬──────────────┐")
    print("│ File                                   │ Time (No C.) │ Time (With)  │ Improvement  │")
    print("├────────────────────────────────────────┼──────────────┼──────────────┼──────────────┤")

    for filename in compression_stats['files_tested']:
        nc_file = no_compression_stats['files_data'][filename]
        c_file = compression_stats['files_data'][filename]

        nc_time = sum(r['upload_time'] + r['download_time'] + r['compress_time'] + r['decompress_time']
                     for r in nc_file) / len(nc_file)
        c_time = sum(r['upload_time'] + r['download_time'] + r['compress_time'] + r['decompress_time']
                    for r in c_file) / len(c_file)

        improvement = ((nc_time - c_time) / nc_time * 100) if nc_time > 0 else 0
        improvement_str = f"+{improvement:.1f}%" if improvement > 0 else f"{improvement:.1f}%"

        display_name = filename[:38] if len(filename) <= 38 else filename[:35] + "..."

        print(f"│ {display_name:38s} │ {nc_time:>10.2f}s  │ {c_time:>10.2f}s  │ {improvement_str:>12s} │")

    print("└────────────────────────────────────────┴──────────────┴──────────────┴──────────────┘")


def generate_comparison_graphs(stats_no_compression, stats_compression, resource_stats_no_comp,
                              resource_stats_comp, output_dir="./demo_graphs"):
    """Generate comparison graphs and save as PNG files"""
    if not MATPLOTLIB_AVAILABLE:
        print_color("\n⚠️  matplotlib not available - skipping graph generation", Colors.YELLOW)
        print_color("   Install with: pip install matplotlib", Colors.YELLOW)
        return []

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    generated_files = []

    # Set style
    plt.style.use('seaborn-v0_8-darkgrid' if 'seaborn-v0_8-darkgrid' in plt.style.available else 'default')

    # Graph 1: Total Time Comparison
    fig, ax = plt.subplots(figsize=(10, 6))

    nc_total = (stats_no_compression['total_compress_time'] +
               stats_no_compression['total_upload_time'] +
               stats_no_compression['total_download_time'] +
               stats_no_compression['total_decompress_time'])
    c_total = (stats_compression['total_compress_time'] +
              stats_compression['total_upload_time'] +
              stats_compression['total_download_time'] +
              stats_compression['total_decompress_time'])

    categories = ['Without\nCompression', 'With\nCompression']
    times = [nc_total, c_total]
    colors = ['#e74c3c', '#2ecc71']

    bars = ax.bar(categories, times, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Total Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('Total Round-Trip Time Comparison', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='y', alpha=0.3)

    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}s',
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    # Add improvement percentage
    improvement = ((nc_total - c_total) / nc_total * 100) if nc_total > 0 else 0
    if improvement > 0:
        ax.text(0.5, max(times) * 0.95, f'{improvement:.1f}% faster',
                ha='center', fontsize=12, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.8))

    plt.tight_layout()
    graph1_path = os.path.join(output_dir, f'total_time_comparison_{timestamp}.png')
    plt.savefig(graph1_path, dpi=300, bbox_inches='tight')
    plt.close()
    generated_files.append(graph1_path)

    # Graph 2: Time Breakdown (Stacked Bar Chart)
    fig, ax = plt.subplots(figsize=(10, 6))

    nc_times = [stats_no_compression['total_compress_time'],
                stats_no_compression['total_upload_time'],
                stats_no_compression['total_download_time'],
                stats_no_compression['total_decompress_time']]
    c_times = [stats_compression['total_compress_time'],
               stats_compression['total_upload_time'],
               stats_compression['total_download_time'],
               stats_compression['total_decompress_time']]

    x = np.arange(2)
    width = 0.6
    labels = ['Compress', 'Upload', 'Download', 'Decompress']
    colors_stack = ['#3498db', '#e74c3c', '#f39c12', '#9b59b6']

    bottom_nc = 0
    bottom_c = 0

    for i, (label, color) in enumerate(zip(labels, colors_stack)):
        ax.bar(0, nc_times[i], width, bottom=bottom_nc, label=label, color=color, alpha=0.8, edgecolor='black')
        ax.bar(1, c_times[i], width, bottom=bottom_c, color=color, alpha=0.8, edgecolor='black')
        bottom_nc += nc_times[i]
        bottom_c += c_times[i]

    ax.set_ylabel('Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('Time Breakdown by Operation', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(['Without\nCompression', 'With\nCompression'])
    ax.legend(loc='upper right', fontsize=10)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    graph2_path = os.path.join(output_dir, f'time_breakdown_{timestamp}.png')
    plt.savefig(graph2_path, dpi=300, bbox_inches='tight')
    plt.close()
    generated_files.append(graph2_path)

    # Graph 3: Data Transfer Comparison
    fig, ax = plt.subplots(figsize=(10, 6))

    nc_total_data = sum(r['original_size_bytes'] for r in stats_no_compression['_raw_results']) * 2  # both ways
    c_total_data = sum(r['transfer_size_bytes'] for r in stats_compression['_raw_results']) * 2

    nc_total_mb = nc_total_data / (1024 * 1024)
    c_total_mb = c_total_data / (1024 * 1024)

    bars = ax.bar(categories, [nc_total_mb, c_total_mb], color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Data Transferred (MB)', fontsize=12, fontweight='bold')
    ax.set_title('Total Data Transferred (Upload + Download)', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='y', alpha=0.3)

    # Add value labels
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f} MB',
                ha='center', va='bottom', fontsize=11, fontweight='bold')

    # Add data saved
    data_saved_pct = ((nc_total_mb - c_total_mb) / nc_total_mb * 100) if nc_total_mb > 0 else 0
    ax.text(0.5, max([nc_total_mb, c_total_mb]) * 0.95,
            f'{data_saved_pct:.1f}% less data',
            ha='center', fontsize=12, fontweight='bold',
            bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.8))

    plt.tight_layout()
    graph3_path = os.path.join(output_dir, f'data_transfer_{timestamp}.png')
    plt.savefig(graph3_path, dpi=300, bbox_inches='tight')
    plt.close()
    generated_files.append(graph3_path)

    # Graph 4: Resource Usage Comparison
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # CPU Usage
    cpu_categories = ['Avg CPU', 'Peak CPU']
    nc_cpu = [resource_stats_no_comp['avg_cpu_percent'], resource_stats_no_comp['max_cpu_percent']]
    c_cpu = [resource_stats_comp['avg_cpu_percent'], resource_stats_comp['max_cpu_percent']]

    x = np.arange(len(cpu_categories))
    width = 0.35

    bars1 = ax1.bar(x - width/2, nc_cpu, width, label='Without Compression',
                    color='#e74c3c', alpha=0.8, edgecolor='black')
    bars2 = ax1.bar(x + width/2, c_cpu, width, label='With Compression',
                    color='#2ecc71', alpha=0.8, edgecolor='black')

    ax1.set_ylabel('CPU Usage (%)', fontsize=11, fontweight='bold')
    ax1.set_title('CPU Usage Comparison', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(cpu_categories)
    ax1.legend(fontsize=9)
    ax1.grid(axis='y', alpha=0.3)

    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}%',
                    ha='center', va='bottom', fontsize=9)

    # Memory Usage
    mem_categories = ['Avg Memory', 'Peak Memory']
    nc_mem = [resource_stats_no_comp['avg_memory_mb'], resource_stats_no_comp['peak_memory_mb']]
    c_mem = [resource_stats_comp['avg_memory_mb'], resource_stats_comp['peak_memory_mb']]

    bars3 = ax2.bar(x - width/2, nc_mem, width, label='Without Compression',
                    color='#e74c3c', alpha=0.8, edgecolor='black')
    bars4 = ax2.bar(x + width/2, c_mem, width, label='With Compression',
                    color='#2ecc71', alpha=0.8, edgecolor='black')

    ax2.set_ylabel('Memory Usage (MB)', fontsize=11, fontweight='bold')
    ax2.set_title('Memory Usage Comparison', fontsize=12, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(mem_categories)
    ax2.legend(fontsize=9)
    ax2.grid(axis='y', alpha=0.3)

    # Add value labels
    for bars in [bars3, bars4]:
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.1f}',
                    ha='center', va='bottom', fontsize=9)

    plt.tight_layout()
    graph4_path = os.path.join(output_dir, f'resource_usage_{timestamp}.png')
    plt.savefig(graph4_path, dpi=300, bbox_inches='tight')
    plt.close()
    generated_files.append(graph4_path)

    # Graph 5: Per-File Compression Ratio
    if len(stats_compression['files_tested']) > 0:
        fig, ax = plt.subplots(figsize=(12, 6))

        filenames = []
        compression_ratios = []

        for filename in stats_compression['files_tested']:
            file_results = stats_compression['files_data'][filename]
            avg_ratio = sum(r['compression_ratio'] for r in file_results) / len(file_results)

            # Truncate long filenames
            display_name = filename[:25] + '...' if len(filename) > 25 else filename
            filenames.append(display_name)
            compression_ratios.append(avg_ratio)

        bars = ax.barh(filenames, compression_ratios, color='#3498db', alpha=0.8, edgecolor='black', linewidth=1.5)
        ax.set_xlabel('Compression Ratio (%)', fontsize=12, fontweight='bold')
        ax.set_title('Compression Ratio by File', fontsize=14, fontweight='bold', pad=20)
        ax.grid(axis='x', alpha=0.3)

        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(width, bar.get_y() + bar.get_height()/2.,
                    f' {width:.1f}%',
                    ha='left', va='center', fontsize=10, fontweight='bold')

        plt.tight_layout()
        graph5_path = os.path.join(output_dir, f'compression_ratio_by_file_{timestamp}.png')
        plt.savefig(graph5_path, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(graph5_path)

    return generated_files


def run_demo():
    """Run the compression demo"""
    print_header("🚀 iRODS COMPRESSION DEMO", Colors.GREEN)
    print_color("This demo will run benchmarks with and without compression", Colors.CYAN)
    print_color("to showcase the performance differences.", Colors.CYAN)
    print()

    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    print(f"Demo started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Test runs per configuration: {DEMO_RUNS}")

    if not ZSTD_AVAILABLE:
        print_color("\nError: zstandard not installed! Install with: pip install zstandard", Colors.RED)
        sys.exit(1)

    # Get test files
    test_files = get_test_files(TEST_FILES_DIR)
    print_color(f"\nFound {len(test_files)} test file(s):", Colors.CYAN)
    for f in test_files:
        size = os.path.getsize(f)
        print(f"  📄 {os.path.basename(f):30s} ({format_size(size)})")

    # Connect to iRODS
    print_header("🔌 CONNECTING TO iRODS", Colors.YELLOW)
    session = create_session()
    print_color(f"✓ Connected as {session.username}@{session.zone}", Colors.GREEN)

    if not verify_connection(session):
        session.cleanup()
        sys.exit(1)

    try:
        # Clean up demo_graphs directory if it has files
        demo_graphs_dir = Path("./demo_graphs")
        if demo_graphs_dir.exists() and any(demo_graphs_dir.iterdir()):
            print_color("Cleaning up previous demo graphs...", Colors.YELLOW)
            for file in demo_graphs_dir.iterdir():
                if file.is_file():
                    file.unlink()
                    print_color(f"  Removed: {file.name}", Colors.YELLOW)
            print_color("✓ Demo graphs cleanup complete", Colors.GREEN)
            print()

        # Clean up before starting
        cleanup_irods_directory(session)

        # ====================
        # TEST 1: NO COMPRESSION
        # ====================
        print_header("📤 TEST 1: WITHOUT COMPRESSION", Colors.YELLOW)
        print_color("Running benchmark with compression DISABLED...", Colors.CYAN)
        print()

        monitor_no_comp = ResourceMonitor(interval=0.1)
        monitor_no_comp.start()

        results_no_compression = run_performance_test(
            session=session,
            test_files=test_files,
            test_runs=DEMO_RUNS,
            base_compression_level=0,  # No compression
            enable_verification=True,
            enable_metadata=False,
            network_speed_mbps=None
        )

        resource_stats_no_comp = monitor_no_comp.stop()

        if not results_no_compression:
            print_color("\nNo successful test runs without compression", Colors.RED)
            session.cleanup()
            return

        stats_no_compression = calculate_aggregate_statistics(results_no_compression)
        stats_no_compression['_raw_results'] = results_no_compression

        print_color("\n✓ Test 1 completed successfully", Colors.GREEN)

        # Clean up between tests
        cleanup_irods_directory(session)

        # Test network speed
        print_header("🌐 TESTING NETWORK SPEED", Colors.CYAN)
        print_color("Testing network speed to determine optimal compression level...", Colors.CYAN)
        print()

        upload_speed, download_speed, latency = test_network_speed(
            session, NETWORK_TEST_SIZE_MB, NETWORK_TEST_SAMPLES
        )

        if upload_speed is None:
            print_color("\nNetwork test failed, cannot continue demo", Colors.RED)
            session.cleanup()
            sys.exit(1)

        avg_speed = (upload_speed + download_speed) / 2
        compression_level = select_compression_level(avg_speed)

        # Clean up after network test
        cleanup_irods_directory(session)

        # ====================
        # TEST 2: WITH ADAPTIVE COMPRESSION
        # ====================
        print_header("📦 TEST 2: WITH ADAPTIVE COMPRESSION", Colors.YELLOW)
        print_color("Running benchmark with compression ENABLED...", Colors.CYAN)
        print()

        monitor_comp = ResourceMonitor(interval=0.1)
        monitor_comp.start()

        results_compression = run_performance_test(
            session=session,
            test_files=test_files,
            test_runs=DEMO_RUNS,
            base_compression_level=compression_level,
            enable_verification=True,
            enable_metadata=True,
            network_speed_mbps=avg_speed
        )

        resource_stats_comp = monitor_comp.stop()

        if not results_compression:
            print_color("\nNo successful test runs with compression", Colors.RED)
            session.cleanup()
            return

        stats_compression = calculate_aggregate_statistics(results_compression)
        stats_compression['_raw_results'] = results_compression

        print_color("\n✓ Test 2 completed successfully", Colors.GREEN)

        # Clean up after tests
        cleanup_irods_directory(session)

        # ====================
        # SHOW COMPARISON
        # ====================
        print_comparison_table(stats_no_compression, stats_compression, avg_speed)

        # Resource usage comparison
        print()
        print_header("💻 CLIENT-SIDE RESOURCE USAGE COMPARISON", Colors.MAGENTA)

        print("╔═══════════════════════════════════╦═══════════════════╦═══════════════════╗")
        print("║ Resource                          ║ Without Compress. ║ With Compression  ║")
        print("╠═══════════════════════════════════╬═══════════════════╬═══════════════════╣")
        print(f"║ Average CPU Usage                 ║ {resource_stats_no_comp['avg_cpu_percent']:>13.1f}%    ║ {resource_stats_comp['avg_cpu_percent']:>13.1f}%    ║")
        print(f"║ Peak CPU Usage                    ║ {resource_stats_no_comp['max_cpu_percent']:>13.1f}%    ║ {resource_stats_comp['max_cpu_percent']:>13.1f}%    ║")
        print(f"║ Average Memory (RSS)              ║ {resource_stats_no_comp['avg_memory_mb']:>12.1f} MB  ║ {resource_stats_comp['avg_memory_mb']:>12.1f} MB  ║")
        print(f"║ Peak Memory (RSS)                 ║ {resource_stats_no_comp['peak_memory_mb']:>12.1f} MB  ║ {resource_stats_comp['peak_memory_mb']:>12.1f} MB  ║")
        print("╚═══════════════════════════════════╩═══════════════════╩═══════════════════╝")

        # Final summary
        print()
        print_header("✅ DEMO COMPLETED", Colors.GREEN)

        nc_total = (stats_no_compression['total_compress_time'] +
                   stats_no_compression['total_upload_time'] +
                   stats_no_compression['total_download_time'] +
                   stats_no_compression['total_decompress_time'])
        c_total = (stats_compression['total_compress_time'] +
                  stats_compression['total_upload_time'] +
                  stats_compression['total_download_time'] +
                  stats_compression['total_decompress_time'])

        time_saved = nc_total - c_total
        time_saved_pct = (time_saved / nc_total * 100) if nc_total > 0 else 0

        if time_saved > 0:
            print_color(f"✨ Compression saved {time_saved:.2f} seconds ({time_saved_pct:.1f}% faster)!", Colors.GREEN)
            print_color("   Adaptive compression is RECOMMENDED for this network.", Colors.GREEN)
        else:
            print_color(f"⚠️  Compression was {abs(time_saved):.2f} seconds slower on this fast network.", Colors.YELLOW)
            print_color("   Consider using lower compression level or no compression.", Colors.YELLOW)

        print()
        print_color(f"Demo completed at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}", Colors.CYAN)

        # Generate comparison graphs
        print()
        print_header("📊 GENERATING COMPARISON GRAPHS", Colors.CYAN)
        graph_files = generate_comparison_graphs(
            stats_no_compression,
            stats_compression,
            resource_stats_no_comp,
            resource_stats_comp
        )

        if graph_files:
            print_color(f"✓ Generated {len(graph_files)} graph(s):", Colors.GREEN)
            for graph_file in graph_files:
                print_color(f"  📈 {graph_file}", Colors.CYAN)
        print()

    except KeyboardInterrupt:
        print_color("\n\n⚠️  Demo interrupted by user", Colors.YELLOW)
    except Exception as e:
        print_color(f"\n❌ Error during demo: {e}", Colors.RED)
        import traceback
        traceback.print_exc()
    finally:
        session.cleanup()

    print()
    print_separator('=', 80, Colors.GREEN)


if __name__ == "__main__":
    run_demo()
