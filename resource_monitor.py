#!/usr/bin/env python3

"""
Resource Monitor Module for iRODS Performance Testing
Tracks CPU and memory usage during benchmark execution
"""

import time
import threading
from datetime import datetime

try:
    import psutil
except ImportError:
    print("ERROR: psutil module not found. Install with: pip install psutil")
    print("Run: pip install psutil")
    import sys
    sys.exit(1)


class ResourceMonitor:
    """Monitor CPU and memory usage in a background thread"""
    
    def __init__(self, interval=0.1):
        """
        Initialize the resource monitor
        
        Args:
            interval: Sampling interval in seconds (default 0.1 = 100ms)
        """
        self.process = psutil.Process()
        self.interval = interval
        self.monitoring = False
        self.thread = None
        
        # Storage for measurements
        self.cpu_samples = []
        self.memory_samples = []
        self.start_time = None
        self.end_time = None
        self.start_memory = 0
        self.peak_memory = 0
        
    def start(self):
        """Start monitoring resources"""
        self.monitoring = True
        self.cpu_samples = []
        self.memory_samples = []
        self.start_time = time.time()
        
        # Get baseline memory
        mem_info = self.process.memory_info()
        self.start_memory = mem_info.rss / (1024 * 1024)  # MB
        self.peak_memory = self.start_memory
        
        # Initialize CPU percent (first call always returns 0)
        self.process.cpu_percent(interval=None)
        
        # Start monitoring thread
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        
        print(f"[ResourceMonitor] Started monitoring (baseline memory: {self.start_memory:.2f} MB)")
    
    def stop(self):
        """Stop monitoring and return statistics"""
        self.monitoring = False
        self.end_time = time.time()
        
        if self.thread:
            self.thread.join(timeout=2.0)
        
        # Get final memory reading
        final_mem_info = self.process.memory_info()
        final_memory = final_mem_info.rss / (1024 * 1024)
        
        # Calculate statistics
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        max_cpu = max(self.cpu_samples) if self.cpu_samples else 0
        avg_memory = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        duration = self.end_time - self.start_time if self.start_time else 0
        
        stats = {
            'duration_seconds': duration,
            'samples_collected': len(self.cpu_samples),
            'avg_cpu_percent': avg_cpu,
            'max_cpu_percent': max_cpu,
            'avg_memory_mb': avg_memory,
            'start_memory_mb': self.start_memory,
            'final_memory_mb': final_memory,
            'peak_memory_mb': self.peak_memory,
            'memory_delta_mb': final_memory - self.start_memory,
        }
        
        print(f"[ResourceMonitor] Stopped monitoring ({len(self.cpu_samples)} samples collected)")
        
        return stats
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while self.monitoring:
            try:
                # Get CPU percentage
                cpu_percent = self.process.cpu_percent(interval=None)
                self.cpu_samples.append(cpu_percent)
                
                # Get memory usage (RSS - Resident Set Size)
                mem_info = self.process.memory_info()
                memory_mb = mem_info.rss / (1024 * 1024)  # Convert to MB
                self.memory_samples.append(memory_mb)
                
                # Track peak memory
                if memory_mb > self.peak_memory:
                    self.peak_memory = memory_mb
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            
            time.sleep(self.interval)
    
    def get_stats(self):
        """Get current statistics without stopping monitoring"""
        if not self.cpu_samples:
            return None
        
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples)
        avg_memory = sum(self.memory_samples) / len(self.memory_samples)
        
        return {
            'avg_cpu_percent': avg_cpu,
            'avg_memory_mb': avg_memory,
            'peak_memory_mb': self.peak_memory,
            'samples': len(self.cpu_samples)
        }


def format_stats(stats):
    """Format statistics as a readable string"""
    if not stats:
        return "No statistics available"
    
    lines = [
        "\n" + "=" * 60,
        "Resource Usage Statistics",
        "=" * 60,
        f"Duration:              {stats['duration_seconds']:.2f} seconds",
        f"Samples collected:     {stats['samples_collected']}",
        "",
        "CPU Usage:",
        f"  Average CPU:         {stats['avg_cpu_percent']:.2f}%",
        f"  Peak CPU:            {stats['max_cpu_percent']:.2f}%",
        "",
        "Memory Usage:",
        f"  Start memory (RSS):  {stats['start_memory_mb']:.2f} MB",
        f"  Average memory:      {stats['avg_memory_mb']:.2f} MB",
        f"  Peak memory:         {stats['peak_memory_mb']:.2f} MB",
        f"  Final memory:        {stats['final_memory_mb']:.2f} MB",
        f"  Memory delta:        {stats['memory_delta_mb']:+.2f} MB",
        "=" * 60,
    ]
    
    return "\n".join(lines)


def save_stats_to_file(stats, filepath):
    """Save statistics to a file"""
    with open(filepath, 'w') as f:
        f.write("iRODS Performance Test - Resource Usage Report\n")
        f.write("=" * 60 + "\n")
        f.write(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        f.write("\n")
        f.write(f"Test Duration:         {stats['duration_seconds']:.2f} seconds\n")
        f.write(f"Samples Collected:     {stats['samples_collected']}\n")
        f.write(f"Sample Interval:       ~{stats['duration_seconds']/stats['samples_collected']:.3f} seconds\n")
        f.write("\n")
        f.write("CPU Usage:\n")
        f.write(f"  Average CPU:         {stats['avg_cpu_percent']:.2f}%\n")
        f.write(f"  Peak CPU:            {stats['max_cpu_percent']:.2f}%\n")
        f.write("\n")
        f.write("Memory Usage:\n")
        f.write(f"  Start memory (RSS):  {stats['start_memory_mb']:.2f} MB\n")
        f.write(f"  Average memory:      {stats['avg_memory_mb']:.2f} MB\n")
        f.write(f"  Peak memory:         {stats['peak_memory_mb']:.2f} MB\n")
        f.write(f"  Final memory:        {stats['final_memory_mb']:.2f} MB\n")
        f.write(f"  Memory delta:        {stats['memory_delta_mb']:+.2f} MB\n")
        f.write("\n")
        f.write("Notes:\n")
        f.write("  - RSS: Resident Set Size (physical memory actually in RAM)\n")
        f.write("  - CPU percentages are per-core (can exceed 100% on multi-core systems)\n")
        f.write("  - Memory delta shows net memory increase/decrease during test\n")


# Example usage
if __name__ == "__main__":
    print("Resource Monitor Test\n")
    
    # Create monitor
    monitor = ResourceMonitor(interval=0.1)
    
    # Start monitoring
    monitor.start()
    
    # Simulate some work
    print("Doing some work...")
    data = []
    for i in range(10):
        # Allocate some memory
        data.append([0] * (1024 * 1024))  # ~8MB per iteration
        time.sleep(0.5)
    
    # Stop monitoring and get stats
    stats = monitor.stop()
    
    # Display results
    print(format_stats(stats))
    
    # Save to file
    save_stats_to_file(stats, "resource_usage_test.txt")
    print("\nStats saved to: resource_usage_test.txt")
