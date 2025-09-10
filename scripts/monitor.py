#!/usr/bin/env python3
"""
STAR Alignment Monitor
Real-time monitoring of STAR alignment job progress.
"""

import subprocess
import csv
import json
import time
import argparse
import os
from pathlib import Path
from datetime import datetime
import sys

class STARMonitor:
    """Monitor STAR alignment job progress."""
    
    def __init__(self, workflow_dir):
        self.workflow_dir = Path(workflow_dir)
        self.manifest_file = self.workflow_dir / "data" / "sample_manifest.csv"
        self.log_dir = self.workflow_dir / "logs"
        self.bam_dir = self.workflow_dir / "outputs" / "bams"
        
    def get_lsf_status(self):
        """Get LSF job status."""
        try:
            result = subprocess.run(['bjobs', '-J', 'star_align*'], 
                                  capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError:
            return "No LSF jobs found"
    
    def get_sample_stats(self):
        """Get sample processing statistics."""
        try:
            with open(self.manifest_file, 'r') as f:
                reader = csv.DictReader(f)
                samples = list(reader)
        except Exception:
            return 0, 0, 0
        
        total_samples = len(samples)
        completed_samples = 0
        
        for sample in samples:
            sample_id = sample['sample_id']
            r1_path = sample['r1_path']
            
            # Extract directory structure from input path
            input_dir = os.path.dirname(r1_path)
            base_dir = "/data/salomonis-archive/czb-tabula-sapiens"
            output_base_dir = str(self.workflow_dir / "outputs")
            bam_output_dir = input_dir.replace(base_dir, output_base_dir)
            
            bam_file = Path(bam_output_dir) / f"{sample_id}.bam"
            if bam_file.exists() and bam_file.stat().st_size > 1000:
                completed_samples += 1
        
        pending_samples = total_samples - completed_samples
        return total_samples, completed_samples, pending_samples
    
    def get_log_stats(self):
        """Get log file statistics."""
        if not self.log_dir.exists():
            return 0, 0, 0
        
        log_files = list(self.log_dir.glob("star_align_*.out"))
        completed = 0
        failed = 0
        running = 0
        
        for log_file in log_files:
            try:
                with open(log_file, 'r') as f:
                    content = f.read()
                    if "STAR alignment completed successfully" in content:
                        completed += 1
                    elif "ERROR:" in content or "FAILED" in content:
                        failed += 1
                    else:
                        running += 1
            except Exception:
                continue
        
        return completed, failed, running
    
    def get_storage_usage(self):
        """Get storage usage."""
        try:
            result = subprocess.run(['du', '-sh', str(self.bam_dir)], 
                                  capture_output=True, text=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError:
            return "Unknown"
    
    def generate_report(self):
        """Generate status report."""
        print("=" * 60)
        print("🧬 STAR ALIGNMENT MONITOR")
        print("=" * 60)
        print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # LSF Status
        print("📊 LSF JOB STATUS:")
        print("-" * 20)
        lsf_status = self.get_lsf_status()
        print(lsf_status)
        print()
        
        # Sample Statistics
        print("📈 SAMPLE STATISTICS:")
        print("-" * 20)
        total, completed, pending = self.get_sample_stats()
        if total > 0:
            completion_rate = (completed / total) * 100
            print(f"Total samples: {total}")
            print(f"Completed: {completed}")
            print(f"Pending: {pending}")
            print(f"Completion rate: {completion_rate:.1f}%")
        else:
            print("No samples found")
        print()
        
        # Log Statistics
        print("📝 LOG STATISTICS:")
        print("-" * 15)
        log_completed, log_failed, log_running = self.get_log_stats()
        print(f"Logs completed: {log_completed}")
        print(f"Logs failed: {log_failed}")
        print(f"Logs running: {log_running}")
        print()
        
        # Storage Usage
        print("💾 STORAGE USAGE:")
        print("-" * 15)
        storage = self.get_storage_usage()
        print(f"BAM files: {storage}")
        print()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'total_samples': total,
            'completed_samples': completed,
            'pending_samples': pending,
            'completion_rate': (completed / total) * 100 if total > 0 else 0,
            'log_completed': log_completed,
            'log_failed': log_failed,
            'log_running': log_running
        }
    
    def monitor_loop(self, interval=300):
        """Continuous monitoring loop."""
        print(f"🔄 Starting continuous monitoring (interval: {interval}s)")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                self.generate_report()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n⏹️  Monitoring stopped")

def main():
    parser = argparse.ArgumentParser(description="Monitor STAR alignment jobs")
    parser.add_argument("--workflow-dir", default="/data/salomonis-archive/czb-tabula-sapiens/star_workflow",
                       help="Workflow directory")
    parser.add_argument("--monitor", action="store_true",
                       help="Start continuous monitoring")
    parser.add_argument("--interval", type=int, default=300,
                       help="Monitoring interval in seconds")
    
    args = parser.parse_args()
    
    monitor = STARMonitor(args.workflow_dir)
    
    if args.monitor:
        monitor.monitor_loop(args.interval)
    else:
        monitor.generate_report()

if __name__ == "__main__":
    main()