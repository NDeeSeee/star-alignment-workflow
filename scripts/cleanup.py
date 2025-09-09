#!/usr/bin/env python3
"""
STAR Alignment Cleanup Script
Manages storage and cleanup operations for STAR alignment workflow.
"""

import os
import shutil
import subprocess
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import json

class CleanupManager:
    """Manage cleanup operations for STAR alignment workflow."""
    
    def __init__(self, workflow_dir):
        self.workflow_dir = Path(workflow_dir)
        self.outputs_dir = self.workflow_dir / "outputs"
        self.logs_dir = self.workflow_dir / "logs"
        self.temp_dir = self.workflow_dir / "temp"
        
    def get_disk_usage(self):
        """Get disk usage information."""
        try:
            result = subprocess.run(['df', '-h', str(self.workflow_dir)], 
                                  capture_output=True, text=True, check=True)
            lines = result.stdout.strip().split('\n')
            if len(lines) >= 2:
                parts = lines[1].split()
                return {
                    'filesystem': parts[0],
                    'size': parts[1],
                    'used': parts[2],
                    'available': parts[3],
                    'use_percent': parts[4],
                    'mounted_on': parts[5]
                }
        except subprocess.CalledProcessError:
            return None
    
    def get_directory_size(self, path):
        """Get size of directory."""
        try:
            result = subprocess.run(['du', '-sh', str(path)], 
                                  capture_output=True, text=True, check=True)
            return result.stdout.split()[0]
        except subprocess.CalledProcessError:
            return "Unknown"
    
    def cleanup_temp_files(self, older_than_hours=24):
        """Clean up temporary files older than specified hours."""
        if not self.temp_dir.exists():
            return 0, 0
        
        cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
        cleaned_count = 0
        cleaned_size = 0
        
        for item in self.temp_dir.rglob('*'):
            if item.is_file():
                file_mtime = datetime.fromtimestamp(item.stat().st_mtime)
                if file_mtime < cutoff_time:
                    try:
                        file_size = item.stat().st_size
                        item.unlink()
                        cleaned_count += 1
                        cleaned_size += file_size
                    except Exception as e:
                        print(f"Error deleting {item}: {e}")
        
        return cleaned_count, cleaned_size
    
    def cleanup_intermediate_files(self):
        """Clean up intermediate STAR output files."""
        cleaned_count = 0
        cleaned_size = 0
        
        # Clean up intermediate files in outputs
        for sample_dir in self.outputs_dir.iterdir():
            if sample_dir.is_dir() and sample_dir.name != "bams":
                for file_path in sample_dir.rglob('*'):
                    if file_path.is_file():
                        # Keep only final BAM files and essential outputs
                        if (file_path.suffix == '.bam' and 'final' in file_path.name) or \
                           file_path.name.endswith('.flagstat'):
                            continue
                        
                        try:
                            file_size = file_path.stat().st_size
                            file_path.unlink()
                            cleaned_count += 1
                            cleaned_size += file_size
                        except Exception as e:
                            print(f"Error deleting {file_path}: {e}")
        
        return cleaned_count, cleaned_size
    
    def compress_logs(self, older_than_days=7):
        """Compress old log files."""
        if not self.logs_dir.exists():
            return 0
        
        cutoff_time = datetime.now() - timedelta(days=older_than_days)
        compressed_count = 0
        
        for log_file in self.logs_dir.glob('*.out'):
            if log_file.stat().st_mtime < cutoff_time.timestamp():
                try:
                    subprocess.run(['gzip', str(log_file)], check=True)
                    compressed_count += 1
                except subprocess.CalledProcessError as e:
                    print(f"Error compressing {log_file}: {e}")
        
        return compressed_count
    
    def generate_report(self):
        """Generate storage report."""
        print("=" * 60)
        print("🧹 STORAGE CLEANUP REPORT")
        print("=" * 60)
        print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Overall disk usage
        print("💾 OVERALL DISK USAGE:")
        print("-" * 20)
        disk_usage = self.get_disk_usage()
        if disk_usage:
            print(f"Filesystem: {disk_usage['filesystem']}")
            print(f"Size: {disk_usage['size']}")
            print(f"Used: {disk_usage['used']}")
            print(f"Available: {disk_usage['available']}")
            print(f"Use: {disk_usage['use_percent']}")
        print()
        
        # Directory sizes
        print("📁 DIRECTORY SIZES:")
        print("-" * 15)
        directories = [
            ("BAM files", self.workflow_dir / "bams"),
            ("Outputs", self.outputs_dir),
            ("Logs", self.logs_dir),
            ("Temp files", self.temp_dir)
        ]
        
        for name, path in directories:
            if path.exists():
                size = self.get_directory_size(path)
                print(f"{name:15}: {size}")
            else:
                print(f"{name:15}: Not found")
        print()
        
        # File counts
        print("📊 FILE COUNTS:")
        print("-" * 12)
        bam_count = len(list((self.workflow_dir / "bams").rglob('*.bam'))) if (self.workflow_dir / "bams").exists() else 0
        log_count = len(list(self.logs_dir.glob('*.out'))) if self.logs_dir.exists() else 0
        temp_count = len(list(self.temp_dir.rglob('*'))) if self.temp_dir.exists() else 0
        
        print(f"BAM files: {bam_count}")
        print(f"Log files: {log_count}")
        print(f"Temp files: {temp_count}")
        print()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'disk_usage': disk_usage,
            'bam_count': bam_count,
            'log_count': log_count,
            'temp_count': temp_count
        }
    
    def auto_cleanup(self, dry_run=False):
        """Perform automatic cleanup operations."""
        print("=" * 60)
        print("🧹 AUTOMATIC CLEANUP")
        print("=" * 60)
        print(f"Dry run: {dry_run}")
        print()
        
        total_cleaned = 0
        total_size = 0
        
        # Clean temp files
        print("🧹 Cleaning temporary files...")
        if not dry_run:
            cleaned_count, cleaned_size = self.cleanup_temp_files()
            total_cleaned += cleaned_count
            total_size += cleaned_size
            print(f"Cleaned {cleaned_count} temp files ({cleaned_size/1024/1024:.1f} MB)")
        else:
            print("Would clean temp files (dry run)")
        
        # Clean intermediate files
        print("🧹 Cleaning intermediate files...")
        if not dry_run:
            cleaned_count, cleaned_size = self.cleanup_intermediate_files()
            total_cleaned += cleaned_count
            total_size += cleaned_size
            print(f"Cleaned {cleaned_count} intermediate files ({cleaned_size/1024/1024:.1f} MB)")
        else:
            print("Would clean intermediate files (dry run)")
        
        # Compress old logs
        print("📦 Compressing old logs...")
        if not dry_run:
            compressed_count = self.compress_logs()
            print(f"Compressed {compressed_count} log files")
        else:
            print("Would compress old logs (dry run)")
        
        print()
        print(f"Total cleanup: {total_cleaned} files, {total_size/1024/1024:.1f} MB")

def main():
    parser = argparse.ArgumentParser(description="Cleanup STAR alignment workflow")
    parser.add_argument("--workflow-dir", default="/data/salomonis-archive/czb-tabula-sapiens/star_workflow",
                       help="Workflow directory")
    parser.add_argument("--report", action="store_true",
                       help="Generate storage report")
    parser.add_argument("--cleanup", action="store_true",
                       help="Perform cleanup")
    parser.add_argument("--dry-run", action="store_true",
                       help="Dry run mode")
    
    args = parser.parse_args()
    
    manager = CleanupManager(args.workflow_dir)
    
    if args.report:
        manager.generate_report()
    elif args.cleanup:
        manager.auto_cleanup(args.dry_run)
    else:
        print("Use --report or --cleanup")
        parser.print_help()

if __name__ == "__main__":
    main()