#!/usr/bin/env python3
"""
Intelligent Resource Manager for STAR Alignment Workflow
Provides dynamic resource allocation and storage management
"""

import os
import shutil
import subprocess
import psutil
from pathlib import Path
import json
from datetime import datetime

class ResourceManager:
    def __init__(self, workflow_dir):
        self.workflow_dir = Path(workflow_dir)
        self.config_file = self.workflow_dir / "data" / "resource_config.json"
        self.load_config()
        
    def load_config(self):
        """Load or create default resource configuration."""
        default_config = {
            "max_concurrent_jobs": 10,
            "min_chunk_size": 100,
            "max_chunk_size": 2000,
            "default_chunk_size": 1000,
            "cpu_per_job": 8,
            "memory_per_job": "128GB",
            "walltime": "72:00",
            "queue": "normal",
            "storage_threshold": 0.9,  # 90% storage usage threshold
            "scratch_space_path": "/scratch/pavb5f",
            "auto_cleanup": True,
            "last_updated": datetime.now().isoformat()
        }
        
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    self.config = json.load(f)
                # Update with any new default values
                for key, value in default_config.items():
                    if key not in self.config:
                        self.config[key] = value
            except (json.JSONDecodeError, IOError):
                self.config = default_config
        else:
            self.config = default_config
            
        self.save_config()
        
    def save_config(self):
        """Save current configuration."""
        self.config["last_updated"] = datetime.now().isoformat()
        self.config_file.parent.mkdir(exist_ok=True)
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)
            
    def get_system_resources(self):
        """Get current system resource status."""
        try:
            # CPU information
            cpu_count = psutil.cpu_count()
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # Memory information
            memory = psutil.virtual_memory()
            
            # Disk information
            disk = shutil.disk_usage(self.workflow_dir)
            
            # LSF queue information
            lsf_info = self.get_lsf_queue_info()
            
            return {
                "cpu": {
                    "count": cpu_count,
                    "usage_percent": cpu_percent,
                    "available": cpu_count - int(cpu_count * cpu_percent / 100)
                },
                "memory": {
                    "total_gb": memory.total / (1024**3),
                    "available_gb": memory.available / (1024**3),
                    "usage_percent": memory.percent
                },
                "storage": {
                    "total_gb": disk.total / (1024**3),
                    "used_gb": disk.used / (1024**3),
                    "free_gb": disk.free / (1024**3),
                    "usage_percent": (disk.used / disk.total) * 100
                },
                "lsf": lsf_info
            }
        except Exception as e:
            print(f"⚠️  Warning: Could not get system resources: {e}")
            return None
            
    def get_lsf_queue_info(self):
        """Get LSF queue information."""
        try:
            # Get queue status
            result = subprocess.run(['bqueues', '-w'], capture_output=True, text=True)
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                queue_info = {}
                for line in lines[1:]:  # Skip header
                    parts = line.split()
                    if len(parts) >= 6:
                        queue_name = parts[0]
                        queue_info[queue_name] = {
                            "pending": int(parts[1]) if parts[1].isdigit() else 0,
                            "running": int(parts[2]) if parts[2].isdigit() else 0,
                            "suspended": int(parts[3]) if parts[3].isdigit() else 0
                        }
                return queue_info
        except Exception as e:
            print(f"⚠️  Warning: Could not get LSF queue info: {e}")
            
        return {}
        
    def calculate_optimal_chunk_size(self, total_samples, storage_requirement_gb):
        """Calculate optimal chunk size based on available resources."""
        resources = self.get_system_resources()
        if not resources:
            return self.config["default_chunk_size"]
            
        # Storage-based calculation
        available_storage_gb = resources["storage"]["free_gb"]
        storage_per_sample_gb = storage_requirement_gb / total_samples
        
        # Calculate max samples that fit in available storage
        max_samples_by_storage = int(available_storage_gb * 0.8 / storage_per_sample_gb)  # 80% safety margin
        
        # LSF queue-based calculation
        lsf_info = resources["lsf"]
        queue_capacity = 0
        for queue_name, queue_data in lsf_info.items():
            if queue_name == self.config["queue"]:
                queue_capacity = max(0, 100 - queue_data["running"])  # Assume 100 max jobs
                break
                
        # CPU-based calculation
        available_cpus = resources["cpu"]["available"]
        max_samples_by_cpu = available_cpus * 10  # Assume 10 samples per CPU
        
        # Calculate optimal chunk size
        optimal_samples = min(
            max_samples_by_storage,
            max_samples_by_cpu,
            queue_capacity * self.config["default_chunk_size"],
            self.config["max_chunk_size"]
        )
        
        # Ensure minimum chunk size
        optimal_samples = max(optimal_samples, self.config["min_chunk_size"])
        
        return optimal_samples
        
    def check_storage_threshold(self):
        """Check if storage usage exceeds threshold."""
        resources = self.get_system_resources()
        if not resources:
            return False
            
        usage_percent = resources["storage"]["usage_percent"]
        threshold = self.config["storage_threshold"] * 100
        
        return usage_percent > threshold
        
    def get_recommended_action(self, total_samples, storage_requirement_gb):
        """Get recommended action based on current resources."""
        resources = self.get_system_resources()
        if not resources:
            return "unknown", "Cannot determine system resources"
            
        # Check storage threshold
        if self.check_storage_threshold():
            return "storage_full", f"Storage usage: {resources['storage']['usage_percent']:.1f}% (threshold: {self.config['storage_threshold']*100:.1f}%)"
            
        # Calculate optimal chunk size
        optimal_chunk_size = self.calculate_optimal_chunk_size(total_samples, storage_requirement_gb)
        
        # Check if we can process all samples
        total_chunks = (total_samples + optimal_chunk_size - 1) // optimal_chunk_size
        estimated_storage_needed = total_chunks * optimal_chunk_size * (storage_requirement_gb / total_samples)
        
        if estimated_storage_needed > resources["storage"]["free_gb"] * 0.8:
            return "insufficient_storage", f"Need {estimated_storage_needed:.1f}GB, have {resources['storage']['free_gb']:.1f}GB"
            
        # Check LSF queue capacity
        lsf_info = resources["lsf"]
        queue_running = 0
        for queue_name, queue_data in lsf_info.items():
            if queue_name == self.config["queue"]:
                queue_running = queue_data["running"]
                break
                
        if queue_running > 80:  # Assume 100 max jobs
            return "queue_busy", f"LSF queue {self.config['queue']} has {queue_running} running jobs"
            
        return "ready", f"Optimal chunk size: {optimal_chunk_size} samples ({total_chunks} chunks)"
        
    def generate_resource_report(self):
        """Generate comprehensive resource report."""
        resources = self.get_system_resources()
        if not resources:
            return "❌ Could not determine system resources"
            
        report = []
        report.append("📊 SYSTEM RESOURCE REPORT")
        report.append("=" * 50)
        report.append(f"⏰ Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # CPU Information
        report.append("🖥️  CPU RESOURCES:")
        report.append(f"  Total CPUs: {resources['cpu']['count']}")
        report.append(f"  Usage: {resources['cpu']['usage_percent']:.1f}%")
        report.append(f"  Available: {resources['cpu']['available']}")
        report.append("")
        
        # Memory Information
        report.append("🧠 MEMORY RESOURCES:")
        report.append(f"  Total: {resources['memory']['total_gb']:.1f} GB")
        report.append(f"  Available: {resources['memory']['available_gb']:.1f} GB")
        report.append(f"  Usage: {resources['memory']['usage_percent']:.1f}%")
        report.append("")
        
        # Storage Information
        report.append("💾 STORAGE RESOURCES:")
        report.append(f"  Total: {resources['storage']['total_gb']:.1f} GB")
        report.append(f"  Used: {resources['storage']['used_gb']:.1f} GB")
        report.append(f"  Free: {resources['storage']['free_gb']:.1f} GB")
        report.append(f"  Usage: {resources['storage']['usage_percent']:.1f}%")
        report.append("")
        
        # LSF Queue Information
        report.append("📋 LSF QUEUE STATUS:")
        for queue_name, queue_data in resources['lsf'].items():
            report.append(f"  {queue_name}:")
            report.append(f"    Pending: {queue_data['pending']}")
            report.append(f"    Running: {queue_data['running']}")
            report.append(f"    Suspended: {queue_data['suspended']}")
        report.append("")
        
        # Configuration
        report.append("⚙️  CURRENT CONFIGURATION:")
        report.append(f"  Max Concurrent Jobs: {self.config['max_concurrent_jobs']}")
        report.append(f"  Chunk Size Range: {self.config['min_chunk_size']}-{self.config['max_chunk_size']}")
        report.append(f"  Default Chunk Size: {self.config['default_chunk_size']}")
        report.append(f"  CPU per Job: {self.config['cpu_per_job']}")
        report.append(f"  Memory per Job: {self.config['memory_per_job']}")
        report.append(f"  Walltime: {self.config['walltime']}")
        report.append(f"  Queue: {self.config['queue']}")
        report.append(f"  Storage Threshold: {self.config['storage_threshold']*100:.1f}%")
        
        return "\n".join(report)
        
    def update_config(self, **kwargs):
        """Update configuration with new values."""
        for key, value in kwargs.items():
            if key in self.config:
                self.config[key] = value
        self.save_config()
        
    def get_job_resources(self):
        """Get resource allocation for a single job."""
        return {
            "cpus": self.config["cpu_per_job"],
            "memory": self.config["memory_per_job"],
            "walltime": self.config["walltime"],
            "queue": self.config["queue"]
        }

def main():
    """Test the resource manager."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Resource Manager for STAR Alignment')
    parser.add_argument('--workflow-dir', default='.', help='Workflow directory')
    parser.add_argument('--report', action='store_true', help='Generate resource report')
    parser.add_argument('--check', action='store_true', help='Check resource status')
    parser.add_argument('--config', action='store_true', help='Show current configuration')
    
    args = parser.parse_args()
    
    manager = ResourceManager(args.workflow_dir)
    
    if args.report:
        print(manager.generate_resource_report())
    elif args.check:
        resources = manager.get_system_resources()
        if resources:
            print("✅ System resources accessible")
            print(f"Storage usage: {resources['storage']['usage_percent']:.1f}%")
            print(f"CPU usage: {resources['cpu']['usage_percent']:.1f}%")
        else:
            print("❌ Could not access system resources")
    elif args.config:
        print("⚙️  Current Configuration:")
        for key, value in manager.config.items():
            print(f"  {key}: {value}")
    else:
        print("Use --help to see available options")

if __name__ == '__main__':
    main()