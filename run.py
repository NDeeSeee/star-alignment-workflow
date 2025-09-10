#!/usr/bin/env python3
"""
STAR Alignment Workflow Controller
Single, clean interface for STAR alignment processing with intelligent resource management
"""

import os
import sys
import subprocess
import argparse
import csv
import json
from pathlib import Path
from datetime import datetime

# Import resource manager
sys.path.append(str(Path(__file__).parent / "scripts"))
from resource_manager import AdvancedResourceManager as ResourceManager

# Colors for output
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m' # No Color

def print_status(color, message):
    """Print colored status message."""
    print(f"{color}{message}{NC}")

def check_prerequisites():
    """Check if prerequisites are met."""
    print_status(BLUE, "🔍 Checking prerequisites...")
    
    # Check if we're in the right directory
    workflow_dir = Path.cwd()
    if not (workflow_dir / "scripts").exists():
        print_status(RED, "❌ Error: Not in workflow directory")
        print_status(YELLOW, "💡 Run from: /data/salomonis-archive/czb-tabula-sapiens/star_workflow")
        return False
    
    # Check if manifest exists
    manifest_file = workflow_dir / "data" / "sample_manifest.csv"
    if not manifest_file.exists():
        print_status(YELLOW, "⚠️  Manifest not found. Creating...")
        return create_manifest()
    
    print_status(GREEN, "✅ Prerequisites check passed")
    return True

def create_manifest():
    """Create sample manifest."""
    print_status(BLUE, "📋 Creating sample manifest...")
    
    workflow_dir = Path.cwd()
    script_path = workflow_dir / "scripts" / "create_manifest.py"
    
    if not script_path.exists():
        print_status(RED, "❌ Error: create_manifest.py not found")
        return False
    
    try:
        result = subprocess.run([sys.executable, str(script_path)], 
                              capture_output=True, text=True, check=True)
        print(result.stdout)
        print_status(GREEN, "✅ Manifest created successfully")
        return True
    except subprocess.CalledProcessError as e:
        print_status(RED, f"❌ Error creating manifest: {e.stderr}")
        return False

def create_chunks(chunk_size=None):
    """Create chunk manifests with intelligent sizing."""
    print_status(BLUE, "📦 Creating chunk manifests...")
    
    workflow_dir = Path.cwd()
    manifest_file = workflow_dir / "data" / "sample_manifest.csv"
    
    if not manifest_file.exists():
        print_status(RED, "❌ Error: Manifest file not found")
        return False
    
    # Count samples
    with open(manifest_file, 'r') as f:
        reader = csv.DictReader(f)
        samples = list(reader)
    
    total_samples = len(samples)
    print_status(BLUE, f"📊 Total samples: {total_samples}")
    
    # Calculate storage requirement (5.29 TB total FASTQ data)
    storage_per_sample_gb = (5.29 * 1024) / total_samples  # Convert TB to GB
    total_storage_gb = storage_per_sample_gb * total_samples * 3  # 3x for BAM files
    
    # Initialize resource manager
    resource_manager = ResourceManager(workflow_dir)
    
    # Get optimal chunk size
    if chunk_size is None:
        optimal_chunk_size = resource_manager.calculate_optimal_chunk_size(total_samples, total_storage_gb)
        print_status(BLUE, f"🧠 Intelligent chunk sizing: {optimal_chunk_size} samples per chunk")
    else:
        optimal_chunk_size = chunk_size
        print_status(BLUE, f"📏 User-specified chunk size: {optimal_chunk_size} samples per chunk")
    
    # Create chunks directory
    chunks_dir = workflow_dir / "chunks"
    chunks_dir.mkdir(exist_ok=True)
    
    # Create chunk manifests
    total_chunks = (total_samples + optimal_chunk_size - 1) // optimal_chunk_size
    chunk_status = []
    
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * optimal_chunk_size
        end_idx = min(start_idx + optimal_chunk_size, total_samples)
        chunk_samples = samples[start_idx:end_idx]
        
        chunk_id = f"chunk_{chunk_idx + 1:03d}"
        chunk_file = chunks_dir / f"{chunk_id}_manifest.csv"
        
        # Write chunk manifest
        with open(chunk_file, 'w', newline='') as f:
            fieldnames = ['sample_id', 'r1_path', 'r2_path', 'r1_size', 'r2_size', 'status']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk_samples)
        
        # Track chunk status
        chunk_status.append({
            'chunk_id': chunk_id,
            'chunk_file': str(chunk_file),
            'start_idx': start_idx + 1,
            'end_idx': end_idx,
            'sample_count': len(chunk_samples),
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
            'started_at': None,
            'completed_at': None,
            'failed_samples': 0,
            'completed_samples': 0
        })
        
        print_status(GREEN, f"✅ Created {chunk_id}: samples {start_idx + 1}-{end_idx} ({len(chunk_samples)} samples)")
    
    # Save chunk status
    chunk_status_file = workflow_dir / "data" / "chunk_status.csv"
    with open(chunk_status_file, 'w', newline='') as f:
        fieldnames = ['chunk_id', 'chunk_file', 'start_idx', 'end_idx', 'sample_count', 
                     'status', 'created_at', 'started_at', 'completed_at', 
                     'failed_samples', 'completed_samples']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunk_status)
    
    print_status(GREEN, f"🎉 Created {total_chunks} chunks successfully!")
    return True

def submit_jobs(chunk_id=None):
    """Submit STAR alignment jobs with intelligent resource management."""
    print_status(BLUE, "🚀 Submitting STAR alignment jobs...")
    
    workflow_dir = Path.cwd()
    resource_manager = ResourceManager(workflow_dir)
    
    # Check resource status
    print_status(BLUE, "🔍 Checking system resources...")
    resources = resource_manager.get_system_resources()
    if resources:
        print_status(BLUE, f"💾 Storage: {resources['storage']['usage_percent']:.1f}% used")
        print_status(BLUE, f"🖥️  CPU: {resources['cpu']['usage_percent']:.1f}% used")
    
    # Check if chunks exist
    chunk_status_file = workflow_dir / "data" / "chunk_status.csv"
    if not chunk_status_file.exists():
        print_status(YELLOW, "⚠️  No chunks found. Creating with intelligent sizing...")
        if not create_chunks():
            return False
    
    # Read chunk status
    with open(chunk_status_file, 'r') as f:
        reader = csv.DictReader(f)
        chunks = list(reader)
    
    if chunk_id:
        # Submit specific chunk
        chunk = next((c for c in chunks if c['chunk_id'] == chunk_id), None)
        if not chunk:
            print_status(RED, f"❌ Error: Chunk {chunk_id} not found")
            return False
        
        if chunk['status'] != 'pending':
            print_status(YELLOW, f"⚠️  Chunk {chunk_id} is already {chunk['status']}")
            return False
        
        chunks_to_submit = [chunk]
    else:
        # Submit all pending chunks
        chunks_to_submit = [c for c in chunks if c['status'] == 'pending']
    
    if not chunks_to_submit:
        print_status(GREEN, "✅ No pending chunks to submit!")
        return True
    
    print_status(BLUE, f"📦 Submitting {len(chunks_to_submit)} chunks...")
    
    # Get job resources
    job_resources = resource_manager.get_job_resources()
    
    success_count = 0
    for chunk in chunks_to_submit:
        chunk_file = Path(chunk['chunk_file'])
        if not chunk_file.exists():
            print_status(RED, f"❌ Error: Chunk file not found: {chunk_file}")
            continue
        
        # Create job submission script
        job_script = create_job_submission_script(chunk, job_resources)
        
        try:
            result = subprocess.run(['bash', str(job_script)], 
                                  capture_output=True, text=True, check=True)
            print_status(GREEN, f"✅ Chunk {chunk['chunk_id']} submitted successfully!")
            success_count += 1
            
            # Update chunk status
            update_chunk_status(chunk['chunk_id'], 'running', started_at=datetime.now().isoformat())
            
        except subprocess.CalledProcessError as e:
            print_status(RED, f"❌ Error submitting chunk {chunk['chunk_id']}: {e.stderr}")
    
    print_status(GREEN, f"✅ Successfully submitted {success_count}/{len(chunks_to_submit)} chunks")
    return success_count == len(chunks_to_submit)

def create_job_submission_script(chunk, job_resources):
    """Create job submission script for a chunk."""
    workflow_dir = Path.cwd()
    chunks_dir = workflow_dir / "chunks"
    
    script_content = f"""#!/bin/bash
# Chunked STAR Alignment Job Submission
# Chunk: {chunk['chunk_id']}

set -euo pipefail

# Configuration
CHUNK_ID="{chunk['chunk_id']}"
CHUNK_FILE="{chunk['chunk_file']}"
WORKFLOW_DIR="{workflow_dir}"
SCRIPT_DIR="{workflow_dir}/scripts"
ALIGN_SCRIPT="${{SCRIPT_DIR}}/star_align.sh"

# Colors
RED='\\033[0;31m'
GREEN='\\033[0;32m'
YELLOW='\\033[1;33m'
BLUE='\\033[0;34m'
NC='\\033[0m'

print_status() {{
    local color=$1
    local message=$2
    echo -e "${{color}}${{message}}${{NC}}"
}}

# Validate files
if [[ ! -f "${{CHUNK_FILE}}" ]]; then
    print_status $RED "❌ ERROR: Chunk file not found: ${{CHUNK_FILE}}"
    exit 1
fi

if [[ ! -f "${{ALIGN_SCRIPT}}" ]]; then
    print_status $RED "❌ ERROR: Alignment script not found: ${{ALIGN_SCRIPT}}"
    exit 1
fi

# Count samples in chunk
TOTAL_SAMPLES=$(tail -n +2 "${{CHUNK_FILE}}" | wc -l)
print_status $BLUE "📊 Chunk ${{CHUNK_ID}}: ${{TOTAL_SAMPLES}} samples"

# LSF Configuration
JOB_NAME="star_chunk_{chunk['chunk_id']}"
CPUS_PER_JOB={job_resources['cpus']}
MEMORY_PER_JOB="{job_resources['memory']}"
WALLTIME="{job_resources['walltime']}"
QUEUE="{job_resources['queue']}"

print_status $BLUE "📋 Job Configuration:"
echo "Job Name: ${{JOB_NAME}}"
echo "Array Size: 1-${{TOTAL_SAMPLES}}"
echo "CPUs per Job: ${{CPUS_PER_JOB}}"
echo "Memory per Job: ${{MEMORY_PER_JOB}}"
echo "Walltime: ${{WALLTIME}}"
echo "Queue: ${{QUEUE}}"

# Submit job array
print_status $GREEN "🚀 Submitting chunk ${{CHUNK_ID}}..."

bsub -J "${{JOB_NAME}}[1-${{TOTAL_SAMPLES}}]" \\
     -n ${{CPUS_PER_JOB}} \\
     -M ${{MEMORY_PER_JOB}} \\
     -R "span[hosts=1]" \\
     -W ${{WALLTIME}} \\
     -q ${{QUEUE}} \\
     -o "${{WORKFLOW_DIR}}/logs/${{JOB_NAME}}_%I.out" \\
     -e "${{WORKFLOW_DIR}}/logs/${{JOB_NAME}}_%I.err" \\
     "${{ALIGN_SCRIPT}}" "${{CHUNK_FILE}}"

if [[ $? -eq 0 ]]; then
    print_status $GREEN "✅ Chunk ${{CHUNK_ID}} submitted successfully!"
    echo ""
    print_status $BLUE "📊 Monitor jobs with:"
    echo "  bjobs -J ${{JOB_NAME}}"
    echo "  bjobs -u \\$(whoami)"
    echo ""
    print_status $BLUE "📁 Check logs in:"
    echo "  ${{WORKFLOW_DIR}}/logs/"
else
    print_status $RED "❌ Chunk submission failed!"
    exit 1
fi
"""
    
    script_file = chunks_dir / f"submit_{chunk['chunk_id']}.sh"
    with open(script_file, 'w') as f:
        f.write(script_content)
    os.chmod(script_file, 0o755)
    
    return script_file

def update_chunk_status(chunk_id, status, **kwargs):
    """Update chunk status."""
    workflow_dir = Path.cwd()
    chunk_status_file = workflow_dir / "data" / "chunk_status.csv"
    
    if not chunk_status_file.exists():
        return
    
    with open(chunk_status_file, 'r') as f:
        reader = csv.DictReader(f)
        chunks = list(reader)
    
    for chunk in chunks:
        if chunk['chunk_id'] == chunk_id:
            chunk['status'] = status
            for key, value in kwargs.items():
                chunk[key] = value
            break
    
    with open(chunk_status_file, 'w', newline='') as f:
        fieldnames = ['chunk_id', 'chunk_file', 'start_idx', 'end_idx', 'sample_count', 
                     'status', 'created_at', 'started_at', 'completed_at', 
                     'failed_samples', 'completed_samples']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(chunks)

def monitor_progress():
    """Monitor job progress."""
    print_status(BLUE, "📊 Starting progress monitoring...")
    
    workflow_dir = Path.cwd()
    script_path = workflow_dir / "scripts" / "monitor.py"
    
    if not script_path.exists():
        print_status(RED, "❌ Error: monitor.py not found")
        return False
    
    try:
        subprocess.run([sys.executable, str(script_path), "--monitor"], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print_status(RED, f"❌ Error monitoring: {e}")
        return False

def show_status():
    """Show current workflow status."""
    print_status(BLUE, "📈 Current workflow status...")
    
    workflow_dir = Path.cwd()
    
    # Show chunk status if available
    chunk_status_file = workflow_dir / "data" / "chunk_status.csv"
    if chunk_status_file.exists():
        with open(chunk_status_file, 'r') as f:
            reader = csv.DictReader(f)
            chunks = list(reader)
        
        total_chunks = len(chunks)
        pending = sum(1 for c in chunks if c['status'] == 'pending')
        running = sum(1 for c in chunks if c['status'] == 'running')
        completed = sum(1 for c in chunks if c['status'] == 'completed')
        failed = sum(1 for c in chunks if c['status'] == 'failed')
        
        print_status(BLUE, "📦 Chunk Status:")
        print(f"  Total Chunks: {total_chunks}")
        print(f"  Pending: {pending}")
        print(f"  Running: {running}")
        print(f"  Completed: {completed}")
        print(f"  Failed: {failed}")
        print()
    
    # Show system resources
    resource_manager = ResourceManager(workflow_dir)
    print_status(BLUE, "🔍 System Resources:")
    resources = resource_manager.get_system_resources()
    if resources:
        print(f"  Storage: {resources['storage']['usage_percent']:.1f}% used")
        print(f"  CPU: {resources['cpu']['usage_percent']:.1f}% used")
        print(f"  Memory: {resources['memory']['usage_percent']:.1f}% used")
    
    # Show general status
    script_path = workflow_dir / "scripts" / "monitor.py"
    if script_path.exists():
        try:
            subprocess.run([sys.executable, str(script_path)], check=True)
        except subprocess.CalledProcessError:
            pass

def cleanup_storage():
    """Clean up storage."""
    print_status(BLUE, "🧹 Cleaning up storage...")
    
    workflow_dir = Path.cwd()
    script_path = workflow_dir / "scripts" / "cleanup.py"
    
    if not script_path.exists():
        print_status(RED, "❌ Error: cleanup.py not found")
        return False
    
    try:
        subprocess.run([sys.executable, str(script_path), "--cleanup"], check=True)
        print_status(GREEN, "✅ Storage cleanup completed")
        return True
    except subprocess.CalledProcessError as e:
        print_status(RED, f"❌ Error cleaning up: {e}")
        return False

def show_resources():
    """Show detailed resource information."""
    workflow_dir = Path.cwd()
    resource_manager = ResourceManager(workflow_dir)
    print(resource_manager.generate_advanced_report())

def show_help():
    """Show help message."""
    print_status(BLUE, "🧬 STAR Alignment Workflow Controller")
    print_status(BLUE, f"📁 Working directory: {Path.cwd()}")
    print()
    print("🧬 STAR Alignment Workflow Controller")
    print("=" * 50)
    print()
    print("Usage: python3 run.py [COMMAND] [OPTIONS]")
    print()
    print("Commands:")
    print("  init [SIZE]     - Initialize workflow (create manifest and chunks)")
    print("  create [SIZE]   - Create chunk manifests (intelligent sizing)")
    print("  submit [ID]     - Submit specific chunk or all chunks")
    print("  monitor         - Monitor job progress (real-time)")
    print("  status          - Show current status")
    print("  resources       - Show detailed resource information")
    print("  cleanup         - Clean up storage")
    print("  help            - Show this help message")
    print()
    print("Examples:")
    print("  python3 run.py init 1000    # Create chunks of 1000 samples")
    print("  python3 run.py submit       # Submit all pending chunks")
    print("  python3 run.py submit chunk_001  # Submit specific chunk")
    print("  python3 run.py monitor      # Watch progress")
    print("  python3 run.py status       # Check status")
    print("  python3 run.py resources    # Show system resources")
    print()
    print("Features:")
    print("  ✅ Intelligent resource management")
    print("  ✅ Dynamic chunk sizing")
    print("  ✅ Storage monitoring")
    print("  ✅ Queue management")
    print("  ✅ Automatic cleanup")

def main():
    parser = argparse.ArgumentParser(description='STAR Alignment Workflow Controller')
    parser.add_argument('command', nargs='?', help='Command to execute')
    parser.add_argument('arg', nargs='?', help='Command argument')
    
    args = parser.parse_args()
    
    if not args.command or args.command == 'help':
        show_help()
        return
    
    # Check prerequisites for most commands
    if args.command not in ['help', 'resources']:
        if not check_prerequisites():
            return
    
    # Execute commands
    if args.command == 'init':
        chunk_size = int(args.arg) if args.arg else None
        if create_manifest() and create_chunks(chunk_size):
            print_status(GREEN, "🎉 Workflow initialized successfully!")
            print_status(BLUE, "💡 Next steps:")
            print_status(BLUE, "  1. python3 run.py status")
            print_status(BLUE, "  2. python3 run.py submit")
            print_status(BLUE, "  3. python3 run.py monitor")
    
    elif args.command == 'create':
        chunk_size = int(args.arg) if args.arg else None
        create_chunks(chunk_size)
    
    elif args.command == 'submit':
        submit_jobs(args.arg)
    
    elif args.command == 'monitor':
        monitor_progress()
    
    elif args.command == 'status':
        show_status()
    
    elif args.command == 'resources':
        show_resources()
    
    elif args.command == 'cleanup':
        cleanup_storage()
    
    else:
        print_status(RED, f"❌ Unknown command: {args.command}")
        show_help()

if __name__ == '__main__':
    main()