#!/usr/bin/env python3
"""
STAR Alignment Sample Manifest Creator
Creates a clean, validated manifest for STAR alignment processing.
"""

import os
import csv
import sys
from pathlib import Path
import argparse

def create_sample_manifest(base_dir, output_file, min_size=100000):
    """Create sample manifest with validation."""
    
    print(f"🔍 Scanning {base_dir} for FASTQ pairs...")
    
    # Find all R1 files
    r1_files = list(Path(base_dir).rglob("*_R1_*.fastq.gz"))
    print(f"📊 Found {len(r1_files)} R1 files")
    
    valid_pairs = []
    skipped_count = 0
    
    for r1_file in r1_files:
        # Generate R2 path
        r2_file = r1_file.parent / r1_file.name.replace("_R1_", "_R2_")
        
        if not r2_file.exists():
            skipped_count += 1
            continue
            
        # Check file sizes
        r1_size = r1_file.stat().st_size
        r2_size = r2_file.stat().st_size
        
        if r1_size < min_size or r2_size < min_size:
            skipped_count += 1
            continue
            
        # Extract sample ID
        sample_id = r1_file.name.split("_R1_")[0]
        
        valid_pairs.append({
            'sample_id': sample_id,
            'r1_path': str(r1_file),
            'r2_path': str(r2_file),
            'r1_size': r1_size,
            'r2_size': r2_size
        })
    
    print(f"✅ Found {len(valid_pairs)} valid pairs (skipped {skipped_count} small/empty files)")
    
    # Write manifest
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['sample_id', 'r1_path', 'r2_path', 'r1_size', 'r2_size', 'status']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for idx, sample in enumerate(valid_pairs, 1):
            sample['status'] = 'pending'
            writer.writerow(sample)
    
    print(f"📄 Manifest created: {output_file}")
    print(f"📊 Total samples: {len(valid_pairs)}")
    
    return len(valid_pairs)

def main():
    parser = argparse.ArgumentParser(description="Create STAR alignment sample manifest")
    parser.add_argument("--base-dir", default="/data/salomonis-archive/czb-tabula-sapiens",
                       help="Base directory containing FASTQ files")
    parser.add_argument("--output", default="sample_manifest.csv",
                       help="Output manifest file")
    parser.add_argument("--min-size", type=int, default=100000,
                       help="Minimum file size in bytes")
    
    args = parser.parse_args()
    
    try:
        count = create_sample_manifest(args.base_dir, args.output, args.min_size)
        print(f"\n🎉 Success! Created manifest with {count} samples")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()