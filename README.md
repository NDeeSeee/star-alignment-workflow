# STAR Alignment Workflow - SmartSeq2 Samples Only

## 🧬 **STAR Alignment Infrastructure for Tabula Sapiens SmartSeq2 Data**

A clean, production-ready STAR alignment workflow with intelligent resource management for processing **SmartSeq2 FASTQ files only** (excluding 10X samples).

## 🚀 **Quick Start**

```bash
cd /data/salomonis-archive/czb-tabula-sapiens/star_workflow

# 1. Check system resources (CRITICAL - 99.5% storage used!)
python3 run.py resources

# 2. Initialize workflow with intelligent chunk sizing
python3 run.py init

# 3. Submit jobs (only if resources available)
python3 run.py submit

# 4. Monitor progress
python3 run.py monitor
```

## 📁 **Directory Structure**

```
star_workflow/
├── run.py                   # Single controller with intelligent resource management
├── README.md               # This file
├── scripts/                # Essential workflow scripts
│   ├── create_manifest.py  # SmartSeq2 manifest creator
│   ├── star_align.sh       # STAR alignment script
│   ├── monitor.py          # Progress monitoring
│   ├── cleanup.py          # Storage management
│   └── resource_manager.py # Intelligent resource management
├── data/                   # Sample manifest & configuration
├── logs/                   # Job logs
├── outputs/                # All outputs (consolidated)
│   ├── bams/               # Final BAM files
│   └── temp/               # Temporary files
└── temp/                   # System temporary files
```

## 🛠️ **Available Commands**

| Command | Purpose | Usage |
|---------|---------|-------|
| `init [SIZE]` | Initialize workflow with intelligent chunk sizing | `python3 run.py init 1000` |
| `submit [ID]` | Submit specific chunk or all chunks | `python3 run.py submit` |
| `monitor` | Real-time monitoring | `python3 run.py monitor` |
| `status` | Show current status | `python3 run.py status` |
| `resources` | Show detailed resource information | `python3 run.py resources` |
| `cleanup` | Clean up storage | `python3 run.py cleanup` |
| `help` | Show help | `python3 run.py help` |

## 📊 **Workflow Overview**

### **Data Processing Pipeline**
1. **Sample Discovery**: Scan for SmartSeq2 R1/R2 FASTQ pairs only
2. **Manifest Creation**: Generate validated SmartSeq2 sample list
3. **Job Submission**: Submit LSF job array
4. **STAR Alignment**: 2-pass alignment with GRCh38
5. **BAM Generation**: Sorted BAM files with statistics
6. **Monitoring**: Real-time progress tracking

### **Sample Distribution**
- **Total SmartSeq2 Samples**: 35,674 (after filtering empty wells)
- **Pilots with SmartSeq2**: Pilot1, Pilot2, Pilot3, Pilot6, Pilot7
- **Tissue Types**: 30+ different tissues and cell types
- **Excluded**: All 10X samples (77 samples excluded)
- **Total FASTQ Data**: 5.29 TB
- **Average Sample Size**: 0.15 GB per sample

### **Resource Configuration**
- **CPUs per job**: 8
- **Memory per job**: 128GB
- **Walltime**: 72 hours
- **Queue**: normal
- **Reference**: GRCh38 with splice junctions

## 📈 **Expected Performance**

- **Total Samples**: 35,674 SmartSeq2 samples
- **Processing Rate**: ~7,000-12,000 samples/day
- **Success Rate**: >95%
- **Timeline**: 3-5 days for complete processing
- **Storage**: ~13-16TB for final BAM files (2.5-3x FASTQ size)

## ⚠️ **Critical Requirements**

### **Storage Management**
- **Current**: 100% disk usage (564T/567T used)
- **Available**: Only 3.3T remaining
- **Need**: ~13-16TB for BAM files
- **Action**: Request additional storage or use scratch space

### **System Requirements**
- **LSF Scheduler**: Available
- **STAR Module**: 2.7.10b
- **Reference Genome**: GRCh38 (available)
- **Python**: 3.x with standard libraries

## 🔧 **Troubleshooting**

### **Common Issues**

1. **Storage Full**
   ```bash
   python3 run.py cleanup
   ```

2. **Jobs Failed**
   ```bash
   # Check logs
   ls -lt logs/
   
   # Check LSF status
   bjobs -J star_align
   ```

3. **Slow Progress**
   ```bash
   python3 run.py status
   ```

### **Monitoring Commands**

```bash
# Check LSF jobs
bjobs -J star_align

# Check logs
tail -f logs/star_align_*.out

# Check storage
python3 scripts/cleanup.py --report
```

## 📋 **Quality Control**

### **File Validation**
- Minimum file size: 100KB (filters empty wells)
- R1/R2 pair validation
- File integrity checks
- **SmartSeq2 samples only** (10X samples excluded)

### **Alignment Quality**
- 2-pass STAR alignment
- Splice junction detection
- Alignment statistics generation
- BAM file validation

## 🎯 **Production Deployment**

### **Pre-Production Checklist**
- [ ] Run `python3 run.py init` to create SmartSeq2 manifest
- [ ] Check storage with `python3 run.py cleanup --report`
- [ ] Clean storage if needed with `python3 run.py cleanup`
- [ ] Verify system resources

### **Production Commands**
```bash
# Start processing
python3 run.py submit

# Monitor progress
python3 run.py monitor

# Check status
python3 run.py status
```

## 📞 **Support**

### **Log Files**
- **Location**: `logs/`
- **Format**: `star_align_{job_id}_{array_index}.out/err`
- **Content**: Detailed alignment logs and errors

### **Output Files**
- **BAM Files**: `outputs/bams/{sample_id}/{sample_id}.bam`
- **Statistics**: `outputs/bams/{sample_id}/{sample_id}.flagstat`
- **Manifest**: `data/sample_manifest.csv`

### **Monitoring**
- **Real-time**: `python3 run.py monitor`
- **Status**: `python3 run.py status`
- **Storage**: `python3 scripts/cleanup.py --report`

## 📊 **Data Summary**

### **SmartSeq2 Sample Distribution**
- **Pilot1**: 8,089 samples
- **Pilot2**: 22,764 samples  
- **Pilot3**: 368 samples
- **Pilot6**: 370 samples
- **Pilot7**: 4,814 samples
- **Total**: 35,674 samples

### **Tissue Types Included**
- Blood, Bone Marrow, Brain, Heart, Kidney, Liver, Lung
- Muscle, Skin, Spleen, Thymus, Tongue, Trachea
- And 20+ other tissues and cell types

### **Excluded Data**
- **10X samples**: 77 samples (excluded from processing)
- **Empty wells**: 731 samples (filtered out)
- **Small files**: <100KB (filtered out)

## 🔮 **Advanced Features**

The workflow includes sophisticated HPC resource management and predictive capabilities:

### **Storage Failure Prediction**
```bash
python3 run.py storage
```
- **Real-time storage monitoring** with failure prediction
- **Risk assessment** (LOW/MEDIUM/HIGH/CRITICAL)
- **Preventive measures** and recommendations
- **Time-to-failure estimation**

### **Resource Exhaustion Prediction**
```bash
python3 run.py exhaustion
```
- **Multi-resource monitoring** (CPU, Memory, Storage, Queues)
- **Exhaustion probability** calculation
- **Queue utilization** analysis
- **Optimization recommendations**

### **Network Topology Awareness**
```bash
python3 run.py topology
```
- **Network interface discovery** and status
- **Performance metrics** (bytes sent/received, error rates)
- **Routing information** and connectivity
- **Storage connectivity** analysis

### **HPC System Integration**
```bash
python3 run.py hpc
```
- **Scheduler detection** (LSF, SLURM, PBS)
- **Queue information** and job status
- **Management system integration** (Ganglia, Nagios)
- **System command availability**

### **Intelligent Resource Management**
- **Dynamic chunk sizing** based on system state
- **Queue-aware job submission** with optimal resource allocation
- **Cost optimization** across different queues
- **Predictive job scheduling** with success probability estimation

---

**Last Updated**: $(date)  
**Status**: Production Ready ✅  
**Focus**: SmartSeq2 Samples Only 🧬  
**Infrastructure**: Streamlined & Optimized 🚀  
**Advanced Features**: HPC-Ready with Predictive Analytics 🔮