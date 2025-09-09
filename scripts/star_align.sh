#!/bin/bash
# STAR Alignment Script for Tabula Sapiens
# Optimized 2-pass STAR alignment with error handling

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKFLOW_DIR="$(dirname "$SCRIPT_DIR")"
BASE_DIR="/data/salomonis-archive/czb-tabula-sapiens"

# Check if chunk file is provided as argument
if [[ $# -eq 1 ]]; then
    MANIFEST_FILE="$1"
    echo "📦 Using chunk manifest: ${MANIFEST_FILE}"
else
    MANIFEST_FILE="${WORKFLOW_DIR}/data/sample_manifest.csv"
    echo "📄 Using main manifest: ${MANIFEST_FILE}"
fi

# STAR Configuration
GENOME_DIR="/data/salomonis2/Genomes/Star2pass-GRCH38/GenomeRef"
GENOME="/data/salomonis2/Genomes/Star2pass-GRCH38/GRCh38.d1.vd1.fa"
STAR_THREADS=8
STAR_MEMORY="100000000000"

# Output directories (consolidated)
OUTPUT_DIR="${WORKFLOW_DIR}/outputs"
LOG_DIR="${WORKFLOW_DIR}/logs"
BAM_DIR="${OUTPUT_DIR}/bams"
TEMP_DIR="${OUTPUT_DIR}/temp"

# Create directories
mkdir -p "${OUTPUT_DIR}" "${LOG_DIR}" "${BAM_DIR}" "${TEMP_DIR}"

# Logging function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_DIR}/star_${LSB_JOBID}_${LSB_JOBINDEX}.log"
}

# Error handler
error_handler() {
    local exit_code=$?
    local line_number=$1
    log "❌ ERROR: Script failed at line ${line_number} with exit code ${exit_code}"
    exit ${exit_code}
}

trap 'error_handler ${LINENO}' ERR

# Get job index
JOB_INDEX=${LSB_JOBINDEX:-1}
log "🚀 Starting STAR alignment job ${JOB_INDEX} (Job ID: ${LSB_JOBID})"

# Extract sample information
SAMPLE_INFO=$(awk -F',' -v idx="${JOB_INDEX}" '
    NR == idx + 1 {
        print $1 "," $2 "," $3 "," $4 "," $5
    }
' "${MANIFEST_FILE}")

if [[ -z "${SAMPLE_INFO}" ]]; then
    log "❌ ERROR: No sample found for job index ${JOB_INDEX}"
    exit 1
fi

# Parse sample information
IFS=',' read -r SAMPLE_ID R1_PATH R2_PATH R1_SIZE R2_SIZE <<< "${SAMPLE_INFO}"

log "📊 Processing sample: ${SAMPLE_ID}"
log "📁 R1: ${R1_PATH} ($(numfmt --to=iec ${R1_SIZE}))"
log "📁 R2: ${R2_PATH} ($(numfmt --to=iec ${R2_SIZE}))"

# Validate input files
if [[ ! -f "${R1_PATH}" ]] || [[ ! -f "${R2_PATH}" ]]; then
    log "❌ ERROR: Input files not found"
    exit 1
fi

# Create sample directories
SAMPLE_OUTPUT_DIR="${OUTPUT_DIR}/${SAMPLE_ID}"
SAMPLE_BAM_DIR="${BAM_DIR}/${SAMPLE_ID}"
mkdir -p "${SAMPLE_OUTPUT_DIR}" "${SAMPLE_BAM_DIR}"

# Load STAR module
log "🔧 Loading STAR module..."
module load STAR/2.7.10b

# Check STAR availability
if ! command -v STAR &> /dev/null; then
    log "❌ ERROR: STAR command not found"
    exit 1
fi

log "✅ STAR version: $(STAR --version 2>&1 | head -1)"

# STAR 2-pass alignment
log "🧬 Starting STAR 2-pass alignment..."

# Pass 1: Generate splice junctions
log "📝 Pass 1: Generating splice junctions..."
STAR --genomeDir "${GENOME_DIR}" \
     --readFilesIn "${R1_PATH}" "${R2_PATH}" \
     --runThreadN ${STAR_THREADS} \
     --outFilterMultimapScoreRange 1 \
     --outFilterMultimapNmax 20 \
     --outFilterMismatchNmax 10 \
     --alignIntronMax 500000 \
     --alignMatesGapMax 1000000 \
     --sjdbScore 2 \
     --alignSJDBoverhangMin 1 \
     --genomeLoad NoSharedMemory \
     --limitBAMsortRAM ${STAR_MEMORY} \
     --readFilesCommand gunzip -c \
     --outFileNamePrefix "${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_pass1_" \
     --outFilterMatchNminOverLread 0.33 \
     --outFilterScoreMinOverLread 0.33 \
     --sjdbOverhang 100 \
     --outSAMstrandField intronMotif \
     --outSAMtype None \
     --outSAMmode None \
     --outTmpDir "${TEMP_DIR}/${SAMPLE_ID}_pass1"

log "✅ Pass 1 completed successfully"

# Pass 2: Generate sample-specific genome
log "📝 Pass 2: Generating sample-specific genome..."
SAMPLE_GENOME_DIR="${SAMPLE_OUTPUT_DIR}/GenomeRef_${SAMPLE_ID}"
mkdir -p "${SAMPLE_GENOME_DIR}"

STAR --runMode genomeGenerate \
     --genomeDir "${SAMPLE_GENOME_DIR}" \
     --genomeFastaFiles "${GENOME}" \
     --sjdbOverhang 100 \
     --runThreadN ${STAR_THREADS} \
     --sjdbFileChrStartEnd "${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_pass1_SJ.out.tab" \
     --outFileNamePrefix "${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_pass2_" \
     --outTmpDir "${TEMP_DIR}/${SAMPLE_ID}_pass2"

log "✅ Genome generation completed successfully"

# Pass 3: Final alignment with BAM output
log "📝 Pass 3: Final alignment with BAM output..."
STAR --genomeDir "${SAMPLE_GENOME_DIR}" \
     --readFilesIn "${R1_PATH}" "${R2_PATH}" \
     --runThreadN ${STAR_THREADS} \
     --outFilterMultimapScoreRange 1 \
     --outFilterMultimapNmax 20 \
     --outFilterMismatchNmax 10 \
     --alignIntronMax 500000 \
     --alignMatesGapMax 1000000 \
     --sjdbScore 2 \
     --alignSJDBoverhangMin 1 \
     --genomeLoad NoSharedMemory \
     --limitBAMsortRAM ${STAR_MEMORY} \
     --readFilesCommand gunzip -c \
     --outFileNamePrefix "${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_final_" \
     --outFilterMatchNminOverLread 0.33 \
     --outFilterScoreMinOverLread 0.33 \
     --sjdbOverhang 100 \
     --outSAMstrandField intronMotif \
     --outSAMattributes NH HI NM MD AS XS \
     --outSAMunmapped Within \
     --outSAMtype BAM SortedByCoordinate \
     --outSAMheaderHD "@HD VN:1.4" \
     --outTmpDir "${TEMP_DIR}/${SAMPLE_ID}_final"

log "✅ Final alignment completed successfully"

# Move final BAM file
FINAL_BAM="${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_final_Aligned.sortedByCoord.out.bam"
if [[ -f "${FINAL_BAM}" ]]; then
    mv "${FINAL_BAM}" "${SAMPLE_BAM_DIR}/${SAMPLE_ID}.bam"
    BAM_SIZE=$(stat -c%s "${SAMPLE_BAM_DIR}/${SAMPLE_ID}.bam")
    log "📦 BAM file created: ${SAMPLE_BAM_DIR}/${SAMPLE_ID}.bam ($(numfmt --to=iec ${BAM_SIZE}))"
else
    log "❌ ERROR: Final BAM file not found"
    exit 1
fi

# Generate alignment statistics
log "📊 Generating alignment statistics..."
if module load samtools/1.13.0 2>/dev/null && command -v samtools &> /dev/null; then
    samtools flagstat "${SAMPLE_BAM_DIR}/${SAMPLE_ID}.bam" > "${SAMPLE_BAM_DIR}/${SAMPLE_ID}.flagstat"
    log "📈 Alignment statistics saved"
else
    echo "samtools not available - statistics skipped" > "${SAMPLE_BAM_DIR}/${SAMPLE_ID}.flagstat"
    log "⚠️  samtools not available, skipping statistics"
fi

# Cleanup intermediate files
log "🧹 Cleaning up intermediate files..."
rm -rf "${SAMPLE_GENOME_DIR}"
rm -f "${SAMPLE_OUTPUT_DIR}/${SAMPLE_ID}_pass1_SJ.out.tab"
rm -rf "${TEMP_DIR}/${SAMPLE_ID}_pass1"
rm -rf "${TEMP_DIR}/${SAMPLE_ID}_pass2"
rm -rf "${TEMP_DIR}/${SAMPLE_ID}_final"

log "✅ Cleanup completed"

# Update manifest status
if [[ -w "${MANIFEST_FILE}" ]]; then
    awk -F',' -v idx="${JOB_INDEX}" -v status="completed" '
        NR == idx + 1 { $6 = status }
        { print }
    ' "${MANIFEST_FILE}" > "${MANIFEST_FILE}.tmp" && mv "${MANIFEST_FILE}.tmp" "${MANIFEST_FILE}"
    log "📝 Manifest status updated"
fi

log "🎉 STAR alignment completed successfully for sample: ${SAMPLE_ID}"
log "⏰ Job ${JOB_INDEX} finished at $(date)"

exit 0