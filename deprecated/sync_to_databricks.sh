#!/bin/bash
set -e

# Databricks Sync Script for Synthea Output
# Uploads specific folders from output to Databricks Unity Catalog Volume
# Target: /Volumes/mi2/i2l_syntheticmguh/raw
#
# Synced folders: csv, notes, metadata, notes_llm, parquet
#
# Primary format: Parquet (recommended for analytics)
# - Parquet files are already compressed with ZSTD
# - 10x smaller and faster than CSV
# - Use spark.read.parquet() in Databricks
#
# Features:
# - Gzip compression for CSV, text, and JSONL files (Spark reads .gz natively)
# - Metadata and Parquet folders copied without compression
# - Parallel compression for notes (8 processes)
# - Size comparison reporting
# - Automatic cleanup of temporary files

# Configuration
SOURCE_DIR="../output"
TARGET_VOLUME="dbfs:/Volumes/mi2/i2l_syntheticmguh/raw"
COMPRESS=true  # Set to false to upload uncompressed
TEMP_DIR=""    # Will be set if compression is used

# ANSI color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Cleanup function for temp directory
cleanup() {
    if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
        echo -e "${BLUE}Cleaning up temporary files...${NC}"
        rm -rf "$TEMP_DIR"
        echo -e "${GREEN}✓ Cleanup complete${NC}"
    fi
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "============================================================"
echo "Databricks Sync: Synthea Output to Unity Catalog Volume"
echo "============================================================"
echo ""

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}✗ Databricks CLI not found${NC}"
    echo "  Install with: pip install databricks-cli"
    echo "  Or: brew install databricks"
    exit 1
fi

echo -e "${GREEN}✓ Databricks CLI found${NC}"
databricks --version

# Check if source directory exists
if [ ! -d "$SOURCE_DIR" ]; then
    echo -e "${RED}✗ Source directory not found: $SOURCE_DIR${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Source directory found: $SOURCE_DIR${NC}"

# Check Databricks authentication
echo ""
echo -e "${BLUE}Checking Databricks authentication...${NC}"
if ! databricks workspace list / &> /dev/null; then
    echo -e "${RED}✗ Databricks authentication failed${NC}"
    echo "  Configure with: databricks configure"
    echo "  Or set environment variables:"
    echo "    DATABRICKS_HOST"
    echo "    DATABRICKS_TOKEN"
    exit 1
fi

echo -e "${GREEN}✓ Databricks authentication successful${NC}"

# Display source directory size (only for folders we'll sync)
echo ""
echo -e "${BLUE}Analyzing source directory...${NC}"
SYNC_FOLDERS_STR="csv, notes, metadata, notes_llm, parquet"

# Calculate size only for folders we'll sync
SOURCE_BYTES=0
FILE_COUNT=0
for folder in csv notes metadata notes_llm parquet; do
    if [ -d "$SOURCE_DIR/$folder" ]; then
        FOLDER_BYTES=$(find "$SOURCE_DIR/$folder" -type f -print0 | xargs -0 stat -f%z 2>/dev/null | awk '{s+=$1} END {print s+0}')
        # Fallback for Linux
        if [ -z "$FOLDER_BYTES" ] || [ "$FOLDER_BYTES" = "0" ]; then
            FOLDER_BYTES=$(find "$SOURCE_DIR/$folder" -type f -print0 | xargs -0 stat --format=%s 2>/dev/null | awk '{s+=$1} END {print s+0}')
        fi
        SOURCE_BYTES=$((SOURCE_BYTES + FOLDER_BYTES))
        FOLDER_FILES=$(find "$SOURCE_DIR/$folder" -type f | wc -l | tr -d ' ')
        FILE_COUNT=$((FILE_COUNT + FOLDER_FILES))
    fi
done

# Convert bytes to human readable
if [ "$SOURCE_BYTES" -gt 1073741824 ]; then
    SOURCE_SIZE=$(echo "scale=1; $SOURCE_BYTES / 1073741824" | bc)"G"
elif [ "$SOURCE_BYTES" -gt 1048576 ]; then
    SOURCE_SIZE=$(echo "scale=1; $SOURCE_BYTES / 1048576" | bc)"M"
elif [ "$SOURCE_BYTES" -gt 1024 ]; then
    SOURCE_SIZE=$(echo "scale=1; $SOURCE_BYTES / 1024" | bc)"K"
else
    SOURCE_SIZE="${SOURCE_BYTES}B"
fi

echo "  Folders: $SYNC_FOLDERS_STR"
echo "  Size: $SOURCE_SIZE"
echo "  Files: $FILE_COUNT"

# Compression step
UPLOAD_DIR="$SOURCE_DIR"
if [ "$COMPRESS" = true ]; then
    echo ""
    echo -e "${BLUE}Compressing files for upload...${NC}"
    echo "  (Spark reads .csv.gz and .txt.gz files natively)"

    # Create temporary directory
    TEMP_DIR=$(mktemp -d)
    echo "  Temp directory: $TEMP_DIR"

    # Only sync specific folders: csv, notes, metadata, notes_llm, parquet
    SYNC_FOLDERS=("csv" "notes" "metadata" "notes_llm" "parquet")

    # Create directory structure for target folders
    echo "  Creating directory structure..."
    for folder in "${SYNC_FOLDERS[@]}"; do
        if [ -d "$SOURCE_DIR/$folder" ]; then
            mkdir -p "$TEMP_DIR/$folder"
        fi
    done

    # Compress CSV files
    if [ -d "$SOURCE_DIR/csv" ]; then
        CSV_COUNT=$(find "$SOURCE_DIR/csv" -name "*.csv" -type f | wc -l | tr -d ' ')
        if [ "$CSV_COUNT" -gt 0 ]; then
            echo "  Compressing $CSV_COUNT CSV files..."
            find "$SOURCE_DIR/csv" -name "*.csv" -type f -print0 | while IFS= read -r -d '' file; do
                rel_path="${file#$SOURCE_DIR/}"
                mkdir -p "$(dirname "$TEMP_DIR/$rel_path")"
                gzip -c "$file" > "$TEMP_DIR/${rel_path}.gz" 2>/dev/null
            done
            echo -e "  ${GREEN}✓ CSV files compressed${NC}"
        fi
    fi

    # Compress text files (clinical notes) - parallel compression
    if [ -d "$SOURCE_DIR/notes" ]; then
        TXT_COUNT=$(find "$SOURCE_DIR/notes" -name "*.txt" -type f | wc -l | tr -d ' ')
        if [ "$TXT_COUNT" -gt 0 ]; then
            echo "  Compressing $TXT_COUNT text files (parallel)..."
            # Create subdirectories
            find "$SOURCE_DIR/notes" -type d -print0 | while IFS= read -r -d '' dir; do
                rel_path="${dir#$SOURCE_DIR/}"
                mkdir -p "$TEMP_DIR/$rel_path"
            done
            # Parallel compression (8 processes, suppress gzip output)
            export SOURCE_DIR TEMP_DIR
            find "$SOURCE_DIR/notes" -name "*.txt" -type f -print0 | \
                xargs -0 -P 8 -I {} bash -c 'rel="${1#$SOURCE_DIR/}"; gzip -c "$1" > "$TEMP_DIR/${rel}.gz" 2>/dev/null' _ {}
            echo -e "  ${GREEN}✓ Text files compressed${NC}"
        fi
    fi

    # Copy metadata folder without compression
    if [ -d "$SOURCE_DIR/metadata" ]; then
        METADATA_COUNT=$(find "$SOURCE_DIR/metadata" -type f | wc -l | tr -d ' ')
        if [ "$METADATA_COUNT" -gt 0 ]; then
            echo "  Copying $METADATA_COUNT metadata files (no compression)..."
            cp -r "$SOURCE_DIR/metadata"/* "$TEMP_DIR/metadata/" 2>/dev/null
            echo -e "  ${GREEN}✓ Metadata files copied${NC}"
        fi
    fi

    # Copy parquet folder without compression (already compressed)
    if [ -d "$SOURCE_DIR/parquet" ]; then
        PARQUET_COUNT=$(find "$SOURCE_DIR/parquet" -type f | wc -l | tr -d ' ')
        if [ "$PARQUET_COUNT" -gt 0 ]; then
            echo "  Copying $PARQUET_COUNT Parquet files (no compression)..."
            cp -r "$SOURCE_DIR/parquet"/* "$TEMP_DIR/parquet/" 2>/dev/null
            echo -e "  ${GREEN}✓ Parquet files copied${NC}"
        fi
    fi

    # Compress JSONL files (LLM notes)
    if [ -d "$SOURCE_DIR/notes_llm" ]; then
        JSONL_COUNT=$(find "$SOURCE_DIR/notes_llm" -name "*.jsonl" -type f | wc -l | tr -d ' ')
        if [ "$JSONL_COUNT" -gt 0 ]; then
            echo "  Compressing $JSONL_COUNT JSONL files..."
            find "$SOURCE_DIR/notes_llm" -type d -print0 | while IFS= read -r -d '' dir; do
                rel_path="${dir#$SOURCE_DIR/}"
                mkdir -p "$TEMP_DIR/$rel_path"
            done
            find "$SOURCE_DIR/notes_llm" -name "*.jsonl" -type f -print0 | while IFS= read -r -d '' file; do
                rel_path="${file#$SOURCE_DIR/}"
                mkdir -p "$(dirname "$TEMP_DIR/$rel_path")"
                gzip -c "$file" > "$TEMP_DIR/${rel_path}.gz" 2>/dev/null
            done
            echo -e "  ${GREEN}✓ JSONL files compressed${NC}"
        fi
    fi

    # Calculate compressed size (macOS compatible - uses null-terminated find)
    COMPRESSED_SIZE=$(du -sh "$TEMP_DIR" | cut -f1)
    COMPRESSED_BYTES=$(find "$TEMP_DIR" -type f -print0 | xargs -0 stat -f%z 2>/dev/null | awk '{s+=$1} END {print s+0}')
    # Fallback for Linux
    if [ -z "$COMPRESSED_BYTES" ] || [ "$COMPRESSED_BYTES" = "0" ]; then
        COMPRESSED_BYTES=$(find "$TEMP_DIR" -type f -print0 | xargs -0 stat --format=%s 2>/dev/null | awk '{s+=$1} END {print s+0}')
    fi
    COMPRESSED_FILES=$(find "$TEMP_DIR" -type f | wc -l | tr -d ' ')

    # Calculate savings
    if [ "$SOURCE_BYTES" -gt 0 ]; then
        SAVINGS_PCT=$(echo "scale=1; (1 - $COMPRESSED_BYTES / $SOURCE_BYTES) * 100" | bc)
    else
        SAVINGS_PCT="0"
    fi

    echo ""
    echo -e "${GREEN}Compression complete:${NC}"
    echo "  Original:   $SOURCE_SIZE ($FILE_COUNT files)"
    echo "  Compressed: $COMPRESSED_SIZE ($COMPRESSED_FILES files)"
    echo -e "  ${GREEN}Savings:    ${SAVINGS_PCT}%${NC}"

    UPLOAD_DIR="$TEMP_DIR"
fi

# Confirm before proceeding
echo ""
echo -e "${YELLOW}Ready to sync:${NC}"
echo "  From: $UPLOAD_DIR"
echo "  To:   $TARGET_VOLUME"
if [ "$COMPRESS" = true ]; then
    echo -e "  ${GREEN}Compression: enabled${NC}"
fi
echo ""
read -p "Continue? (y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Sync cancelled"
    exit 0
fi

# Create target directory if needed
echo ""
echo -e "${BLUE}Ensuring target volume path exists...${NC}"
databricks fs mkdirs "$TARGET_VOLUME" || true
echo -e "${GREEN}✓ Target path ready${NC}"

# Sync files
echo ""
echo -e "${BLUE}Starting sync...${NC}"
echo ""

# Use databricks fs cp with recursive flag
# This handles large files better than the sync command
databricks fs cp -r "$UPLOAD_DIR/" "$TARGET_VOLUME/" --overwrite

SYNC_EXIT_CODE=$?

echo ""
if [ $SYNC_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}✓ Sync completed successfully${NC}"
    echo -e "${GREEN}============================================================${NC}"

    # Show compression savings if used
    if [ "$COMPRESS" = true ]; then
        echo ""
        echo -e "${GREEN}Transfer summary:${NC}"
        echo "  Original size:   $SOURCE_SIZE"
        echo "  Transferred:     $COMPRESSED_SIZE"
        echo "  Savings:         ${SAVINGS_PCT}%"
    fi

    # List what was uploaded
    echo ""
    echo -e "${BLUE}Verifying upload...${NC}"
    databricks fs ls "$TARGET_VOLUME" | head -20

    echo ""
    echo -e "${GREEN}✓ Files available at: $TARGET_VOLUME${NC}"
    if [ "$COMPRESS" = true ]; then
        echo -e "${BLUE}  Note: Files are gzip compressed (.gz extension)${NC}"
        echo -e "${BLUE}  Spark reads .csv.gz files directly with:${NC}"
        echo -e "${BLUE}    spark.read.csv('$TARGET_VOLUME/csv/')${NC}"
    fi
else
    echo -e "${RED}============================================================${NC}"
    echo -e "${RED}✗ Sync failed with exit code $SYNC_EXIT_CODE${NC}"
    echo -e "${RED}============================================================${NC}"
    exit $SYNC_EXIT_CODE
fi
