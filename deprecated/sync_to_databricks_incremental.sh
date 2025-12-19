#!/bin/bash
set -e

# Databricks Incremental Sync Script for Synthea Output
# Uses the newer 'databricks files sync' command for faster incremental uploads
# Target: /Volumes/mi2/i2l_syntheticmguh/raw

# Configuration
SOURCE_DIR="../output"
TARGET_VOLUME="dbfs:/Volumes/mi2/i2l_syntheticmguh/raw"

# Sync options
WATCH_MODE=false  # Set to true for continuous sync
DELETE_REMOTE=false  # Set to true to delete files not in source

# ANSI color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "============================================================"
echo "Databricks Incremental Sync: Synthea Output"
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

# Display source directory info
echo ""
echo -e "${BLUE}Source directory info:${NC}"
SOURCE_SIZE=$(du -sh "$SOURCE_DIR" | cut -f1)
FILE_COUNT=$(find "$SOURCE_DIR" -type f | wc -l | tr -d ' ')
echo "  Size: $SOURCE_SIZE"
echo "  Files: $FILE_COUNT"

# Build sync command
SYNC_CMD="databricks files sync \"$SOURCE_DIR\" \"$TARGET_VOLUME\""

# Add options
if [ "$WATCH_MODE" = true ]; then
    SYNC_CMD="$SYNC_CMD --watch"
    echo -e "${YELLOW}  Mode: Continuous watch (Ctrl+C to stop)${NC}"
else
    echo "  Mode: One-time sync"
fi

if [ "$DELETE_REMOTE" = true ]; then
    SYNC_CMD="$SYNC_CMD --delete"
    echo -e "${YELLOW}  Warning: Will delete remote files not in source${NC}"
fi

# Confirm before proceeding
echo ""
echo -e "${YELLOW}Ready to sync:${NC}"
echo "  From: $SOURCE_DIR"
echo "  To:   $TARGET_VOLUME"
echo ""

if [ "$WATCH_MODE" = false ]; then
    read -p "Continue? (y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Sync cancelled"
        exit 0
    fi
fi

# Run sync
echo ""
echo -e "${BLUE}Starting sync...${NC}"
echo "Command: $SYNC_CMD"
echo ""

# Check if 'files sync' command is available
if databricks files sync --help &> /dev/null; then
    eval $SYNC_CMD
    SYNC_EXIT_CODE=$?
else
    echo -e "${YELLOW}⚠ 'databricks files sync' not available in this CLI version${NC}"
    echo "  Falling back to 'databricks fs cp -r'..."
    echo ""
    databricks fs cp -r "$SOURCE_DIR/" "$TARGET_VOLUME/" --overwrite
    SYNC_EXIT_CODE=$?
fi

echo ""
if [ $SYNC_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}============================================================${NC}"
    echo -e "${GREEN}✓ Sync completed successfully${NC}"
    echo -e "${GREEN}============================================================${NC}"

    # List what was uploaded
    echo ""
    echo -e "${BLUE}Files in target volume:${NC}"
    databricks fs ls "$TARGET_VOLUME" 2>/dev/null | head -20 || echo "  (Use Databricks UI to view files)"

    echo ""
    echo -e "${GREEN}✓ Data available at: $TARGET_VOLUME${NC}"
else
    echo -e "${RED}============================================================${NC}"
    echo -e "${RED}✗ Sync failed with exit code $SYNC_EXIT_CODE${NC}"
    echo -e "${RED}============================================================${NC}"
    exit $SYNC_EXIT_CODE
fi
