# Databricks Sync Scripts

Scripts for uploading Synthea output data to Databricks Unity Catalog Volumes.

## Target Location

```
/Volumes/mi2/i2l_syntheticmguh/raw
```

## Prerequisites

### 1. Install Databricks CLI

**Option A - pip:**
```bash
pip install databricks-cli
```

**Option B - Homebrew (Mac):**
```bash
brew tap databricks/tap
brew install databricks
```

### 2. Configure Authentication

**Option A - Interactive configuration:**
```bash
databricks configure
```

You'll be prompted for:
- Databricks Host (e.g., `https://your-workspace.cloud.databricks.com`)
- Personal Access Token (generate in User Settings → Access Tokens)

**Option B - Environment variables:**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
```

### 3. Verify Authentication

```bash
databricks workspace list /
```

## Available Scripts

### 1. `sync_to_databricks.sh` - Full Sync

**Use for:** Initial upload or complete refresh

**Features:**
- Uploads all files from `../output` directory
- Shows source directory size and file count
- Confirmation prompt before upload
- Uses `databricks fs cp -r` for reliable bulk uploads
- Overwrites existing files

**Usage:**
```bash
./sync_to_databricks.sh
```

**Example output:**
```
============================================================
Databricks Sync: Synthea Output to Unity Catalog Volume
============================================================

✓ Databricks CLI found
✓ Source directory found: ../output
✓ Databricks authentication successful

Analyzing source directory...
  Size: 1.2G
  Files: 1093

Ready to sync:
  From: ../output
  To:   /Volumes/mi2/i2l_syntheticmguh/raw

Continue? (y/N): y

✓ Sync completed successfully
✓ Files available at: /Volumes/mi2/i2l_syntheticmguh/raw
```

### 2. `sync_to_databricks_incremental.sh` - Incremental Sync

**Use for:** Updates to existing data, faster syncs

**Features:**
- Only uploads changed files (incremental)
- Optional watch mode for continuous sync
- Optional deletion of remote files not in source
- Falls back to `fs cp` if `files sync` not available

**Usage:**

**Basic (one-time incremental sync):**
```bash
./sync_to_databricks_incremental.sh
```

**Watch mode (continuous sync):**
```bash
# Edit script and set WATCH_MODE=true, or run with:
WATCH_MODE=true ./sync_to_databricks_incremental.sh
```

**Delete remote files not in source:**
```bash
# Edit script and set DELETE_REMOTE=true, or run with:
DELETE_REMOTE=true ./sync_to_databricks_incremental.sh
```

## What Gets Uploaded

The scripts upload the entire `../output` directory structure:

```
output/
├── csv/                    # Synthea CSV files (18 tables)
│   ├── patients.csv
│   ├── encounters.csv
│   ├── conditions.csv
│   ├── medications.csv
│   ├── observations.csv
│   ├── procedures.csv
│   ├── immunizations.csv
│   └── ... (11 more)
├── notes/                  # Clinical notes text files
│   └── *.txt (1093 files)
├── notes_llm/             # Processed notes with LLM (if generated)
│   └── *.jsonl
└── fhir/                  # FHIR JSON files (if enabled)
    └── *.json
```

## Target Volume Structure

After sync, files are available at:

```
/Volumes/mi2/i2l_syntheticmguh/raw/
├── csv/
│   ├── patients.csv
│   ├── encounters.csv
│   └── ...
├── notes/
│   └── *.txt
├── notes_llm/
│   └── *.jsonl
└── fhir/
    └── *.json
```

## Databricks Data Loading

After syncing files, load them into Delta tables:

### Python (Databricks Notebook):

```python
# Load CSV files
patients_df = spark.read.csv(
    "/Volumes/mi2/i2l_syntheticmguh/raw/csv/patients.csv",
    header=True,
    inferSchema=True
)

# Save as Delta table
patients_df.write.mode("overwrite").saveAsTable("mi2.i2l_syntheticmguh.patients")
```

### SQL (Databricks SQL):

```sql
-- Create table from CSV
CREATE OR REPLACE TABLE mi2.i2l_syntheticmguh.patients
USING CSV
OPTIONS (
  path '/Volumes/mi2/i2l_syntheticmguh/raw/csv/patients.csv',
  header 'true',
  inferSchema 'true'
);

-- Or use COPY INTO for incremental loads
COPY INTO mi2.i2l_syntheticmguh.patients
FROM '/Volumes/mi2/i2l_syntheticmguh/raw/csv/patients.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
```

## Troubleshooting

### Authentication Errors

**Error:** `Databricks authentication failed`

**Solution:**
```bash
# Reconfigure
databricks configure

# Or check environment variables
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN
```

### Volume Not Found

**Error:** `Volume not found: /Volumes/mi2/i2l_syntheticmguh/raw`

**Solution:**
- Ensure Unity Catalog is enabled in your workspace
- Check that the catalog `mi2` and schema `i2l_syntheticmguh` exist
- Create the volume in Databricks:

```sql
CREATE VOLUME IF NOT EXISTS mi2.i2l_syntheticmguh.raw;
```

### Permission Denied

**Error:** `Permission denied writing to volume`

**Solution:**
- Verify you have WRITE access to the volume
- Check with workspace admin to grant permissions:

```sql
GRANT WRITE_VOLUME ON VOLUME mi2.i2l_syntheticmguh.raw TO `your_user@domain.com`;
```

### Large File Uploads

For very large files (>1GB), consider:

1. **Split large CSVs** before upload:
```bash
split -l 100000 large_file.csv large_file_part_
```

2. **Use parallel uploads** with the Databricks REST API

3. **Increase timeout** if using Python SDK:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(timeout=600)  # 10 minute timeout
```

## Performance Tips

1. **First upload:** Use `sync_to_databricks.sh` for bulk upload
2. **Updates:** Use `sync_to_databricks_incremental.sh` for faster syncs
3. **Large datasets:** Consider compressing CSV files before upload:
   ```bash
   gzip output/csv/*.csv
   ```
4. **Network issues:** Run from a location with good connection to Databricks

## Verify Upload

After sync, verify in Databricks:

### CLI:
```bash
databricks fs ls /Volumes/mi2/i2l_syntheticmguh/raw/
databricks fs ls /Volumes/mi2/i2l_syntheticmguh/raw/csv/
```

### Databricks UI:
1. Navigate to **Data** → **Volumes**
2. Browse to `mi2` → `i2l_syntheticmguh` → `raw`
3. Verify files are present

### Databricks Notebook:
```python
# List files
display(dbutils.fs.ls("/Volumes/mi2/i2l_syntheticmguh/raw/csv/"))

# Check file size
dbutils.fs.ls("/Volumes/mi2/i2l_syntheticmguh/raw/csv/patients.csv")
```

## Script Modifications

### Change Target Volume

Edit the script and change:
```bash
TARGET_VOLUME="/Volumes/your_catalog/your_schema/your_volume"
```

### Change Source Directory

Edit the script and change:
```bash
SOURCE_DIR="../your_output_directory"
```

### Enable Watch Mode

Edit `sync_to_databricks_incremental.sh`:
```bash
WATCH_MODE=true  # Continuous sync
```

### Enable Remote Deletion

⚠️ **Warning:** This deletes files in Databricks not present in source

Edit `sync_to_databricks_incremental.sh`:
```bash
DELETE_REMOTE=true  # Delete remote files not in source
```

## See Also

- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/index.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
- [Databricks Files API](https://docs.databricks.com/api/workspace/files)
