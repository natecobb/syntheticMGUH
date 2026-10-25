#!/bin/bash
# Generate synthetic MGUH population data and convert to Parquet via 2_generate_incremental.py.
#
# Usage:
#   ./2_generate_data.sh [num_patients] [seed] [years_of_history] [batch_size]
#   ./2_generate_data.sh --total 5000 --batch_size 250 --seed 12345 --years 14

set -e

if [[ "${1:-}" == -* ]]; then
    exec uv run python 2_generate_incremental.py "$@"
fi

n=${1:-500}
s=${2:-12345}
y=${3:-14}
b=${4:-$n}

exec uv run python 2_generate_incremental.py --total "$n" --seed "$s" --years "$y" --batch_size "$b"
