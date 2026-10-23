#!/bin/bash

# Sample size defaults to 500 living patients
# MGUH saw about 90,000 patients in 2019 and 145,000 over 2018/2019
set -e

n=${1:-500}
s=${2:-12345}
y=${3:-14}
b=${4:-}

# If a 4th argument (batch_size) is provided, run the iterative, recoverable batch pipeline
if [ -n "$b" ]; then
    exec uv run python 2_generate_incremental.py --total "$n" --seed "$s" --years "$y" --batch_size "$b"
fi

SYNTHEA_JAR="${SYNTHEA_JAR:-bin/synthea-with-dependencies.jar}"

if [ ! -f "$SYNTHEA_JAR" ] || [ ! -d "bin/resources/physiology" ]; then
    echo "Synthea runtime not initialized in ./bin — running ./0_setup_synthea.sh..."
    ./0_setup_synthea.sh
fi

# Delete only this seed's output directory if it exists
SEED_DIR="./output/seed_${s}"
rm -rf "$SEED_DIR"
mkdir -p "$SEED_DIR"
ln -sfn "seed_${s}" ./output/latest

echo "Building for $n patients (seed=$s, years_of_history=$y) -> $SEED_DIR"
java -cp ".:bin/resources:$SYNTHEA_JAR" App "District of Columbia" Washington \
    -c synthea.properties \
    -p "$n" \
    -s "$s" \
    -cs "$s" \
    --exporter.baseDirectory="${SEED_DIR}/" \
    --exporter.years_of_history="$y"
