#!/bin/bash

# Sample size defaults to 10,000 living patients
# MGUH saw about 90,000 patients in 2019 and 145,000 over 2018/2019
n=${1:-500}
s=${2:-12345}
y=${3:-14}

# Delete the output directory if it exists
rm -r ../output

echo "Building for $n patients"
cd .. && ./run_synthea "District of Columbia" Washington -c syntheticMGUH/synthea.properties -p $n -s $s --exporter.years_of_history=$y; cd -
#./run_synthea "District of Columbia" Washington -c syntheticMGUH/synthea.properties -p 100 -s 12345 --exporter.years_of_history=3
#./run_synthea "District of Columbia" Washington -p 100 -s 12345 --exporter.years_of_history=3
