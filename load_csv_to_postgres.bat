#!/bin/bash

# This script just uploads the data, it doesn't apply any fixes or add indices like the python script.

# Example
# ./load_csv_to_postgres.bat synthea localhost public postgres "" ../output/csv OMOP/vocabulary_v5

DB=$1
HOSTNAME=$2
SCHEMA=$3
USERNAME=$4
PASSWORD=$5
DATA_DIRECTORY=$6

echo "creating schema `$SCHEMA` if its missing"
psql -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "dropping, creating and loading data from synthea csv files to relational database"
PGOPTIONS="--search_path=$SCHEMA" psql -f "OMOP/ETL/SQL/synthea_ddl.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
for TABLE in allergies careplans conditions encounters immunizations medications observations patients procedures
do
echo "... loading Synthea table $TABLE"
PGOPTIONS="--search_path=$SCHEMA" psql -c "\copy $TABLE from '$DATA_DIRECTORY/$TABLE.csv' CSV HEADER" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
done