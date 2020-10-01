#!/bin/bash

# This represents an slightly patched version of the scripts available from OHDSI
# https://github.com/OHDSI/ETL-Synthea
# We just use the SQL and a bash script, and ignore all of the R code
# Note that this IS NOT a good way of doing ETL transformations. This should be done
# row by row or in chunks, not in single massive transactions.

# You will need the CDM vocab files from Athena, and point to their directory with VOCAB_DIRECTORY
# http://athena.ohdsi.org/vocabulary/list

# TODO - rewrite as a python script
# TODO - make loading the vocab files optional ... this would be tough.

# Example
# ./pg_upload_omop.bat synthea_omop localhost public postgres "" ../output/csv OMOP/vocabulary_v5

DB=$1
HOSTNAME=$2
SCHEMA=$3
USERNAME=$4
PASSWORD=$5
DATA_DIRECTORY=$6
VOCAB_DIRECTORY=$7

echo "creating schema `$SCHEMA` if its missing"
#psql -c "CREATE SCHEMA IF NOT EXISTS $SCHEMA" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

#./load_csv_to_rdbms.bat $1 $2 $3 $4 $5 $6
#echo "dropping, creating and loading data from synthea csv files to relational database"
#PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/synthea_ddl.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
#for TABLE in allergies careplans conditions encounters immunizations medications observations patients procedures
#do
#echo "... loading Synthea table $TABLE"
#PGOPTIONS="--search_path=$SCHEMA" psql -c "\copy $TABLE from '$DATA_DIRECTORY/$TABLE.csv' CSV HEADER" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
#done

echo "dropping, creating and loading CDM tables"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/drop_cdm_tables.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/cdm_v5.3_ddl.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "loading data from vocab csv files to relational database"
for TABLE in CONCEPT CONCEPT_ANCESTOR CONCEPT_RELATIONSHIP CONCEPT_CLASS CONCEPT_SYNONYM DOMAIN DRUG_STRENGTH RELATIONSHIP VOCABULARY
do
echo "... loading CDM vocab $TABLE"
PGOPTIONS="--search_path=$SCHEMA" psql -c "\copy $TABLE from '$VOCAB_DIRECTORY/$TABLE.csv' CSV DELIMITER E'\t' QUOTE E'\b' HEADER" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
done

echo "creating vocab maps..."
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/create_source_to_source_vocab_map.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/create_source_to_standard_vocab_map.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"


echo "creating visit logic tables..."
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/AllVisitTable.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/AAVITable.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/final_visit_ids.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Converting Synthea to OMOP"

# Moved up, might be more performant at the end for large data sets
# but I doubt it.
echo "Adding indices and constraints"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/cdm_v5.3_indexes_ddl.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/cdm_v5.3_constraints_ddl.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading location"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_location.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading person"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_person.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading death"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_death.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading observation_period"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_observation_period.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading visit_occurrence"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_visit_occurrence.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading observation"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_observation.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading condition_occurrence"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_condition_occurrence.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading procedure_occurrence"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_procedure_occurrence.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading measurement"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_measurement.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading drug exposure"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_drug_exposure.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading condition_era"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_condition_era.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

echo "Loading drug_era"
PGOPTIONS="--search_path=$SCHEMA" psql -f "ETL/SQL/insert_drug_era.sql" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"

# Drop unused tables
# Note that there is a way to prune the tables
echo "Dropping source synthea tables"
for TABLE in allergies careplans conditions encounters immunizations medications observations patients procedures imaging_studies
do
PGOPTIONS="--search_path=$SCHEMA" psql -c "DROP TABLE IF EXISTS $TABLE CASCADE" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
done

echo "Dropping build tables
for TABLE in final_visit_ids all_visits assign_all_visit_ids
do
PGOPTIONS="--search_path=$SCHEMA" psql -c "DROP TABLE IF EXISTS $TABLE CASCADE" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
done

echo "Dropping unused tables ... this could break some OMOP specific tools"
for TABLE in care_site visit_detail specimen organizations note note_nlp device_exposure cohort cohort_attribute cohort_definition provider payer_plan_period cdm_source cost fact_relationship
do
PGOPTIONS="--search_path=$SCHEMA" psql -c "DROP TABLE IF EXISTS $TABLE CASCADE" "postgresql://$USERNAME:$PASSWORD@$HOSTNAME/$DB"
done