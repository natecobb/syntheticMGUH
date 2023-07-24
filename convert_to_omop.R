#!/usr/bin/env Rscript

# We are loading a version 5.4 CDM into a local PostgreSQL database called "syntheticMGUH".
# The ETLSyntheaBuilder package leverages the OHDSI/CommonDataModel package for CDM creation.
# Valid CDM versions are determined by executing CommonDataModel::listSupportedVersions().
# The strings representing supported CDM versions are currently "5.3" and "5.4".

library(ETLSyntheaBuilder) #renv::install_packages("OHDSI/ETL-Synthea")
library(DatabaseConnector)
library("optparse")


cmd_args = commandArgs(trailingOnly = TRUE)
#cmd_args = c("--uid=postgres", "--pwd=''", "--host=localhost", "--db=syntheticMGUH", "--source=public", "--target=OMOP")
parser <- OptionParser()
parser <- add_option(parser, c("-u", "--uid"), action="store", default='postgres')
parser <- add_option(parser, c("-p", "--pwd"), action="store", default='')
parser <- add_option(parser, "--host",action="store", default='localhost')
parser <- add_option(parser, "--db",action="store", default='localhost')
parser <- add_option(parser, "--source",action="store", default='public')
parser <- add_option(parser, "--target",action="store", default='OMOP')
args <- parse_args(parser, args = cmd_args, positional_arguments = TRUE)

cd <- DatabaseConnector::createConnectionDetails(
  dbms     = "postgresql",
  server   = sprintf("%s/%s", args$options$host, args$options$db),
  user     = args$options$uid,
  password = args$options$pwd,
  port     = 5432,
)


print(cd)

syntheaSchema  <- args$options$source
cdmSchema      <- args$options$target
cdmVersion     <- "5.4"
syntheaVersion <- "3.0.0"
vocabFileLoc   <- "OMOP/vocabulary_v5"

con <- connect(cd)
dbExecute(con, "DROP SCHEMA IF EXISTS OMOP CASCADE;")
dbExecute(con, "CREATE SCHEMA OMOP;")
dbDisconnect(con)

con <- connect(cd)
print("Altering source tables to convert `code`s to varchar")
dbExecute(con, "alter table public.allergies alter column code type varchar(32);")
dbExecute(con, "alter table public.immunizations alter column code type varchar(32);")
dbExecute(con, "alter table public.conditions alter column code type varchar(32);")
dbExecute(con, "alter table public.devices alter column code type varchar(32);")
dbExecute(con, "alter table public.encounters alter column code type varchar(32);")
dbExecute(con, "alter table public.encounters alter column reasoncode type varchar(32);")
dbExecute(con, "alter table public.imaging_studies alter column bodysite_code type varchar(32);")
dbExecute(con, "alter table public.imaging_studies alter column procedure_code type varchar(32);")
dbExecute(con, "alter table public.immunizations alter column code type varchar(32);")
dbExecute(con, "alter table public.medications alter column code type varchar(32);")
dbExecute(con, "alter table public.observations alter column code type varchar(32);")
dbExecute(con, "alter table public.procedures alter column code type varchar(32);")
dbExecute(con, "alter table public.procedures alter column reasoncode type varchar(32);")
dbExecute(con, "alter table public.claims alter column diagnosis1 type varchar(32);")
dbExecute(con, "alter table public.claims alter column currentillnessdate type timestamptz USING currentillnessdate::timestamp with time zone;")
# These are temporary and we'll roll them back, difference between 3.0.0 and 3.2.0
print("Altering source tables to roll back naming of `payer_transitions` dates to 3.0.0")
dbExecute(con, "alter table public.payer_transitions rename column start_date to start_year;")
dbExecute(con, "alter table public.payer_transitions rename column end_date to end_year;")
dbDisconnect(con)


ETLSyntheaBuilder::CreateCDMTables(connectionDetails = cd, cdmSchema = cdmSchema, cdmVersion = cdmVersion)
ETLSyntheaBuilder::LoadVocabFromCsv(connectionDetails = cd, cdmSchema = cdmSchema, vocabFileLoc = vocabFileLoc)
ETLSyntheaBuilder::LoadEventTables(connectionDetails = cd, cdmSchema = cdmSchema, syntheaSchema = syntheaSchema, cdmVersion = cdmVersion, syntheaVersion = syntheaVersion)

con <- connect(cd)
dbRemoveTable(con, "omop.all_visits")
dbRemoveTable(con, "omop.assign_all_visit_ids")
dbRemoveTable(con, "omop.final_visit_ids")
dbDisconnect(con)

con <- connect(cd)
print("Revising `payer_transitions` dates to 3.2.0")
dbExecute(con, "alter table public.payer_transitions rename column start_year to start_date;")
dbExecute(con, "alter table public.payer_transitions rename column end_year to end_date;")
dbDisconnect(con)
