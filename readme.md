# Synthentic MGUH

This is a series of scripts to build a synthetic Medstar Georgetown Hospital EHR data set. This data set is composed of 17 linked tables covering, patients (patients, care plans, conditions and coverage), encounters (notes, observations, medications, immunizations, procedures, suppolies, imaging and devices), payers and providers. The included scripts generate a Medstar/DC specific database and load it into a relational database with indexing. Portions can optionally be converted into OMOP format.
   
The underlying open source generator [Synthea](https://github.com/synthetichealth/synthea) was designed to create what  would approximate a Massachusetts exchange database, so generates state or city based data sets. It is possible to merge multiple runs, but all patients within a given run will be associated with hospitals/providers in that city/state. So there isn't a lot of reason to do so (we would not get patients from Arlington, VA visiting MGUH)

## Installation
 * Download the Synthea source from github
   * v3.0 tag (12/9/2021)
 * Build Synthea
   * Easiest to just first ./run_synthea which will build and execute a test run_
 * Drop this folder into your download Synthea source directory.
 
## SyntheticMGUH Build Process

From within the syntheticMGUH directory, follow the following process. Note that the defaults for each script will build a data set of 10,000 patients all in DC, with MGUH as the only hospital. The inclusion of `notes` is optional.
 * execute `pip3 install -r requirements.txt` (or the equivalent with your environment manager)
 * Create DC/Medstar specific provider files with `python3 build_provider_files.py`
   * If you want to include WHC use the `--hospitals both` flag
 * Create a Synthea data set as CSV files with `./build_synthetic_dc_population.bat`
 * Load to a relational database wth `python3 load_csv_to_rdbms.py --include_notes`
 * Optionally, covert to OMOP with `./OMOP/convert_to_omop_postgres.bat`
   * To do this you first need to download the CDM vocabulary files from Athena, see the script for details.
   * This will delete the raw Synthea tables afterwards.

## Stock Synthea Modifications

### Build batch file
- Executes all modules *except* veterans modules (as of 9/17/2020)

### Provider files
* Eliminates all hospitals but MGUH and optionally WHC
* Adds the Medstar Promptcare locations
* Trims the files to DC only and fixes some bad address issues

### Properties File
- output is CSV only; FHIR is disabled due to size
- sets the year that individual mandate went into force as 2014 (default is 2006 for Massachusetts)
- 12 years of EHR data is exported, this approximately matches when MGUH converted to Cerner
- death by natural causes is enabled
- clinical notes export is on
- death_by_natural_causes is on
- physiology generators are on
- symptom export is on
- append_numbers_to_person_names is off

### COVID-19 Module

* With version 3.0 of Synthea we no longer need to build our own COVID tables

### Codes

Synthea data is primarily de-normalized in the sense that keys/codes and text strings are both present in each table where a lookup would normally be performed. In `SQL` director is a script `normalize_codes.sql` which will build a code dictionary table called `codes`.

TODO - additional script to fully normalize the existing tables (eg drop redundant text columns.)
 
### OMOP

** This is currently broken. The old SQL script doesn't run successfully. The OHDSI scripts have been updated and improved but are based on the 2.7 data format for Synthea. **

* The `convert_omop_postgres.bat` script in the OMOP directory will transform the raw Synthea tables (using SQL) into a limited OMOP format. Several tables are missing such as providers. The script was adapted from open source code in the Synthea-OMOP repo, and has been tidied and fixed. Unfortunately it is Postgres only. 
* The [OHDSI github repo]https://github.com/OHDSI/ETL-Synthea) has R code that may work against SQL Server as well, but I did not test this.  

