# Synthentic MGUH

This is a series of scripts to build a synthetic Medstar Georgetown Hospital EHR data set. This data set is composed of 17 linked tables covering, patients (patients, care plans, conditions and coverage), encounters (notes, observations, medications, immunizations, procedures, suppolies, imaging and devices), payers and providers. The included scripts generate a Medstar/DC specific database and load it into a relational database with indexing. Portions can optionally be converted into OMOP format.
   
The underlying open source generator [Synthea](https://github.com/synthetichealth/synthea) was designed to create what  would approximate a Massachusetts exchange database, so generates state or city based data sets. It is possible to merge multiple runs, but all patients within a given run will be associated with hospitals/providers in that city/state. So there isn't a lot of reason to do so (we would not get patients from Arlington, VA visiting MGUH)

Note: this version is Postgresql specific - there are a few places this is hardcoded (eg connection strings), and the SQL script to convert codes relies on the Postgresql `DISTINCT ON` command. However, converting it to be less specific would be fairly trivial.

TODO:

 * Need an additional script to drop the `description` fields from various tables so that they depend only on the `code` and `vocabulary` fields and the `codes` table.
 

## 2023 Revision

Additional code fixes the issue around excess data prior to the "EHR launch date"; the Python load script deletes encounter data prior to the target date, but it retains "history" data (allergies, conditions etc) and assigns those to the first encounter as if they had been entered from paper.

This version moves to create a data set that is more usable for teaching purposes even if it breaks with reality in 
some places.
 * Bridgepoint included as longterm and nursing home, so that we have this kind of data
 * MGUH is included as a hospice (as of 2023 has inpatient hospice)
 * MGUH and MWHC become dialysis centers (technically only do inpatient)
 * Includes the VA modules, creating visits at VA centers. Not clear if this is avoidable.
 
Uses the new OMOP R conversion script from OHDSI. Because we use Synthea 3.2 the R script is modified to roll-back a couple of minor db structure changes.
 
 ## Installation
 * Download the Synthea source from github
   * v3.2.0 tag
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
 * Optionally, covert to OMOP with `Rscript convert_to_omop.R`

## Stock Synthea Modifications

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

### Codes

Synthea data is primarily de-normalized in the sense that keys/codes and text strings are both present in each table where a lookup would normally be performed. The script `normalize_codes.sql` will build a code dictionary table called `codes`.

