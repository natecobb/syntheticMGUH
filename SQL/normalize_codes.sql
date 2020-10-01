-- Builds a `codes` table in preperation for full normalization. This table is suprisingly small
-- as Synthea in reality uses a small number of codes.

-- Given that this requires Postgres specific code to run, likely best to re-write in
-- Python to deal with the fact that descriptions are not-consistent.

DROP TABLE IF EXISTS codes;
create table codes (vocab varchar(100), code varchar(32), description varchar(512))
ALTER TABLE codes ADD PRIMARY KEY (vocab, code);

-- Postgres specific due to glitch in synthea that codes have different descriptions
-- we use the "DISTINCT ON" clause to take just the first code/description combo.

INSERT INTO codes
select distinct on (vocab, code) vocab, code, lower(description) as description from (
SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM allergies
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM careplans
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM conditions
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM devices
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM encounters
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, reasoncode, reasondescription
 FROM medications
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM procedures
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM procedures
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code, lower(description) as description
 FROM supplies
UNION
 SELECT DISTINCT 'SNOMED' as vocab, bodysite_code, lower(bodysite_description) as description
 FROM imaging_studies) as foo
WHERE code is not null;

INSERT INTO codes
SELECT DISTINCT ON (code) 'RxNorm' as vocab, code, lower(description)
 FROM medications;

INSERT INTO codes
SELECT DISTINCT 'CVX' as vocab, code, lower(description)
 FROM immunizations;

INSERT INTO codes
SELECT DISTINCT 'LOINC' as vocab, code, lower(description)
 FROM observations;

INSERT INTO codes
SELECT DISTINCT 'DICOM-DCM' as vocab, modality_code, lower(modality_description)
FROM imaging_studies
UNION
SELECT DISTINCT ON (sop_code)  'DICOM-SOP' as vocab, sop_code, lower(sop_description)
FROM imaging_studies;

