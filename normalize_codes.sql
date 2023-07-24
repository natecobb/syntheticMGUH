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
SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM allergies
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM careplans
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM conditions
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM devices
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM encounters
 -- There's a bug here. The codes are actually RxNorm, but the text are SNOMED
 -- SELECT DISTINCT 'SNOMED' as vocab, reasoncode, reasondescription
 -- FROM medications
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM procedures
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, reasoncode::varchar(32), lower(reasondescription) as description
 FROM procedures
 UNION
 SELECT DISTINCT 'SNOMED' as vocab, code::varchar(32), lower(description) as description
 FROM supplies
UNION
 SELECT DISTINCT 'SNOMED' as vocab, bodysite_code::varchar(32), lower(bodysite_description) as description
 FROM imaging_studies) as foo
WHERE code is not null;

INSERT INTO codes
SELECT DISTINCT ON (code) 'RxNorm' as vocab, code::varchar(32), lower(description)
 FROM medications;

INSERT INTO codes
SELECT DISTINCT 'CVX' as vocab, code::varchar(32), lower(description)
 FROM immunizations;

INSERT INTO codes
-- Thank god for PostgreSQL's DISTINCT ON function
SELECT DISTINCT ON (code) 'LOINC' as vocab, code::varchar(32), lower(description)
 FROM observations
 ORDER BY code, length(lower(description)) DESC;

INSERT INTO codes
SELECT DISTINCT 'DICOM-DCM' as vocab, modality_code::varchar(32), lower(modality_description)
FROM imaging_studies
UNION
SELECT DISTINCT ON (sop_code)  'DICOM-SOP' as vocab, sop_code::varchar(32), lower(sop_description)
FROM imaging_studies;

