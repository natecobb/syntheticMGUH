#!/usr/bin/env python3

# Import the data using Python.
# We could also adapt the OMOP bash script, but this is cross database
# TODO - schema support

import os
import re
import d6tstack
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime


# TODO
# Synthea keeps encounters etc cascading from anything considered active, like a careplan, This leads to a
# data set thats both enormous, but also not mirroring what we want.
# We can assume that some tables would be only complete in the EHR from time of implementation, while others might
# have been manually entered on paper. These tables will break integrity, ie have an encounter ID that doesn't
# exist. They could be updated to all have the encounter ID of the first post-trim encounter?
# Plan
# Trim encounters, claims, claims_transactions, imaging_studies, notes, observations, payer_transitions, procedures
#      supplies
# Keep allergies, careplans, conditions, devices, immunizations, medications, organizations, patients, payers
#      providers
def sql_column_types(dfparam):
    # We could create a set of manual definitions, but this accounts for if the Synthea schema changes
    # Readr in R is able to guess these things :-(

    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            if i in ["id", "patient", "encounter", "provider", "payer", "organization"]:
                dtypedict.update({i: sqlalchemy.types.VARCHAR(length=64)})
            else:
                dtypedict.update({i: sqlalchemy.types.VARCHAR(length=1024)})
        # Pandas imports all dates as datetime
        # Some Synthea "DATE" fields are dates, others are datetimes
        if "datetime" in str(j):
            if i in ["birthdate", "deathdate"]:
                dtypedict.update({i: sqlalchemy.types.Date()})
            else:
                dtypedict.update({i: sqlalchemy.types.DateTime()})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            if i in ["code"]:
                # Some codes are bigint, some are int, some are character ...
                # This allows us to use a single denormalized code table if we want
                dtypedict.update({i: sqlalchemy.types.VARCHAR(length=32)})
            else:
                dtypedict.update({i: sqlalchemy.types.BIGINT()})
    return dtypedict



def main(uid = 'postgres', pwd = '', host = 'localhost', db = 'syntheticMGUH',
         synthea_data_path = '../output', ehr_launch_date = datetime.strptime('2010-01-01', '%Y-%m-%d'),
         include_notes = False):
    #
    # uid = 'postgres'
    # pwd = ''

    # host = 'localhost'
    # db = 'syntheticMGUH'
    # synthea_data_path = '../output'
    # start = datetime.strptime('2010-01-01', '%Y-%m-%d')
    # include_notes = False

    # Synthea exports CSV without quotes. Some DC address lines are bad and contain commas due to bad addresses
    # eg "1234 Connecticut Ave NW, Washington DC, DC, 23007"
    # You will need to hand edit these or write a better regexp expression than mine.
    # This is because the Sythnea lookup tables in the source use quotes in the CSV files, but the
    # export files do not
    index_columns = set(["id", "patient", "encounter", "provider",
                         "payer", "organization", "date", "start", "stop"])
    synthea_files = {
        "allergies": ["STOP", "START"],
        "careplans": ["STOP", "START"],
        "claims_transactions": ["FROMDATE", "TODATE"],
        "claims": ["SERVICEDATE", "LASTBILLEDDATE1",
                   "LASTBILLEDDATE2","LASTBILLEDDATEP"],
        "conditions": ["STOP", "START"],
        "devices": ["STOP", "START"],
        "encounters": ["STOP", "START"],
        "imaging_studies": ["DATE"],
        "immunizations": ["DATE"],
        "medications": ["STOP", "START"],
        "observations": ["DATE"],
        "organizations": [],
        "patients": ["BIRTHDATE", "DEATHDATE"],
        "payers": [],
        "payer_transitions": ["START_DATE", "END_DATE"],
        "procedures": ["START", "STOP"],
        "providers": [],
        "supplies": ["DATE"]
    }
    
    # Note allergies never has a stop date, so shouldn't be filtered
    filtered_files = ["encounters", "claims_transactions", "claims", "devices", "imaging_studies",
                      "medications", "observations", "procedures", "supplies", "conditions",
                      "careplans", "conditions", "immunizations"
                      ]

    print(f"importing from folder {synthea_data_path}")

    db_connection_string = f'postgresql+psycopg2://{uid}:{pwd}@{host}/{db}'
    db_engine = create_engine(db_connection_string)

    for file_name, date_fields in synthea_files.items():
        print(f"... loading {file_name}")

        # Read the file
        print(f"...... reading")
        df = pd.read_csv(os.path.join(synthea_data_path, "csv", f"{file_name}.csv"),
                         parse_dates=date_fields,
                         infer_datetime_format=True)

        # Cast the columns names to lower case
        # Postgres treats everything as lower case unless explicitly quoted
        df.columns = map(str.lower, df.columns)

        # Synthea keeps old data if its tied to a current active problem etc
        # for our purposes we can drop it, but we need to run a second process
        # to update the tables that will now have orphaned encounter IDs.
        # allergies, careplans, conditions, devices, immunizations, medications, supplies
        # should move to the first encounter (eg, as if they are historic data documented in the
        # first EHR encounter)
        if file_name in filtered_files:
            print(f"...... filtering by date ('{synthea_files[file_name][0].lower()}')")
            date_index_field = synthea_files[file_name][0].lower()
            df = df[df[date_index_field] >= ehr_launch_date]

        # Drop duplicates
        print(f"...... dropping duplicates")
        if "id" in df.columns:
            df.drop_duplicates(subset = "id", inplace=True)
        else:
            df.drop_duplicates(inplace=True)

        with db_engine.connect() as db_connection:
            # Delete any existing data, foreign keys and indexes
            db_connection.execute(f"DROP TABLE IF EXISTS {file_name} CASCADE")

        # New transaction
        # Write the data frame to the server
        if db_engine.driver != 'psycopg2':
            with db_engine.connect() as db_connection:
                print(f"...... writing using Pandas")
                df.to_sql(file_name,
                          db_connection,
                          if_exists="replace",
                          index=False,
                          dtype=sql_column_types(df),
                          method="multi",
                          chunksize=10000)
        else:
            # This goes at 2x for Postgres, won't work for other databases
            print(f"...... writing using d6tstack")
            d6tstack.utils.pd_to_psql(df =  df,
                                      uri = db_connection_string,
                                      table_name = file_name,
                                      if_exists='replace')

    # Full text
    if include_notes:
        print("... loading notes")
        regex_id =  "(^.*_)(.*?).txt"
        regex_notes =  "([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*)\n\n([\s\S]*?)\n\n\n\n"
        notes_files = os.listdir(os.path.join(synthea_data_path, "notes"))
        with db_engine.connect() as db_connection:
            db_connection.execute("DROP TABLE IF EXISTS notes CASCADE")
            for file_name in notes_files:
                patient_id = re.search(regex_id, file_name).group(2)
                all_notes = open(os.path.join(synthea_data_path, "notes", file_name)).read()
                notes = re.findall(regex_notes, all_notes)
                notes_df = pd.DataFrame(notes, columns=["date", "note_text"])
                notes_df["patient"] = patient_id
                notes_df["date"] = pd.to_datetime(notes_df["date"], format = "%Y-%m-%d")
                notes_df.to_sql("notes",
                                db_connection,
                                if_exists='append',
                                index=False,
                                method="multi",
                                chunksize=10000)
    # End of db_connection block, thus an implicit commit
    # We need to do this in case we used d6tutil instead of SQLAlchemy

    # Add indexes and foreign keys
    # This adds miminal indexes and they are all a single column.

    index_columns = set(["id", "patient", "encounter", "provider",
                         "payer", "organization", "date", "start", "stop"])
    foreign_keys = ["patient", "encounter", "provider", "payer", "organization"]

    if include_notes: synthea_files["notes"] = ["date"]

    print("Adding indices")
    with db_engine.connect() as db_connection:
        for file_name, date_fields in synthea_files.items():
            print(f"... indexing {file_name}")
            # Read the column names
            df = pd.read_sql(f"SELECT * FROM {file_name} LIMIT 1", db_connection)

            # Add indexes and foreign keys
            # performance, since we're looking for complete duplicates
            for column in index_columns.intersection(set(list(df))):
                if column == "id":
                    # We now do this in Pandas above - saved in case
                    # Pandas flops for some reason
                    # print(f"   ... dropping duplicates from {file_name}")
                    # db_connection.execute(f"""
                    # DELETE FROM {file_name}
                    # WHERE id IN
                    # (SELECT id
                    # FROM
                    #    (SELECT id, ROW_NUMBER() OVER (PARTITION BY id) AS rn
                    #     FROM {file_name}) t
                    #     WHERE rn > 1);""")

                    print(f"  ... adding primary key for 'id'")
                    db_connection.execute(f"ALTER TABLE {file_name} ADD PRIMARY KEY (id)")
                else:
                    print(f"  ... adding index for '{column}'")
                    db_connection.execute(f"CREATE INDEX {file_name}_{column} ON {file_name} ({column})")

    # Adjust for invalid encounter IDs from where we trimmed our data files
    # When an encounter is missing we assume its from the trimming process, and
    # set it to be the patients first visit.
    print(f"... updating patients with first encounter")
    with db_engine.connect() as db_connection:
        db_connection.execute("""
            ALTER TABLE patients ADD COLUMN first_encounter_id text;
            WITH first_encounters AS (
            SELECT DISTINCT ON (patients.id)
               patients.id,
               first_value(encounters.id) OVER (PARTITION BY patient ORDER BY start ASC) first_encounter_id
            FROM patients
             LEFT JOIN encounters ON patients.id = encounters.patient
            )
            UPDATE patients
             SET first_encounter_id = first_encounters.first_encounter_id
            FROM first_encounters
              WHERE patients.id = first_encounters.id;
        """)

        # These are data that would have been entered manually and associated with the first encounter
        # Still need to trim the other tables if they don't have an encounter.
        print(f"... Updating filtered tables with last encounter id")
        for table in ["devices", "medications", "conditions", "allergies", "careplans", "conditions", "immunizations"]:
            print(f"....... {table}")
            db_connection.execute(f"""
                WITH fixed_encounters AS (
                SELECT DISTINCT {table}.encounter,
                              COALESCE(encounters.id, first_encounter_id) correct_encounter
                FROM {table}
                       LEFT JOIN patients ON patients.id = {table}.patient
                       LEFT JOIN encounters ON {table}.encounter = encounters.id
                WHERE encounters.id IS NULL)
                UPDATE {table}
                SET encounter = correct_encounter
                FROM fixed_encounters
                WHERE fixed_encounters.encounter = {table}.encounter;
            """)

        print(f"... Cleaning other tables to drop rows without encounters")
        for table in ["observations", "imaging_studies", "procedures", "supplies"]:
            print(f"....... {table}")
            db_connection.execute(f"""
            WITH missing_encounters AS (
                SELECT encounter missing_encounter
                FROM {table}
                LEFT JOIN encounters ON encounters.id = {table}.encounter
                WHERE encounters.id IS NULL
            )
            DELETE FROM {table}
            USING missing_encounters
            WHERE encounter = missing_encounter
            """)

        print(f"... Dropping orphan rows in observations (Synthea glitch related to QALYs")
        for table in ["observations"]:
            print(f"....... {table}")
            db_connection.execute(f"""
                DELETE FROM {table}
                WHERE encounter IS NULL
            """)


        for file_name, date_fields in synthea_files.items():
            # Read the column names
            df = pd.read_sql(f"SELECT * FROM {file_name} LIMIT 1", db_connection)

            # Add indexes and foreign keys
            for column in index_columns.intersection(set(list(df))):

                # These need to be done after the DDL is all executed, or added in order
                if column in foreign_keys:
                    print(f"... adding constraint for '{file_name}'")
                    print(f"........ 'FOREIGN KEY ({column}) REFERENCES {column}s (id)'")
                    fk_sql = f"""
                    ALTER TABLE {file_name} 
                        ADD CONSTRAINT FK_{file_name}_{column} 
                        FOREIGN KEY ({column}) REFERENCES {column}s (id)
                    """
                    db_connection.execute(fk_sql)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(prog='load_csv_to_rdbms.py')
    parser.add_argument('--uid', default = 'postgres')
    parser.add_argument('--pwd', default='')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--db', default='syntheticMGUH')
    parser.add_argument('--path', default='../output')
    parser.add_argument('--start', default='2010-01-01', type=lambda s: datetime.strptime(s, '%Y-%m-%d'))
    parser.add_argument('--include_notes', action='store_true', default = True)
    args = parser.parse_args()

    main(uid=args.uid,
         pwd=args.pwd,
         host=args.host,
         db=args.db,
         synthea_data_path=args.path,
         ehr_launch_date=args.start,
         include_notes = args.include_notes)
