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


def main(uid, pwd, host, db, synthea_data_path, include_notes):

    # Synthea exports CSV without quotes. Some DC address lines are bad and contain commas due to bad addresses
    # eg "1234 Connecticut Ave NW, Washington DC, DC, 23007"
    # You will need to hand edit these or write a better regexp expression than mine.
    # This is because the Sythnea lookup tables in the source use quotes in the CSV files, but the
    # export files do not
    index_columns = set(["id", "patient", "encounter", "provider",
                         "payer", "organization", "date", "start", "stop"])
    synthea_files = {
        "organizations": [],
        "providers": [],
        "payers": [],
        "patients": ["BIRTHDATE", "DEATHDATE"],
        "encounters": ["START", "STOP"],
        "payer_transitions": ["START_YEAR", "END_YEAR"],
        "allergies": ["START", "STOP"],
        "careplans": ["START", "STOP"],
        "conditions": ["START", "STOP"],
        "devices": ["START", "STOP"],
        "imaging_studies": ["DATE"],
        "immunizations": ["DATE"],
        "medications": ["START", "STOP"],
        "observations": ["DATE"],
        "procedures": ["DATE"],
        "supplies": ["DATE"]
    }
    print(f"importing from folder {synthea_data_path}")

    db_connection_string = f'postgresql+psycopg2://{uid}:{pwd}@{host}/{db}'
    db_connection = create_engine(db_connection_string)

    for file_name, date_fields in synthea_files.items():
        print(f"... loading {file_name}")

        # Read the file
        df = pd.read_csv(os.path.join(synthea_data_path, "csv", f"{file_name}.csv"),
                         parse_dates=date_fields,
                         infer_datetime_format=True)

        # Cast the columns names to lower case
        # Postgres treats everything as lower case unless explicitly quoted
        df.columns = map(str.lower, df.columns)

        # Delete any existing data, foreign keys and indexes
        db_connection.execute(f"DROP TABLE IF EXISTS {file_name} CASCADE")

        # Write the data frame to the server
        if  db_connection.driver != 'psycopg2':
            df.to_sql(file_name,
                      db_connection,
                      if_exists="replace",
                      index=False,
                      dtype=sql_column_types(df),
                      method="multi",
                      chunksize=10000)
        else:
            # This goes at 2x for Postgres, won't work for other databases
            d6tstack.utils.pd_to_psql(df =  df,
                                      uri = db_connection_string,
                                      table_name = file_name,
                                      if_exists='replace')

    # Full text
    if include_notes:
        print("... loading notes")
        regex_id =  "(^.*_)(.*?).txt"
        regex_notes =  "([1-2][0-9]{3}-[0-9]{2}-[0-9]{2}.*)\n\n([\s\S]*?)\n\n\n\n"
        db_connection.execute("DROP TABLE IF EXISTS notes CASCADE")
        notes_files = os.listdir(os.path.join(synthea_data_path, "notes"))
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


    # Add indexes and foreign keys
    # This adds miminal indexes and they are all a single column.

    index_columns = set(["id", "patient", "encounter", "provider",
                         "payer", "organization", "date", "start", "stop"])
    foreign_keys = ["patient", "encounter", "provider", "payer", "organization"]

    if include_notes: synthea_files["notes"] = ["date"]

    print("Adding indices")
    for file_name, date_fields in synthea_files.items():
        print(f"... indexing {file_name}")

        # Read the column names
        df = pd.read_sql(f"SELECT * FROM {file_name} LIMIT 1", db_connection)

        # Add indexes and foreign keys
        for column in index_columns.intersection(set(list(df))):
            if column == "id":
                db_connection.execute(f"ALTER TABLE {file_name} ADD PRIMARY KEY (id)")
            else:
                db_connection.execute(f"CREATE INDEX {file_name}_{column} ON {file_name} ({column})")
            # These need to be done after the DDL is all execute, or added in order
            if column in foreign_keys:
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
    parser.add_argument('--db', default='synthea')
    parser.add_argument('--path', default='../output')
    parser.add_argument('--include_notes', action='store_const', const=True)
    args = parser.parse_args()

    main(uid=args.uid,
         pwd=args.pwd,
         host=args.host,
         db=args.db,
         synthea_data_path=args.path,
         include_notes = args.include_notes)
