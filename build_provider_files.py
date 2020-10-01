#!/usr/bin/env python3

# Supposedly the only files in use are
# hospitals, veterans, primarycare, and urgentcare
# but we do them all


import os
from io import StringIO

import pandas as pd


def main(src_directory, target_directory, hospitals):

    if not os.path.exists(target_directory): os.makedirs(target_directory)

    for filename in ["dialysis.csv", "hospice.csv", "hospitals.csv", "home_health_agencies.csv", "longterm.csv", "nursing.csv",
                     "primary_care_facilities.csv", "rehab.csv", "va_facilities.csv", "urgent_care_facilities.csv"]:
        provider_df = pd.read_csv(os.path.join(src_directory, filename), index_col = 0)
        provider_df = provider_df[provider_df["state"] == "DC"]
        # Fix city, state
        provider_df["city"] = provider_df["city"].str.replace(',', ' ')
        provider_df["state"] = provider_df["state"].str.replace(',', ' ')
        provider_df.to_csv(os.path.join(target_directory, filename))

    # Health system specific
    for filename in ["hospitals.csv", "rehab.csv", "dialysis.csv", "home_health_agencies.csv"]:
        provider_df = pd.read_csv(os.path.join(target_directory, filename), index_col = 0)
        provider_df = provider_df[provider_df["name"].str.startswith("GEORGETOWN UNIV") |
                                  provider_df["name"].str.startswith("WASHINGTON HOSPITAL") |
                                  provider_df["name"].str.startswith("MEDSTAR")]
        provider_df.to_csv(os.path.join(target_directory, filename))

    if hospitals != "both":
        provider_df = pd.read_csv(os.path.join(target_directory, "hospitals.csv"), index_col = 0)
        if hospitals == "MGUH":
            provider_df = provider_df[provider_df["name"].str.startswith("MEDSTAR GEORGETOWN")]
        elif hospitals == "WHC":
            provider_df = provider_df[provider_df["name"].str.startswith("MEDSTAR WASHINGTON")]
        provider_df.to_csv(os.path.join(target_directory, "hospitals.csv"))

    # Urgent care
    medstar_urgent_care = """,id,name,address,city,state,zip,county,phone,type,ownership,emergency,quality,LAT,LON
    4810,4001,Medstar Promptcare - Adams Morgan,1805 Columbia Rd.,WASHINGTON,DC,20009,DISTRICT OF COLUMBIA,202-797-4960,"URGENT MEDICAL CARE CENTERS AND CLINICS (EXCEPT HOSPITALS), FREESTANDING",Proprietary,No,Not Available,38.922567,-77.043162
    4811,4002,Medstar Promptcare - Capitol Hill ,228 7th St. SE,WASHINGTON,DC,20003,DISTRICT OF COLUMBIA,202-698-0795,"URGENT MEDICAL CARE CENTERS AND CLINICS (EXCEPT HOSPITALS), FREESTANDING",Proprietary,No,Not Available,38.886417,-76.995996
    """
    provider_df = pd.read_csv(StringIO(medstar_urgent_care), index_col = 0)
    provider_df.to_csv(os.path.join(target_directory, "urgent_care_facilities.csv"))

if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(prog='build_provider_files.py')
    parser.add_argument('--source', nargs='?', default="../src/main/resources/providers",
                        help='Source directory for original files')
    parser.add_argument('--target', nargs='?', default="../src/main/resources/providers/syntheticMGUH",
                        help='Target directory to write custom files to.')
    parser.add_argument('--hospitals', nargs='?', default="MGUH",
                        choices=['both', 'MGUH', 'WHC'],
                        help='Hospitals to include: both, MGUH or WHC.')
    args = parser.parse_args()

    main(src_directory=args.source,
         target_directory=args.target,
         hospitals=args.hospitals)

