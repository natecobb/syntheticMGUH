#!/usr/bin/env python3

# Supposedly the only files in use are
# hospitals, veterans, primarycare, and urgentcare
# but we do them all


import os
from io import StringIO

import pandas as pd


def main(src_directory, target_directory, hospitals):

    if not os.path.exists(target_directory): os.makedirs(target_directory)
    def filter_file_by_npi(source_file, npi_list, target_file = None):
        if target_file == None: target_file = source_file
        provider_df = pd.read_csv(os.path.join(src_directory, source_file), index_col=0)
        provider_df = provider_df[provider_df["npi"].isin(npi_list)]
        provider_df.to_csv(os.path.join(target_directory, target_file))

    hospital_locations = [1427145176 if hospitals in ['both', 'MGUH'] else None,  # MGUH
                          1548378235 if hospitals in ['both', 'MWHC'] else None
                         ]
    rehab_locations = [1326496456]  # National Rehab
    hospice_providers = [1427145176] # MGUH
    home_health_providers = [1407960248] # MedStar Health VNA
    ambulatory_surgery_locations = [1912090432] # MEDSTAR SURGERY CENTER AT LAFAYETTE CENTRE LLC
    primary_care_locations = [1922395581, # MMG - GEORGETOWN FAMILY PRACTICE LLC
                              1285636522, # MEDSTAR GEORGETOWN MEDICAL CENTER, INC
                              1194374710 # GUH-KIDS MOBILE MEDICAL CLINIC PROGRAM LLC
                              # Missing Navy Yard, Lafayette, WHC
                              ]
    longterm_providers = [1285772772] # BRIDGEPOINT HOSPITAL CAPITOL HILL
    nursing_home_providers = [1205923604] # BRIDGEPOINT SUB-ACUTE AND REHAB CAPITOL HILL
    dialysis_providers = hospital_locations

    filter_file_by_npi("hospitals.csv", hospice_providers, "hospice.csv")
    filter_file_by_npi("longterm.csv", longterm_providers)
    filter_file_by_npi("nursing.csv", nursing_home_providers)
    filter_file_by_npi("home_health_agencies.csv", home_health_providers)
    filter_file_by_npi("ambulatory_surgical_center.csv", ambulatory_surgery_locations)
    filter_file_by_npi("primary_care_facilities.csv", primary_care_locations)
    filter_file_by_npi("primary_care_facilities.csv", primary_care_locations, "primary_care_facilities_children.csv")
    filter_file_by_npi("primary_care_facilities.csv", primary_care_locations, "primary_care_facilities_women.csv")
    filter_file_by_npi("hospitals.csv", dialysis_providers, "dialysis.csv")
    filter_file_by_npi("rehab.csv", rehab_locations)
    filter_file_by_npi("hospitals.csv", hospital_locations)
    # These are in a different format, probably should be filtered to just have a single provider in AK
    # to force Synthea to use a local hospital.
    #filter_file_by_npi("va_facilities.csv", [])
    #filter_file_by_npi("ihs_centers.csv", [])
    #filter_file_by_npi("ihs_facilities.csv", [])

    # Urgent care
    # Note that these are more recent, but will be included historically as Synthea doesn't have
    # open/close dates
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

