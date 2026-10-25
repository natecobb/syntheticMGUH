#!/usr/bin/env python3

"""
Build DC/MedStar-specific provider CSV files for Synthea.

Reads default provider CSVs directly from synthea/bin/synthea-with-dependencies.jar
(or from a source directory if --source points to a folder) and writes
filtered DC/MedStar provider CSVs to ./synthea/providers/syntheticMGUH/.
"""

import os
import zipfile
from io import StringIO

import pandas as pd


def load_source_csv(src_path, filename):
    """Load a provider CSV from either a Synthea JAR file or a directory."""
    if os.path.isfile(src_path) and zipfile.is_zipfile(src_path):
        with zipfile.ZipFile(src_path, "r") as zf:
            with zf.open(f"providers/{filename}") as f:
                return pd.read_csv(f, index_col=0, dtype=str)
    return pd.read_csv(os.path.join(src_path, filename), index_col=0, dtype=str)


def main(src_path, target_directory, hospitals):
    if not os.path.exists(src_path):
        raise FileNotFoundError(
            f"Synthea source '{src_path}' not found. Run ./0_setup_synthea.sh first "
            "or pass --source <path/to/synthea-with-dependencies.jar or providers dir>."
        )

    os.makedirs(target_directory, exist_ok=True)

    def filter_file_by_npi(source_file, npi_list, target_file=None):
        if target_file is None:
            target_file = source_file
        str_npis = {str(npi) for npi in npi_list if npi is not None}
        provider_df = load_source_csv(src_path, source_file)
        provider_df = provider_df[provider_df["npi"].isin(str_npis)]
        provider_df.to_csv(os.path.join(target_directory, target_file))

    hospital_locations = [
        1427145176 if hospitals in ['both', 'MGUH'] else None,        # MGUH
        1548378235 if hospitals in ['both', 'WHC', 'MWHC'] else None  # MWHC
    ]
    rehab_locations = [1326496456]  # National Rehab
    hospice_providers = [1427145176]  # MGUH
    home_health_providers = [1407960248]  # MedStar Health VNA
    ambulatory_surgery_locations = [1912090432]  # MEDSTAR SURGERY CENTER AT LAFAYETTE CENTRE LLC
    primary_care_locations = [
        1922395581,  # MMG - GEORGETOWN FAMILY PRACTICE LLC
        1285636522,  # MEDSTAR GEORGETOWN MEDICAL CENTER, INC
        1194374710,  # GUH-KIDS MOBILE MEDICAL CLINIC PROGRAM LLC
        1417571530,  # ONE MEDSTAR PRIMARY CARE LLC (1001 G ST NW)
        # Missing Navy Yard, Lafayette, WHC
    ]
    longterm_providers = [1285772772]  # BRIDGEPOINT HOSPITAL CAPITOL HILL
    nursing_home_providers = [1205923604]  # BRIDGEPOINT SUB-ACUTE AND REHAB CAPITOL HILL
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

    # Empty VA facilities so veteran care routes to MedStar facilities via default_to_hospital_on_failure
    va_df = load_source_csv(src_path, "va_facilities.csv").iloc[0:0]
    va_df.to_csv(os.path.join(target_directory, "va_facilities.csv"))

    # Urgent care
    # Note that these are more recent, but will be included historically as Synthea doesn't have
    # open/close dates
    medstar_urgent_care = (
        ",id,name,address,city,state,zip,county,phone,type,ownership,emergency,quality,LAT,LON\n"
        '4810,4001,Medstar Promptcare - Adams Morgan,1805 Columbia Rd.,WASHINGTON,DC,20009,DISTRICT OF COLUMBIA,202-797-4960,"URGENT MEDICAL CARE CENTERS AND CLINICS (EXCEPT HOSPITALS), FREESTANDING",Proprietary,No,Not Available,38.922567,-77.043162\n'
        '4811,4002,Medstar Promptcare - Capitol Hill ,228 7th St. SE,WASHINGTON,DC,20003,DISTRICT OF COLUMBIA,202-698-0795,"URGENT MEDICAL CARE CENTERS AND CLINICS (EXCEPT HOSPITALS), FREESTANDING",Proprietary,No,Not Available,38.886417,-76.995996\n'
    )
    provider_df = pd.read_csv(StringIO(medstar_urgent_care), index_col=0, dtype=str)
    provider_df.to_csv(os.path.join(target_directory, "urgent_care_facilities.csv"))
    print(f"✓ Generated MedStar provider files in {target_directory} (hospitals={hospitals})")


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser(prog='1_build_providers.py')
    parser.add_argument('--source', nargs='?', default="synthea/bin/synthea-with-dependencies.jar",
                        help='Synthea fat JAR or providers source directory (default: synthea/bin/synthea-with-dependencies.jar)')
    parser.add_argument('--target', nargs='?', default="synthea/providers/syntheticMGUH",
                        help='Target directory to write custom files to (default: synthea/providers/syntheticMGUH)')
    parser.add_argument('--hospitals', nargs='?', default="MGUH",
                        choices=['both', 'MGUH', 'WHC', 'MWHC'],
                        help='Hospitals to include: both, MGUH or WHC.')
    args = parser.parse_args()

    main(src_path=args.source,
         target_directory=args.target,
         hospitals=args.hospitals)
