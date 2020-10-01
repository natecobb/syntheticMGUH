#!/bin/bash

# Sample size defaults to 10,000 living patients
# MGUH saw about 90,000 patients in 2019 and 145,000 over 2018/2019
n=${1:-500}
s=${2:-12345}

modules=opioid_addiction:dialysis:allergic_rhinitis:pregnancy:self_harm:atopy:asthma:covid19:ear_infections:sinusitis:dementia:mTBI:anemia___unknown_etiology:urinary_tract_infections:hypothyroidism:osteoarthritis:appendicitis:copd:contraceptive_maintenance:fibromyalgia:total_joint_replacement:sore_throat:rheumatoid_arthritis:gallstones:bronchitis:sexual_activity:homelessness:epilepsy:wellness_encounters:injuries:colorectal_cancer:med_rec:congestive_heart_failure:lung_cancer:contraceptives:osteoporosis:breast_cancer:female_reproduction:gout:metabolic_syndrome_disease:allergies:metabolic_syndrome_care:lupus:cystic_fibrosis:dermatitis:food_allergies:hypertension

# Should also build our needed lookup tables
# python3 covid_historic_timeline.py ....

# Synthea has to be run from its own directory
# cd ..
# pushd ...
# popd

# Delete the output directory if it exists
rm -r ../output

echo "Building for $n patients"
cd .. && ./run_synthea -p $n -s $s "District of Columbia" Washington -c syntheticMGUH/synthea.properties -m $modules ; cd -

cd syntheticMGUH