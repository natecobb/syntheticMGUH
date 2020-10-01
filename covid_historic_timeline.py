#! /usr/bin/env python3

# Generate a probability file for COVID infection based on historical data
# This is based on postive test data for the US, which when adjusted for population
# essentially represents the likelyhood of becoming infected and testing positive.
# however, since this is national

import os
from datetime import timedelta

import pandas as pd
import requests

us_state_abbrv = {'Alabama': 'AL', 'Alaska': 'AK', 'American Samoa': 'AS', 'Arizona': 'AZ', 'Arkansas': 'AR',
                  'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
                  'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Guam': 'GU', 'Hawaii': 'HI',
                  'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY',
                  'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA', 'Michigan': 'MI',
                  'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE',
                  'Nevada': 'NV', 'New Hampshire': 'NH', 'NewJersey': 'NJ', 'NewMexico': 'NM', 'NewYork': 'NY',
                  'NorthCarolina': 'NC', 'North Dakota': 'ND', 'Northern Mariana Islands': 'MP', 'Ohio': 'OH',
                  'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Puerto Rico': 'PR', 'Rhode Island': 'RI',
                  'South Carolina': 'SC', 'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT',
                  'Vermont': 'VT', 'Virgin Islands': 'VI', 'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
                  'Wisconsin': 'WI', 'Wyoming': 'WY'}

def main(target_region, src_directory):
  populations = requests.get("https://datausa.io/api/data?drilldowns=State&measures=Population&year=latest").json()["data"]
  populations = dict(zip([x["State"] for x in populations], [x["Population"] for x in populations]))
  us_population = requests.get("https://datausa.io/api/data?drilldowns=Nation&measures=Population&year=latest").json()["data"][0]
  populations["United States"] = us_population["Population"]

  adjustment_factor = 10
  lucky_ones = 0.01

  # COVID Tracking URL
  if target_region == "United States":
    covid_tracking_url = "https://api.covidtracking.com/v1/us/daily.csv"
  else:
    covid_tracking_url = "https://api.covidtracking.com/v1/states/{state}/daily.csv".format(state = us_state_abbrv[target_region])

  target_population = populations[target_region]
  covid_df = pd.read_csv(covid_tracking_url)
  covid_df['start_date'] = pd.to_datetime(covid_df['date'], format = "%Y%m%d")
  covid_df['end_date'] = covid_df['start_date'].shift(1)
  covid_df['new_cases'] = covid_df["positive"] - covid_df["positive"].shift(-1)
  covid_df.at[len(covid_df) - 1, 'new_cases'] = covid_df.at[len(covid_df) - 1, 'positive']
  covid_df["case_rate"] = covid_df['new_cases'] / target_population

  end_epidemic = covid_df['start_date'][0] + timedelta(days = 30 * 6)
  covid_df.at[0, 'end_date'] = end_epidemic
  covid_df['time'] = covid_df.apply(lambda row :"{:0.0f}-{:0.0f}".format(row["start_date"].timestamp() * 1000, row["end_date"].timestamp() * 1000),
                                    axis = 1)
  covid_df['Call Infection Submodule'] = covid_df["case_rate"] * adjustment_factor
  covid_df['Wait Until Exposure'] = 1 - covid_df["case_rate"] - lucky_ones
  covid_df['Terminal'] = lucky_ones
  covid_df = covid_df[:-1]

  covid_df.to_csv(os.path.join(src_directory, "covid19_prob.csv"),
                           columns = ['time', 'Call Infection Submodule', 'Terminal', 'Wait Until Exposure'],
                           index = False)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(prog='covid_historic_timeline.py')
    parser.add_argument('--region', nargs='?', default = "District of Columbia",
                        choices=[*us_state_abbrv] + ["United States"],
                        help='The full name of the state or region (defaults to "District of Columbia"; use "United States" for the entire US.')
    parser.add_argument('--directory', nargs='?', default = "../src/main/resources/modules/lookup_tables",
                        help='Directory to save the lookup table')
    args = parser.parse_args()

    main(target_region = args.region,
         src_directory = args.directory)

