#!/usr/bin/env python3]

# This is just a cleaned up version of the script in the Synthea source.

import csv
from datetime import datetime, timedelta

def main():
  # Probability constants
  start_prob = 0.0001
  max_prob = 0.02
  lucky_ones = 0.01
  doubling_time = timedelta(days=7)

  ## Time range
  # The very first COVD case was 1/20/2020, but testing and community spread didn't really start until March 20202
  start = datetime(2020, 1, 20)
  end = start + doubling_time - timedelta(milliseconds=1)

  headers = ['time', 'Call Infection Submodule', 'Terminal', 'Wait Until Exposure']
  cycle = 0
  with open('covid19_prob.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers)
    writer.writeheader()
    while start <= datetime.now() + doubling_time:
      row = {}
      prob_of_covid19 = start_prob * (2 ** cycle)
      if prob_of_covid19 > max_prob:
        prob_of_covid19 = max_prob
      end += doubling_time - timedelta(milliseconds=1)
      row['time'] = "{:0.0f}-{:0.0f}".format(start.timestamp() * 1000, end.timestamp() * 1000)
      row['Call Infection Submodule'] = prob_of_covid19
      row['Wait Until Exposure'] = 1 - prob_of_covid19 - lucky_ones
      row['Terminal'] = lucky_ones
      writer.writerow(row)
      start += doubling_time
      cycle += 1

if __name__ == '__main__':
  main()

