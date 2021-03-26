"""
load_cache.py
=============

Load all known assets into the cache.

Locations are held in:

   "processes/local/GetClimateStats/asset_locations.txt"

"""

import time

# Process identifier
identifier = "GetFullClimateStats"
identifier = "GetClimateStats"

# Read in assets and process them
assets = []

n = 1

with open("asset_locations.txt") as reader:
    for line in reader:
        if line.find("LAT") > -1: continue
        (lat, lon) = line.strip().split()
        assets.append("test_%03d,%s,%s" % (n, lat, lon))
        n += 1

if identifier == "GetClimateStats":
    inputs = {"Experiment": "rcp45", "TimePeriod": "2035"}
else:
    inputs = {}

options = {"verbose": True, "similarity_threshold": 0.1}

tester = ProcessTester(identifier)

first = "test_253"
first = "test_697"
do_run = False
do_run = True

for loc in assets:

    print("Running for: %s" % loc)

    if identifier == "GetClimateStats":
        inputs["Locations"] = loc
    else:
        inputs["Location"] = loc
    
    if loc.find(first) == 0 or do_run: 
        do_run = True
        time.sleep(1.1)
        tester.runTest(identifier, inputs, {}, options)

