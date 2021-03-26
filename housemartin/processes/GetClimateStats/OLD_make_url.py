#!/usr/bin/env python

import os, sys, re, pickle 
id = sys.argv[1]

d = "/disks/kona2/old-kona1/wps_test/proc_outputs/2015-01-23/GetClimateStats/%s" % id
print(d)

print(open(d + "/status.txt").read())

inputs = os.path.join(d, "inputs.pickle")
f = open(inputs)
p = pickle.load(f)


locs = "|".join(p["Locations"])
exp = p["Experiment"]
tp = p["TimePeriod"]

url = "http://ceda-wps2.badc.rl.ac.uk:8080/wps?Request=Execute&Identifier=GetClimateStats&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Locations=%s;Experiment=%s;TimePeriod=%s&ResponseForm=RawDataOutput" % (locs, exp, tp)
print(url)
