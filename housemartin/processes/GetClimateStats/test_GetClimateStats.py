from lib import Location
import xarray as xr

# ----- TEST 1: Check location search works ---- 

mappers = """

no CORDEX domain:  regNone,-21.9,-41.9 : Brazil : -21.5,318.0 : :  
two CORDEX domain: regAFR_MNA,0.0,0.0 : Africa-ocean : 0.5,0.0 : 0.25,0.25 : AFR-44
AFR CORDEX domain: regAFR,-14.5,25.7 : Africa : -14.5,26.0 : -14.25,25.75 : AFR-44
ARC CORDEX domain: regARC,84.0,18.5 : Arctic-ocean : 84.5,19.0 : 84.25,18.75 : ARC-44 
EUR CORDEX domain: regEUR,46,2.5 : France : 46.5,3.0 : 46.0625,2.5625 : EUR-44
MNA CORDEX domain: regMNA,44,70 : Kazakhstan : 44.5,70.0 : 44.25,70.25 : MNA-44
NAM CORDEX domain: regNAM,40,-100 : USA : 40.5,260.0 : 40.25,-99.75 : NAM-44
Lon 0 test: regLon0,-50.1,0 : Test 1 : -50.5,0.0 : :
Lon -0.7 test: regLon-0.7,-50.1,-0.7 : Test 2 : -50.5,359.0 : :
Lon -0.3 test: regLon-0.3,-50.1,-0.3 : Test 3 : -50.5,0.0 : :
Lon 0.3 test: regLon0.3,-50.1,0.3 : Test 4 : -50.5,0.0 : :
Lon 359.4 test: regLon359.4,-50.1,359.4 : Test 5 : -50.5,359.0 : :
Lon 359.8 test: regLon359.8,-50.1,359.8 : Test 6 : -50.5,0.0 : :
Lon 0 test: regLon0.0,-50.1,0.0 : Test 7 : -50.5,0.0 : :
Lon -2 test: regLon-2,-50.1,-2 : Test 8 : -50.5,358.0 : :
UK test: regUK,50,0.0 : Test 9 : 50.5,0.0 : 50.25,0.25 : ARC-44

""".strip().split("\n")

loc_map = {}


for line in mappers:

    title, loc_string, where, global_gb, regional_gb, domain = [i.strip() for i in line.split(":")]
    domain = domain or None

    loc = Location(loc_string)

    print(loc, loc.global_gb, loc.regional_gb, loc.regional_domain)
    global_gb = tuple([float(i) for i in global_gb.split(",")])
    assert global_gb == loc.global_gb

    if regional_gb:

        regional_gb = tuple([float(i) for i in regional_gb.split(",")])
        assert regional_gb == loc.regional_gb

    assert domain == loc.regional_domain
    print(loc, loc.global_gb, loc.regional_gb, loc.regional_domain)

print("All Location tests passed!!!")
print("**************************\n" * 5)

# ----- TEST 2: Call WPS to extract data    ----
from cows_wps.tests.process_tester import *
# Process identifier
identifier = "GetClimateStats"

# Define as many sets of inputs and outputs as you need 
# They are all referenced in the tester.addTest(...) calls below so it 
# fine to re-use them where appropriate
#
# NOTE: All inputs should be represented as strings in the inputs dictionaries
#
inputs_1 = {"Locations": "a23f43,0,1000",
            "Experiment": "rcp45", "TimePeriod": "2035"}
outputs_1 = { }

inputs_2 = {"Locations": "a23f43,-91,10",
            "Experiment": "rcp45", "TimePeriod": "2035"}
outputs_2 = { }

inputs_3 = {"Locations": "775,-13.24553946,-76.29354989", "Experiment": "rcp85", "TimePeriod": "2055"}

# Define configuration information as required in the ``options`` dictionary
# Supported parameters are:
#   verbose: Boolean	- increases the amount of information shown if True
#
options = {"verbose": True, "similarity_threshold": 0.1}

#-------------------------------------------------------------------
# Now we create the test and run the tests
# Define the tester which actually runs the tests
tester = ProcessTester(identifier)

# Add as many tests as you like here, each called as follows
tester.addTest(inputs_1, outputs_1, options, expect_errors = ("Location is not valid",))
tester.addTest(inputs_2, outputs_2, options, expect_errors = ("Location is not valid",))
tester.addTest(inputs_3, outputs_1, options)

# And now run all the tests
tester.runAllTests()
