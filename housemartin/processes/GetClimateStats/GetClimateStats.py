"""
GetClimateStats.py
==================

Process module that holds the GetClimateStats class.

"""

# Standard library imports
import os, stat, time, sys, logging
import json
from collections import OrderedDict as OD

## MONKEY PATCH the encoder for floats in json module
from json import encoder
encoder.FLOAT_REPR = lambda o: format(o, '.2f')

# WPS imports
from cows_wps.process_handler.fileset import FileSet, FLAG
import cows_wps.process_handler.process_support as process_support
from cows_wps.process_handler.context.process_status import STATUS
import processes.internal.ProcessBase.ProcessBase


# Local imports
from processes.local.GetClimateStats.lib import ClimateStatsExtractor, Location, checkValidLocation

# NOTE ABOUT LOGGING:
# You can log with the context.log object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class GetClimateStats(processes.internal.ProcessBase.ProcessBase.ProcessBase):

    # Define arguments that we need to set from inputs
    # Based on args listed in process config file
    # This must be defined as 'args_to_set = []' if no arguments!
    args_to_set = ["Locations", "Variables", "Experiment", "TimePeriod"]

    # Define defaults for arguments that might not be set
    # A dictionary of arguments that we can over-write default values for
    # Some args might be mutually-exclusive or inclusive so useful to set 
    # here as well as in the config file.
    input_arg_defaults = {"Variables": []}

    # Define a dictionary for arguments that need to be processed 
    # before they are set (with values as the function doing the processing).
    arg_processers = {}

    #### Set grid reference file
    ###GRID_REFERENCE_FILE = "/disks/kona2/old-kona1/wps_test/ACCLIMATISE_DATA/1_deg/tas_day_HadGEM2-ES_rcp85_r1i1p1_20460101-20651231_ann_01p_change.nc"
    ###DIR_TEMPLATE = "/disks/kona2/old-kona1/wps_test/ACCLIMATISE_DATA/%(var_id)s/%(experiment)s/%(inst_model)s/%(time_period)s/1_deg"
    ###FILE_PATTERN = "%(var_id)s_*_%(experiment)s_*r*_mon_%(stat)s_change.nc"
    
    def _executeProc(self, context, dry_run):
        """
        This is called to step through the various parts of the process 
        executing the actual process if ``dry_run`` is False and just 
        returning information on the volume and duration of the outputs
        if ``dry_run`` is True.
        """
        # Call standard _setup
        self._setup(context)
        a = self.args
        self.start_time = time.time()

        if not dry_run:
            # Now set status to started
            context.setStatus(STATUS.STARTED, 'Job is now running', 0)

            # Get the data
            locations = [Location(loc) for loc in a["Locations"]]
            extractor = ClimateStatsExtractor()
            results_dict = extractor.extractData(a["Experiment"], a["TimePeriod"], locations) 

            response_dict = self._constructResponseDict(a["Experiment"], a["TimePeriod"], a["Variables"], locations, results_dict)
            response = json.dumps(response_dict)
            mime_type = "application/json"

            # Really generate output
            context.outputs["RawDataOutput"] = response
            context.outputs["RawDataOutputContentType"] = mime_type

            # Finish up by calling function to set status to complete and zip up files etc
            process_support.finishProcess(context, self.fileSet, self.startTime, keep = True)
        else:
            estimated_duration = 200 # seconds
            process_support.finishDryRun(context, [], self.fileSet, estimated_duration, acceptedMessage = 'Dry run complete')           


    def _constructResponseDict(self, experiment, time_period, variables, locations, results_dict):
        """
        Constructs a response dictionary made up of sections:
             {"RequestDetails": ...
              "Response": <results_dict> }

        Returns that dictionary
        """
        response_time = time.time() - self.start_time
        
        resp = {"RequestDetails":
                    {"Locations": [loc.requested_location for loc in locations],
                     "TimePeriod": time_period,
                     "Experiment": experiment,
                     "Variables": 
                         ["tas:avg", "tas:99p", "tas:01p",
                          "tasmax:avg", "tasmax:99p",
                          "tasmin:avg", "tasmin:01p",         
                          "pr:avg", "pr:99p",
                          "psl:99p", "psl:01p", 
                          "huss:avg", "huss:99p",
                          "sfcWind:avg", "sfcWind:99p", "sfcWind:01p", 
                          "sfcWindmax:avg", "sfcWindmax:99p", 
                          "tos:avg",
                          "sfcWindDir:avg",
                          "zos:avg"],
                     "ResponseTime": response_time},
     
              "Response": results_dict}
                
        return resp            

    def _validateInputs(self):
        """
        Runs specific checking of arguments and their compatibility.
        """
        for location in self.args["Locations"]:
            if not location.count(",") == 2:
                raise Exception("Each Location must be a list of three items: <asset_id>,<lat>,<lon>, not: %s" % location)

            try:
                lat, lon = [float(l) for l in location.split(",")[1:]]
            except:
                raise Exception("The second and third element of a location must be valid numbers, not: %s" % location)
            else:
                checkValidLocation(lat, lon)
 
        if self.args["Experiment"] not in ("rcp45", "rcp85"): 
            raise Exception("'Experiment' parameter must be one of: rcp45, rcp85.")

        if self.args["TimePeriod"] not in ("2035", "2055"):
            raise Exception("'TimePeriod' parameter must be one of: 2035, 2055.") 

        
