"""
GetFullClimateStats.py
==================

Process module that holds the GetFullClimateStats class.

"""

# Standard library imports
import os, stat, time, sys, logging
import json
from collections import OrderedDict as OD

import random

# WPS imports
from cows_wps.process_handler.fileset import FileSet, FLAG
import cows_wps.process_handler.process_support as process_support
from cows_wps.process_handler.context.process_status import STATUS
import processes.internal.ProcessBase.ProcessBase

# Import process-specific modules
from cdms_utils import axis_utils
from cdms_utils.cdms_compat import *

# Local imports
from processes.local.GetClimateStats.lib import ClimateStatsExtractor, Location

# NOTE ABOUT LOGGING:
# You can log with the context.log object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class GetFullClimateStats(processes.internal.ProcessBase.ProcessBase.ProcessBase):

    # Define arguments that we need to set from inputs
    # Based on args listed in process config file
    # This must be defined as 'args_to_set = []' if no arguments!
    args_to_set = ["Location"]

    # Define defaults for arguments that might not be set
    # A dictionary of arguments that we can over-write default values for
    # Some args might be mutually-exclusive or inclusive so useful to set 
    # here as well as in the config file.
    input_arg_defaults = {}

    # Define a dictionary for arguments that need to be processed 
    # before they are set (with values as the function doing the processing).
    arg_processers = {}

    
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

        if not dry_run:
            # Now set status to started
            context.setStatus(STATUS.STARTED, 'Job is now running', 0)

            # Get the data
            self.extractor = ClimateStatsExtractor()
            location = Location(a["Location"]) 

            response = self.extractor.extractFullSummaryCSV(location)
            mime_type = "text/csv"

            # Really generate output
            context.outputs["RawDataOutputFileName"] = self._getOutputFileName(a["Location"])
            context.outputs["RawDataOutput"] = response
            context.outputs["RawDataOutputContentType"] = mime_type

            # Finish up by calling function to set status to complete and zip up files etc
            process_support.finishProcess(context, self.fileSet, self.startTime, keep = True)
        else:
            estimated_duration = 10 # seconds
            process_support.finishDryRun(context, [], self.fileSet, estimated_duration, acceptedMessage = 'Dry run complete')           

    def _getOutputFileName(self, location):
        "Returns a filename to suggest the output file name to the WPS controller."
        id, lat, lon = location.split(",")
        filename = "climate_stats_lat_%.3f_lon_%.3f.csv" % (float(lat), float(lon)) 
        return filename

    def _validateInputs(self):
        """
        Runs specific checking of arguments and their compatibility.
        """
        loc = self.args["Location"]

        if loc.count(",") != 2:
            raise Exception("'Location' parameter must be formatted as: 'id,lat,lon'.") 

        
