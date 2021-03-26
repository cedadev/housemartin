"""
lib.py
======

Holds helper classes and functions for use by processes:

 * GetClimateStats
 * GetFullClimateStats

"""

# Standard library imports
import os, sys, re, glob, logging, copy, types
import cPickle as pickle
from collections import OrderedDict

# Third-party imports
import .axis_utils

# Local imports
from processes.local.GetClimateStats.vocabs import vocabs

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Local variables
GWS = "/gws/nopw/j04/acclim"


def checkValidLocation(lat, lon):
    "Checks ``lat`` and ``lon`` are in a valid range for the Earth."
    msg = ""
    if lat < -90 or lat > 90:
        msg += "Latitude is out of range: %s; " % lat
    if lon < -360 or lon > 360:
        msg += "Longitude is out of range: %s;" % lon

    if msg:
        raise Exception("Location is not valid. %s" % msg)


class ProcessedLocationsHolder(object):

    def __init__(self):
        self.dict = {"Global": [], "Regional": []}

    def _extractGridBoxDetails(self, location, domain_type):
        "Extracts lat, lon and domain info from Location object."
        if not isinstance(location, Location):
            raise Exception("ProcessedLocationsHolder only accepts instances of Location class to process.")

        if domain_type == "Global":
            lat, lon = location.global_gb
            domain = domain_type
        else:
            lat, lon = location.regional_gb
            domain = location.regional_domain

        return (lat, lon, domain)

    def isProcessed(self, location, domain_type):
        (lat, lon, domain) = self._extractGridBoxDetails(location, domain_type)

        if (lat, lon, domain) in self.dict[domain_type]:
            return True

        return False

    def add(self, location, domain_type):
        (lat, lon, domain) = self._extractGridBoxDetails(location, domain_type)
        self.dict[domain_type].append((lat, lon, domain))


class StatsCacheBase(object):
    """
    A base class for caching climate stats on the file system.
    """
    # The base directory for the cache; and the file name used for each cache file
    CACHE_DIR = "/tmp"
    FILE_NAME = "cached.dat"

    # A list of facets that need to be set/requested to interact with the cache
    FACETS = []
    # A dictionary of {FACET: PROCESSING_METHOD} mappings to allow special handling of facets
    FACET_MAPPERS = {} 

    def __init__(self):
        logger.info("Setting up cache manager.")
        if not os.path.isdir(self.CACHE_DIR):
            os.mkdir(self.CACHE_DIR)

    def _handleLocationFloat(self, flt):
        """
        Returns a string when given a float. Floats are rounded to 4 decimal places.
        Negative numbers are preceded with "m" instead of "-".
        """ 
        return ("%1.4f" % flt).replace("-", "m")

    def _getDir(self, **kwargs):
        """
        Constructs and returns a directory to hold the cached object.
        """ 
       # logger.info("Looking for cache with details: %s" % str(kwargs))
        items = []
        for facet in self.FACETS:
            if facet not in kwargs:
                raise Exception("Facet not found in cache request: %s" % facet)

            if facet in self.FACET_MAPPERS:
                value = getattr(self, self.FACET_MAPPERS[facet])(kwargs[facet])
            else:
                value = kwargs[facet] 
 
            items.append(value)

        return os.path.join(self.CACHE_DIR, "/".join(items))

    def get(self, **kwargs):
        dir = self._getDir(**kwargs)
        fpath = os.path.join(dir, self.FILE_NAME)

        if not os.path.isdir(dir) or not os.path.isfile(fpath):
            return False

        return self._unpackDataFile(fpath)

    def _unpackDataFile(self, fpath):
        "Unpacks contents of file and returns as a dictionary."
        logger.info("Extracting data from cache file: %s" % fpath)
        data = pickle.load(open(fpath, "rb"))
        return data

    def put(self, **kwargs):
        "Puts contents in to the cache."
        if "data" not in kwargs:
            raise Exception("No data sent to cache PUT.")

        dir = self._getDir(**kwargs)
        fpath = os.path.join(dir, self.FILE_NAME)

        if not os.path.isdir(dir):
            logger.info("Creating cache directories: %s" % dir)
            os.makedirs(dir)

        logger.info("Writing cache file: %s" % fpath)
        pickle.dump(kwargs["data"], open(fpath, "wb"))

    def delete(self, **kwargs):
        "Delets a record from the cache."
        dir = self._getDir(**kwargs)
        fpath = os.path.join(dir, self.FILE_NAME)
        logger.warn("Deleting cache file: %s" % fpath)

        if os.path.isfile(fpath):
            os.remove(fpath)


class ClimateStatsCache(StatsCacheBase):
    """
    A caching class that store pre-read data on the file system with a file per location.
    """
    CACHE_DIR = f"{GWS}/web_cache/summary"
    FACETS = ("domain_type", "experiment", "time_period", "lat", "lon")
    FACET_MAPPERS = {"lat": "_handleLocationFloat", "lon": "_handleLocationFloat"} 

class FullClimateStatsCache(StatsCacheBase):
    """
    A caching class that store pre-read data on the file system with a file per location.
    """
    CACHE_DIR = f"{GWS}/web_cache/full"
    FILE_NAME = "cached.dat"
    FACETS = ('domain_type', 'lat', 'lon')
    FACET_MAPPERS = {"lat": "_handleLocationFloat", "lon": "_handleLocationFloat"}


class Location(object):
    """
    Simple class to hold a mapping of an input location to relevant gridded locations.

    It holds attributes:
     * requested: (lat, lon)
     * asset_id:  str
     * global_gb:    (lat, lon)
     * regional_gb:  (lat, lon)
     * regional_domain: str

    The regional domain is the CORDEX region code.
    """

    GRID_REFERENCE_DIR = f"{GWS}/ACCLIMATISE_GRID_REF_FILES"
    REGIONAL_DOMAINS = "AFR-44 ARC-44 EUR-44 MNA-44 NAM-44".split()
    REGIONAL_FILE_TMPL = "tas_%si.nc"
    GLOBAL_FILE_NAME = "tas_global.nc" 
    REF_VARIABLE = "tas"

    def __init__(self, location):
        if location.count(",") != 2:
            raise Exception("Location must be a comma-seperated list of three items: AssetId,Latitude,Longitude. Not: '%s'." % str(location)) 
     
        items = location.strip().split(",")
        self.requested = [float(i) for i in items[1:]]
        self.asset_id = items[0]

        self._setGlobalGridBox()
        self._setRegionalGridBox()
        # This next one should eventually replace the above _setRegionalGridBox()
        self._setRegionalGridBoxes()

    def __str__(self):
        return "(%s, %s) [%s]" % (self.requested[0], self.requested[1], self.asset_id)

    def _setGlobalGridBox(self):
        """
        Finds the nearest global grid box and sets it to ``self.global_gb``.
        """
        global_ref_file = os.path.join(self.GRID_REFERENCE_DIR, self.GLOBAL_FILE_NAME)
        self.global_gb = self._getGridBoxForLocation(global_ref_file, domain_type = "Global") 

    def _setRegionalGridBox(self):
        """
        Finds the nearest global grid box and sets it to ``self.global_gb``.
        """
        for domain in self.REGIONAL_DOMAINS:
            regional_ref_file = os.path.join(self.GRID_REFERENCE_DIR, self.REGIONAL_FILE_TMPL % domain)

            resp = self._getGridBoxForLocation(regional_ref_file, domain_type = "Regional")

            if resp:
                self.regional_gb = resp
                self.regional_domain = domain
                break
        else:
            # If location not found in any of the regional domains then return (None, None)
            self.regional_gb = (None, None)
            self.regional_domain = None

    def _setRegionalGridBoxes(self):
        """
        Loops through regional domains and sets Ordered Dictionary ``self.regional_gbs``.
        Each entry is: {domain: (lat, lon)}
        """
        self.regional_gbs = OrderedDict()

        for domain in self.REGIONAL_DOMAINS:
            regional_ref_file = os.path.join(self.GRID_REFERENCE_DIR, self.REGIONAL_FILE_TMPL % domain)
            resp = self._getGridBoxForLocation(regional_ref_file, domain_type = "Regional")

            if resp:
                self.regional_gbs[domain] = resp

        # Note: self.regional_gbs can be empty and can be tested for as such

    def _getGridBoxForLocation(self, ref_file, domain_type):
        "Reads grid and returns nearest point as (lat, lon)."
        (lat, lon) = self.requested 

#        f = cdms.open(ref_file)
#        vm = f[self.REF_VARIABLE]
        ds = xr.open_dataset(ref_file, use_cftime=True)
        v = ds[self.REF_VARIABLE]
        lats = get_coord_by_type(v, 'latitude').values
        lons = get_coord_by_type(v, 'longitude').values


        var_lats = [float(l) for l in lats] #  vm.getLatitude()]
        var_lons = [float(l) for l in lons] #vm.getLongitude()]
#        f.close()

        # Check longitude is in range
        lon = self.isInLongitudeRange(lon, var_lons, domain_type)
        if type(lon) == bool and lon == False:
            return False

        if domain_type == "Regional":
            if lat > max(var_lats) or lat < min(var_lats): 
                return False

        glat = axis_utils.nudgeSingleValuesToAxisValues(lat, var_lats)
        glon = axis_utils.nudgeSingleValuesToAxisValues(lon, var_lons)

        return (glat, glon)

    def isInLongitudeRange(self, i, lons, domain_type):
        """
        Return value of ``i`` to use if ``i`` is ``lons``. Accounts for either of these to be defined as far
        -360 to 360.
        If ``i`` not in ``lons``. Return False
        """
        # Special treatent for wrap-around of longitude
        if domain_type == "Global":
            if (i > -1 and i < -0.5) or (i > 359 and i < 359.5):
                return 359
            elif (i >= -0.5 and i < 0) or (i >= 359.0 and i < 360):
                return 0

        if i >= min(lons) and i <= max(lons):
            return i

        # Adjust test value just in case the range of lons is defined as (-360 to 0) or (0 to 360)
        if i < 0: 
            i += 360
        elif i > 0: 
            i -= 360

        if i >= min(lons) and i <= max(lons):
            return i

        return False

    @property
    def requested_location(self):
        "A property that returns a dictionary of the requested location." 
        return {"Id": self.asset_id, "Lat": self.requested[0], "Lon": self.requested[1]}
        

class ClimateStatsExtractor(object):

    DIR_TEMPLATE = f"{GWS}/outputs/data/%(dt)s/%(var_id)s/%(experiment)s/%(inst_model)s/%(time_period)s/%(res)s"
    GLOBAL_FILE_PATTERN = "%(var_id)s_*_%(experiment)s_*r*_%(meaning_period)s_%(statistic)s_change.nc"
    REGIONAL_FILE_PATTERN = "%(var_id)s_*_%(experiment)s_*r*_%(meaning_period)s_%(statistic)s_change.nc"
    

    def __init__(self):
        """
        Set up cache.
        """
        self.cache_stats = ClimateStatsCache()
        self.cache_full = FullClimateStatsCache()

    def _addRequestedLocationToResultsDict(self, location, domain_type, results_dict):
        """
        Merges in the details of ``location`` into the structure of the ``results_dict`` to 
        indicate that the results already obtained are valid for this location.
        """
        if domain_type == "Global":
            location_details = (location.global_gb[0], location.global_gb[1], "Global")
        else:
            location_details = (location.regional_gb[0], location.regional_gb[1], "Regional") 

        # Loop through each of the locations in the results dictionary
        # and add the location details to the appropriate object
        for loc_dict in results_dict["Locations"]:
            processed_location_details = [loc_dict["ModelLocation"]["Lat"], loc_dict["ModelLocation"]["Lon"]]

            # Get previously processed location to compare with current location
            if domain_type == "Global":
                processed_location_details.append("Global")
            else:
                processed_location_details.append("Regional")

            if location_details[0] == None or tuple(processed_location_details) == location_details:
                loc_dict["RequestedLocations"].append(location.requested_location)
                return
        else:
            raise Exception("Did not find location match in %s results dictionary for: %s" % (domain_type, location))

    def extractData(self, experiment, time_period, locations):
        """
        Returns a dictionary of data formatted as:
           {...}
        """
        # Check locations are correct
        for location in locations:

            if not isinstance(location, Location): 
                raise Exception("Incorrect location object sent to extractData(...) - must be instance of Location class.")

        data = {"GlobalData": [], "RegionalData": []}

        # Create an object to keep track of which regional and global grid boxes have already been 
        # processed so that we can re-use them for multiple requested locations where necessary.
        loc_holder = ProcessedLocationsHolder()
                
        for domain_type in ("Global", "Regional"):

            mtype_tag = domain_type + "Data"
            data[mtype_tag] = {"Locations": []}

            for location in locations:

                # Check if this location has already been used
                if loc_holder.isProcessed(location, domain_type):
                    self._addRequestedLocationToResultsDict(location, domain_type, data[mtype_tag])
                    continue
                
                data_dict = self._createLocationDict(domain_type, location)
                data_dict["Results"] = []

                # Set up lat, lons for cache - based on domain type
                if domain_type == "Global":
                    cache_lat_lon = location.global_gb
                else:
                    cache_lat_lon = location.regional_gb 

                    # Check if regional GB not set
                    if cache_lat_lon == (None, None): continue 

                # Check cache for previously calculated results
                cached_results = self.cache_stats.get(domain_type = domain_type, experiment = experiment, time_period = time_period, 
                                                      lat = cache_lat_lon[0], lon = cache_lat_lon[1])

                if cached_results:
                    data_dict["Results"] = cached_results 
                else:
                    results_dict = {}

                    for var_stat in vocabs.getStatisticIds(domain_type): 

                        var_id, stat = var_stat.split(":")
                        
                        inst_models = vocabs.getModelList(domain_type, var_id)

                        for statistic in (stat,):
                         
                            for inst_model in inst_models:

                                logger.info("Extracting data for: %s, %s, %s, %s, %s, %s, %s" % (domain_type, inst_model, experiment, time_period, var_id, statistic, location))
                                results_dict.setdefault(inst_model, {})

                                # Extract the actual data here
                                data_resp = self.extractDataAtPoint(domain_type, inst_model, experiment, time_period, var_id, statistic, location)
                                month_and_year_values = data_resp["values"]
                                
                                for (i, meaning_period) in enumerate(vocabs.meaning_periods):
                                    
                                    value = month_and_year_values[i]
                                    results_dict[inst_model]["%s:%s:%s" % (var_id, meaning_period, statistic)] = value
                      
                    transposed_results = self._transposeResults(results_dict, domain_type)
                    data_dict["Results"] = transposed_results 
                    
                    # Write to the cache
                    self.cache_stats.put(domain_type = domain_type, experiment = experiment, time_period = time_period,
                                   lat = cache_lat_lon[0], lon = cache_lat_lon[1], data = data_dict["Results"])

                # Add location to those processed
                loc_holder.add(location, domain_type)
                data[mtype_tag]["Locations"].append( data_dict )

        return data


    def _transposeResults(self, results_dict, domain_type):
        """
        Transposes the structure of the results dictionary and returns a new dictionary fit for the output structure.
        """
        results = []

        for var_stat in vocabs.getStatisticIds(domain_type): #var_keys:
            var_id, stat = var_stat.split(":")

            inst_models = vocabs.getModelList(domain_type, var_id)

            for statistic in (stat,): #vocabs.getStatsList(var_id):

                for meaning_period in vocabs.meaning_periods:
                    key = "%s:%s:%s" % (var_id, meaning_period, statistic)

                    values = [results_dict[inst_model][key] for inst_model in inst_models]

                    # Format as rounded strings
                    values_rounded = values 
                    results.append( {"VariableName": key, "Values": values_rounded} )

        return results 


    def extractDataAtPoint(self, domain_type, inst_model, experiment, time_period, var_id, statistic, location):
        """
        Works out which data file to use, reads it and returns data value.
        Returns dictionary of: {"values": [...], "grid_box": (lat, lon)}
        """
        values = []
        model = inst_model.split("/")[-1]

        # Defines settings based on domain type
        if domain_type == "Global":
            res = "1_deg"
            file_template = self.GLOBAL_FILE_PATTERN
            lat, lon = location.global_gb
        else:
            # Return if domain is not relevant to the selected grid box
            regional_domain = inst_model.split("/")[1]

            if regional_domain not in location.regional_gbs: 
                return {"values": [None] * 13, "grid_box": None}

            res = "0.5_deg"
            file_template = self.REGIONAL_FILE_PATTERN
            lat, lon = location.regional_gbs[regional_domain]

        dt = domain_type.lower()

        for meaning_period in ("mon", "ann"):
 
            dr = self.DIR_TEMPLATE % vars()
            fpattern = os.path.join(dr, file_template % vars())
            items = glob.glob(fpattern)

            if len(items) != 1:
                raise Exception("Ambiguous response when globbing for file pattern '%s'. Matched %d responses: %s." % (fpattern, len(items), str(items)))

            logger.warn("Extracting data at: %s %s %s %s %s %s %s %s %s" % (domain_type, inst_model, experiment, time_period, var_id, statistic, meaning_period, lat, lon))
 
            file_path = items[0] 
            values.extend( self._extractPointDataFromFile(domain_type, meaning_period, file_path, var_id, lat, lon) )
 
        return {"values": values, "grid_box": (lat, lon)}

    def _createLocationDict(self, domain_type, location):
        "Returns a dictionary about location and mappings."
        if domain_type == "Global":
            d = {"ModelLocation": {"Lat": location.global_gb[0],
                                   "Lon": location.global_gb[1]},
                 "RequestedLocations": [location.requested_location]}

        elif domain_type == "Regional":
            d = {"ModelLocation": {"Lat": location.regional_gb[0],
                                   "Lon": location.regional_gb[1],
                                   "RegionalDomain": location.regional_domain},
                 "RequestedLocations": [location.requested_location]}
        return d

    def _extractPointDataFromFile(self, domain_type, meaning_period, fpath, var_id, lat, lon):
        "Returns a list of data values for the given point."
        logger.warn("Reading data from: %s" % fpath)

        f = cdms.open(fpath)
    
        try: 
            values = f(var_id, latitude=lat, longitude=lon, squeeze=1)
        except:
            logger.warn("Cannot extract variable '%s' from: %s (at: (%s, %s))." % (var_id, fpath, lat, lon))
            if meaning_period == "ann":
                values = [None]
            else:
                values = [None] * 12

        try:
            values = list(values)
        except:
            values = [float(values)]

        # Replace missing values with None value
        resp = []
        for value in values:
            # Test if it is a Masked Value by checking if count of non-masked items is 1
            if (hasattr(value, "count") and (value.count() == 1)) or (type(value) == float and not N.isnan(value)): 
                resp.append(value)
            else:
                resp.append(None)

        f.close()

        return resp


    def extractFullSummaryCSV(self, location):
        """
        Returns a string on CSV content following agreed structure.
        """

        header = "Time Period,Experiment,Model,Model Type,Variable,Statistic,Units,Grid Box Lat,Grid Box Lon,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,Ann\n"
        lines = [header]

        for domain_type in ("Global", "Regional"):
            lines.extend(self._getCSVLines(domain_type, location))

        csv = "".join(lines)
        return csv

    def _getCSVLines(self, domain_type, location):
        """
        Returns data content for CSV file for given experiment.
        """
        # Check cache for previously calculated results
        # Set up lat, lons for cache - based on domain type
        if domain_type == "Global":
            cache_lat_lon = location.global_gb
        else:
            cache_lat_lon = location.regional_gb
            
            # Check if regional GB not set
            if cache_lat_lon == (None, None): return []

        # Check cache for previously calculated results
        cached_results = self.cache_full.get(domain_type = domain_type, lat = cache_lat_lon[0], lon = cache_lat_lon[1])

        if cached_results:
            return cached_results

        lines = []
        months = "jan feb mar apr may jun jul aug sep oct nov dec".split()

        for time_period in ("2035", "2055"):
            for experiment in ("rcp45", "rcp85"):

                    inst_models = vocabs.getAllModels(domain_type) 

                    for inst_model in inst_models:

                        for var_id in vocabs.getVariableList(domain_type, inst_model):

                            for statistic in vocabs.getStatsList(var_id):

                                # Ignore any models not required for this variable
                                required_models = vocabs.getModelList(domain_type, var_id)
                                if inst_model not in required_models: continue 

                                logger.info("Extracting data for: %s, %s, %s, %s, %s, %s, %s" % (domain_type, inst_model, experiment, time_period, var_id, statistic, location))
                                (var_name, units) = vocabs.variables[var_id]

                                # Extract the actual data here
                                data_resp = self.extractDataAtPoint(domain_type, inst_model, experiment, time_period, var_id, statistic, location)

                                # Do not provide data if grid box is not in regional domain
                                if data_resp["grid_box"] == None:
                                    continue
                                else:
                                    actual_lat, actual_lon = data_resp["grid_box"]

                                month_and_year_values = data_resp["values"]

                                # Build CSV line
                                line = "%s,%s,%s,%s,%s,%s,%s,%s,%s," % (time_period, experiment, inst_model, domain_type.title(), var_name, statistic, units, actual_lat, actual_lon)
 
                                # Handle missing values 
                                values_string = ""
                                for value in month_and_year_values:
                                    if type(value) == types.NoneType: 
                                        values_string += "NaN,"
                                    else:
                                        values_string += "%0.2f," % value
                                
                                line += values_string[:-1] + "\n" 
                                lines.append(line)

        # Write to cache:
        self.cache_full.put(domain_type = domain_type, lat = cache_lat_lon[0], lon = cache_lat_lon[1], data = lines)
        return lines
                             

if __name__ == "__main__":

    import pprint
    do_test = True
    do_test_regional = False

    if do_test: 
        x = ClimateStatsExtractor()
        loc = Location("6500,31.010119,26.61568")
        loc = Location("BrentA,61.034917,1.705389")
        print loc.global_gb, loc.regional_gb
        #loc = Location("6500,58.00257,-0.37982")
        #loc = Location("ABC,70,40")

        #res = x.extractData("rcp85", "2055", [loc])
        #res = x.extractFullSummaryCSV(loc)

        url = """http://wps-web1.ceda.ac.uk/wps?Request=Execute&Identifier=GetClimateStats&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Locations=646,-34.67422939,-58.33487996|647,-34.67422939,-58.33487996|648,-38.077271,144.379809|649,-27.11080386,150.9053421|650,-21.97703671,148.0171701|651,-38.077271,144.379809|652,-20.59414,116.77582|653,-20.611281,116.760629|654,-20.777975,115.440375|655,51.33943921,3.176255054|656,4.622010171,114.3424474|657,4.618961697,114.3450802|658,4.67441,114.46585|659,42.881499,-82.406387|660,56.3909213,-116.8754119|661,57.254586,-111.5058773|662,57.254586,-111.5058773|663,49.3069739,-114.0009232|664,51.1385257,-114.5804139|665,56.3836199,-116.7896963|666,56.3836199,-116.7896963|667,49.2978523,-113.8285183|668,54.5424419,-110.4029778|669,51.134111,-114.5918239|670,53.7916725,-113.1015558|671,54.5424419,-110.4029778|672,51.9413362,-114.5574876|673,53.8016042,-113.0964362|674,49.3069739,-114.0009232|675,51.1385257,-114.5804139|676,51.573061,-114.8532431|677,54.05345836,-128.6466681|678,42.881499,-82.406387|679,22.7585,114.61|680,55.66378541,8.151037609|681,55.593854,9.748793|682,29.67993132,28.01455816|683,29.64048225,28.11068848|684,29.77912623,28.05926177|685,29.85798683,28.525425|686,29.80335848,29.48508927|687,29.88276293,27.7113844|688,31.10119369,26.61568474|689,29.85264162,27.94805845|690,29.75842073,28.00507871|691,29.7722372,28.17891229|692,51.04358164,2.460690033|693,-2.824573613,10.072633|694,53.37622162,7.217671343|695,50.848725,6.974348|696,53.094381,14.235814|697,50.848725,6.974348|698,49.056983,8.336805|699,53.484969,9.970306|700,49.005321,8.32434|701,53.484969,9.970306|702,53.094381,14.235814|703,21.09684,72.62463|704,21.09684,72.62463|705,30.56435141,47.34399911|706,30.26876707,47.73220037|707,30.89395491,47.54039541|708,31.07926174,47.62344399|709,33.9464,131.19|710,35.5096,139.743|711,34.9429,136.649|712,33.9464,131.19|713,35.5096,139.743|714,34.9429,136.649|715,43.16805772,51.62083123|716,44.55797008,50.26417079|717,47.25338257,52.47364808|718,3.28417,113.07833|719,3.28386,113.0795|720,5.26556,115.16944|721,3.28111,113.07417|722,5.26556,115.16944|723,3.28386,113.0795|724,5.63742752,115.8881457|725,2.5341,101.809653|726,2.5341,101.809653|727,31.99092,-116.83748|728,22.4936,-97.8945|729,53.32544785,6.94074546|730,51.880075,4.338264|731,51.691667,4.591667|732,51.880075,4.338264|733,-39.32352997,174.28238|734,-39.05967002,174.02561|735,-39.07914995,174.01061|736,-39.47495007,174.17762|737,-39.06118993,174.02181|738,-39.60594998,174.3080699|739,-39.40366998,173.81193|740,-38.99317002,174.2783099|741,5.349200849,5.348154752|742,4.430641635,7.165360989|743,4.421265,7.15209|744,70.68757263,23.5981124|745,63.39433031,8.653355027|746,62.8458198,6.9327729|747,59.27283003,5.519836504|748,59.31799351,10.52961967|749,60.54783934,4.843788576|750,60.62062283,4.849492773|751,18.31823439,55.10244525|752,18.55637143,55.86578838|753,18.85326377,56.30504885|754,22.87553893,55.37258682|755,20.02374396,56.14420549|756,22.15578876,56.03365367|757,22.14466956,56.02139909|758,22.32539784,56.4903097|759,22.15295427,56.02038734|760,22.15444605,56.02813515|761,19.5823292,55.75758547|762,20.02737215,56.14705407|763,22.32595753,56.49593961|764,18.17087812,55.24969485|765,18.00645115,54.56948183|766,21.31932253,57.0905606|767,22.87488035,55.3700119|768,21.30395504,57.10308565|769,18.12708984,55.22858588|770,18.31885446,55.10299878|771,18.17233439,55.25086184|772,22.660498,59.403431|773,22.6591,59.4033|774,24.802914,67.117099|775,-13.24553946,-76.29354989|776,13.76221288,121.061897|777,13.719728,121.062528|778,25.907555,51.556395|779,25.904005,51.508995|780,46.62758994,142.9238499|781,46.63023001,142.91098|782,51.41830006,143.36534|783,49.12059636,142.9342851|784,46.63023001,142.91098|785,27.057924,49.59532|786,27.057924,49.59532|787,1.234,103.767|788,1.234,103.767|789,1.308403,103.686828|790,-29.88973099,31.03589279|791,-29.88973099,31.03589279|792,41.3389,2.16096|793,10.17945668,-61.68693382|794,38.805278,26.940833|795,40.747222,29.7675|796,39.743811,33.460581|797,37.88,41.134167|798,50.84050375,-1.356824372|799,54.0943216,-3.179825508|800,57.57679032,-1.839091287|801,57.75776439,-3.945652228|802,52.85991756,1.462504242|803,55.99190552,-3.352115936|804,53.36052425,0.224776692|805,56.03612673,-3.313654044|806,53.65156096,0.114521032|807,57.70768571,-4.028749128|808,58.84040652,-3.114588391|809,53.66089451,0.109827009|810,56.0950619,-3.305443316|811,53.26649777,-2.857321431|812,31.60093316,-103.4368096|813,48.47808,-122.559001|814,30.21905182,-91.0324623|815,30.00197,-90.417082|816,38.010521,-122.082274|817,30.80061889,-88.08094722|818,48.47808,-122.559001|819,29.90653394,-93.92304795|820,30.00197,-90.417082|821,29.715766,-95.130483|822,29.715766,-95.130483|823,30.095503,-90.907509|824,38.22869105,-76.3339497|825,38.010521,-122.082274|826,33.70876935,-85.79625055|827,33.70876935,-85.79625055|828,10.291,-71.389;Experiment=rcp85;TimePeriod=2035&ResponseForm=RawDataOutput"""

        url = """http://wps-web1.ceda.ac.uk/wps?Request=Execute&Identifier=GetClimateStats&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Locations=1,-12.36,124.27|2,-21.98,114.01|3,-21.44,114.07|4,-10.61,125.99|5,-19.59,116.45|6,-19.59,116.14|7,-19.59,116.14|8,-19.65,115.93|9,-19.5,116.6|10,-19.62,116.41|11,-19.56,116.5|12,-21.21,-39.74|13,-22.64,-40.46|14,-22.62,-40.46|15,-22.66,-40.41|16,-22.66,-40.43|17,-22.64,-40.42|18,-22.65,-40.43|19,4.71,114.44|20,4.95,114.07|21,4.93,114.14|22,4.69,114.15|23,5.21,114.74|24,5.21,114.74|25,4.81,113.96|26,4.8,113.97|27,4.8,113.98|28,4.8,113.97|29,4.95,114.07|30,4.84,114.07|31,4.95,114.07|32,4.93,114.14|33,4.95,114.07|34,4.95,114.11|35,4.93,114.14|36,4.95,114.07|37,4.95,114.07|38,4.95,114.07|39,4.92,114.12|40,4.95,114.11|41,4.84,114.07|42,4.84,114.07|43,4.93,114.14|44,4.93,114.14|45,4.68,114.13|46,4.76,114.17|47,4.69,114.15|48,4.69,114.15|49,4.69,114.15|50,4.69,114.15|51,4.74,114.13|52,4.74,114.18|53,4.75,114.18|54,4.69,114.13|55,4.68,114.14|56,4.71,114.16|57,4.71,114.17|58,4.77,114.19|59,4.77,114.19|60,4.65,114.13|61,4.77,114.19|62,4.75,114.14|63,4.77,114.16|64,5.21,114.74|65,5.24,114.75|66,4.69,114.16|67,4.74,114.18|68,4.65,114.13|69,4.73,114.16|70,4.67,114.13|71,4.75,114.15|72,4.78,114.17|73,4.69,114.15|74,4.78,114.18|75,4.74,114.19|76,4.69,114.13|77,4.68,114.15|78,4.68,114.13|79,4.67,114.12|80,4.71,114.16|81,4.71,114.15|82,4.77,114.17|83,4.78,114.19|84,4.77,114.2|85,4.77,114.16|86,4.68,114.11|87,4.74,114.17|88,4.75,114.21|89,5.01,114.04|90,5.01,114.04|91,4.67,114.38|92,4.68,114.39|93,4.68,114.41|94,4.67,114.39|95,4.67,114.39|96,5.29,114.72|97,5.25,114.75|98,4.73,114.15|99,4.68,114.13|100,4.76,114.17|101,4.77,114.19|102,4.69,114.14|103,4.7,114.16|104,4.68,114.12|105,4.76,114.18|106,4.76,114.19|107,4.77,114.16|108,4.84,114.07|109,4.97,114.12|110,5.18,114.74|111,5.25,114.75|112,5.25,114.75|113,5.24,114.75|114,5.24,114.75|115,5.23,114.75|116,5.27,114.73|117,5.25,114.75|118,4.74,114.18|119,4.67,114.38|120,4.99,114.2|121,5.2,114.74|122,4.73,114.15|123,4.66,114.12|124,4.75,114.14|125,4.65,114.36|126,5.24,114.76|127,5.22,114.74|128,5.26,114.74|129,5.2,114.57|130,4.67,114.4|131,4.67,114.38|132,4.68,114.39|133,4.68,114.4|134,4.68,114.39|135,4.68,114.39|136,4.68,114.42|137,4.65,114.37|138,4.67,114.38|139,4.66,114.38|140,4.67,114.38|141,4.62,114.31|142,4.62,114.28|143,4.64,114.33|144,4.66,114.36|145,4.62,114.31|146,4.62,114.28|147,4.67,114.38|148,4.95,114.11|149,5.18,114.75|150,5.21,114.74|151,5.23,114.76|152,5.2,114.74|153,4.65,114.12|154,4.67,114.1|155,4.83,114.44|156,5.21,114.75|157,5.16,114.63|158,5.1,114.45|159,5.09,114.45|160,5.23,114.73|161,5.21,114.74|162,5.21,114.74|163,5.22,114.75|164,5.22,114.74|165,5.22,114.74|166,5.2,114.74|167,5.18,114.73|168,5.24,114.74|169,5.24,114.75|170,5.23,114.75|171,5.23,114.74|172,5.24,114.75|173,5.23,114.75|174,5.19,114.75|175,5.19,114.75|176,5.2,114.75|177,5.19,114.75|178,5.2,114.75|179,5.23,114.74|180,5.21,114.74|181,5.21,114.74|182,5.21,114.74|183,5.21,114.74|184,5.21,114.74|185,5.21,114.74|186,5.21,114.74|187,5.21,114.74|188,5.21,114.74|189,5.21,114.73|190,5.21,114.62|191,4.84,114.56|192,5.14,114.64|193,4.7,114.4|194,5.21,114.63|195,5.33,114.74|196,5.16,114.63|197,5.19,114.75|198,5.11,114.31|199,4.74,114.15|200,5.11,114.44|201,5.1,114.45|202,5.1,114.45|203,5.1,114.45|204,4.76,114.17|205,4.77,114.15|206,4.67,114.11|207,4.76,114.21|208,4.67,114.14|209,4.65,114.12|210,4.75,114.17|211,4.75,114.14|212,4.69,114.14|213,4.68,114.13|214,4.71,114.14|215,4.77,114.15|216,4.78,114.19|217,4.69,114.14|218,4.71,114.15|219,4.77,114.19|220,4.69,114.14|221,4.69,114.14|222,4.71,114.16|223,4.71,114.16|224,4.99,114.2|225,4.81,113.97|226,5.25,114.75|227,5.25,114.76|228,5.25,114.75|229,5.22,114.75|230,5.22,114.74|231,5.22,114.74|232,5.24,114.75|233,5.24,114.75|234,5.24,114.75|235,5.15,114.65|236,5.24,114.75|237,5.18,114.73|238,5.2,114.74|239,5.2,114.74|240,5.2,114.75|241,5.2,114.76|242,5.25,114.76|243,5.25,114.76|244,5.25,114.76|245,5.2,114.76|246,5.18,114.76|247,5.17,114.75|248,5.2,114.77|249,5.22,114.76|250,5.17,114.77|251,5.25,114.73|252,4.2,8.42|253,4.22,8.42|254,4.31,8.67|255,4.24,8.66|256,4.19,8.47|257,4.22,8.38|258,4.2,8.45|259,4.2,8.47|260,4.18,8.48|261,4.18,8.41|262,4.18,8.41|263,4.18,8.4|264,4.19,8.39|265,4.18,8.41|266,5.38,111.86|267,4.69,113.92|268,4.71,113.96|269,4.57,113.82|270,4.61,113.64|271,4.54,113.62|272,4.55,113.63|273,4.55,113.62|274,6.35,115.65|275,6.33,115.65|276,5.01,105.26|277,5.15,111.82|278,5.15,111.82|279,4.3,113.8|280,4.7,113.94|281,4.68,113.85|282,6.33,115.67|283,5.82,104.19|284,5.83,104.16|285,5.83,104.16|286,5.84,104.1|287,3.24,112.71|288,3.25,112.81|289,3.24,112.72|290,3.23,112.7|291,3.24,112.73|292,3.25,112.77|293,3.23,112.73|294,3.25,112.78|295,4.5,113.91|296,4.49,113.91|297,4.49,113.9|298,4.5,113.92|299,6.33,115.65|300,6.33,115.65|301,6.33,115.65|302,6.35,115.68|303,4.8,113.97|304,4.69,113.93|305,4.63,104.81|306,5.21,104.73|307,6.12,105.31|308,4.64,112.3|309,5.31,111.89|310,4.72,113.1|311,5.62,114.9|312,3.6,112.36|313,5.01,105.29|314,4.74,113.74|315,4.74,113.74|316,4.74,113.73|317,4.74,113.73|318,4.74,113.73|319,4.55,113.62|320,4.69,113.86|321,4.68,113.85|322,4.31,112.7|323,5.62,114.88|324,5.62,114.88|325,5.61,114.88|326,5.38,111.86|327,3.44,112.39|328,4.75,113.74|329,4.74,113.73|330,4.75,113.76|331,4.75,113.76|332,4.73,113.75|333,4.75,113.75|334,4.75,113.74|335,4.75,113.75|336,4.75,113.75|337,5.62,114.89|338,5.64,114.9|339,5.6,114.87|340,5.62,114.89|341,5.62,114.88|342,5.61,114.88|343,5.6,114.88|344,5.62,114.89|345,5.62,114.88|346,5.62,114.88|347,5.62,114.88|348,5.62,114.88|349,4.75,104.9|350,3.76,112.07|351,3.78,112.09|352,6.36,115.3|353,4.96,111.93|354,4.33,112.68|355,3.44,112.37|356,3.76,112.07|357,3.76,112.06|358,3.75,112.06|359,5.62,114.99|360,4.41,113.73|361,4.42,113.72|362,4.42,113.72|363,4.42,113.72|364,4.4,113.72|365,4.41,113.72|366,4.41,113.73|367,4.42,113.72|368,4.42,113.72|369,3.44,112.37|370,3.44,112.38|371,3.46,112.45|372,3.45,112.35|373,3.43,112.37|374,3.61,112.41|375,3.61,112.38|376,4.74,113.73|377,4.41,113.72|378,5.5,105.2|379,4.68,113.89|380,4.13,112.36|381,4.33,112.68|382,4.33,112.69|383,5.08,111.75|384,4.7,113.94|385,4.7,113.94|386,5.5,105.21|387,4.64,112.3|388,4.63,112.3|389,4.63,112.3|390,4.64,112.31|391,4.33,112.68|392,4.33,112.68|393,4.33,112.68|394,4.33,112.69|395,4.33,112.68|396,4.33,112.68|397,4.69,112.49|398,4.69,112.49|399,4.68,112.49|400,4.69,112.49|401,5,105.21|402,5,105.25|403,5,105.25|404,5,105.26|405,5,105.25|406,4.3,113.8|407,4.7,113.94|408,4.69,112.77|409,3.44,112.37|410,4.88,104.78|411,5.21,104.73|412,3.85,112.07|413,4.64,113.62|414,4.95,112.53|415,5.55,114.98|416,4.7,113.94|417,5.52,114.97|418,5.31,111.89|419,4.73,113.19|420,6.21,105.03|421,3.79,112.04|422,6.37,103.84|423,3.78,112.03|424,5.23,111.81|425,5.24,104.67|426,5.14,104.79|427,5.83,114.34|428,5.53,104.99|429,4.45,111.72|430,4.41,111.79|431,4.39,111.96|432,4.78,112.27|433,4.73,112.24|434,5.62,114.89|435,5.64,114.9|436,5.6,114.88|437,6.12,105.29|438,5.95,104.2|439,5.96,104.19|440,6.18,104.07|441,6.33,104.02|442,5.96,104.19|443,4.69,112.5|444,4.84,112.4|445,4.73,104.89|446,4.61,104.81|447,4.31,112.73|448,5.21,104.67|449,5.61,103.91|450,5.61,103.91|451,3.8,112.3|452,4.5,111.64|453,4.23,111.9|454,4.74,113.73|455,4.71,113.72|456,4.71,113.73|457,6.29,105.02|458,6.12,105.04|459,4.44,112.73|460,4.41,112.69|461,4.3,112.63|462,4.33,112.68|463,5.79,114.33|464,4.53,112.1|465,5.82,104.18|466,3.44,112.38|467,3.24,112.71|468,3.25,112.81|469,5.53,104.99|470,4.53,113.06|471,4.8,5.36|472,4.76,5.36|473,4.8,5.36|474,4.81,5.35|475,4.53,7.05|476,4.52,4.7|477,5.36,5.32|478,5.41,5.3|479,5.53,4.86|480,5.39,5.3|481,5.4,5.3|482,5.46,5.17|483,5.45,5.17|484,4.26,7.32|485,4.26,7.32|486,4.26,7.33|487,4.45,7.6|488,5.39,5.31|489,5.37,5.33|490,5.38,5.29|491,5.4,5.32|492,5.39,5.31|493,5.42,5.27|494,5.38,5.29|495,5.39,5.28|496,5.39,5.32|497,5.41,5.29|498,5.38,5.31|499,5.38,5.3|500,64.33,7.78|501,64.39,7.96|502,64.34,7.72|503,64.34,7.72|504,64.26,7.73|505,64.26,7.73|506,64.35,7.84|507,64.26,7.73|508,64.35,7.84|509,64.35,7.84|510,63.53,5.39|511,64.31,7.74|512,64.35,7.78|513,63.39,5.31|514,64.4,7.8|515,64.34,7.72|516,63.5,5.39|517,64.36,7.71|518,64.26,7.73|519,63.57,5.35|520,11.65,118.9|521,26.07,51.99|522,26.16,52.07|523,52.82,143.55|524,52.74,143.59|525,52.92,143.54|526,51.47,143.6|527,40.39,0.72|528,53.47,2.92|529,56.95,1.44|530,56.85,1.59|531,57.36,0.82|532,53.45,1.89|533,57.23,1.62|534,53.46,1.73|535,53.46,1.73|536,56.72,1.26|537,57.28,0.85|538,57.29,0.83|539,57.23,0.82|540,57.26,0.81|541,53.43,2.9|542,53.43,2.9|543,57.13,1.99|544,56.72,1.31|545,61.6,1.58|546,61.63,1.5|547,61.61,1.55|548,61.56,1.61|549,61.53,1.66|550,56.74,1.39|551,53.23,2.62|552,53.4,2.66|553,53.44,2.7|554,53.57,1.63|555,61.29,1.47|556,61.29,1.47|557,61.29,1.47|558,57.17,0.78|559,61.31,1.04|560,61.31,1.04|561,58.77,1.49|562,59.32,1.56|563,53.47,1.91|564,56.73,1.3|565,56.9,1.04|566,57.14,1.93|567,60.99,1.68|568,56.92,1.03|569,60.99,1.68|570,57.03,1.96|571,58.25,0.85|572,53.06,2.23|573,52.95,2.15|574,52.97,2.23|575,53.05,2.19|576,57.15,1.8|577,57.15,1.81|578,53.57,1.63|579,61.06,1.71|580,53.23,2.83|581,57.17,1.75|582,57.09,0.85|583,57.09,0.96|584,57.23,1.01|585,53.52,1.8|586,53.7,2.62|587,53.58,2.79|588,58,-0.38|589,57.17,2.26|590,57.14,2.28|591,57.16,2.29|592,60.99,1.68|593,60.99,1.68|594,61.03,1.13|595,57.15,0.93|596,53.11,2.07|597,53.12,2.1|598,53.46,1.73|599,53.46,1.73|600,53.46,1.73|601,53.46,1.73|602,53.09,2.13|603,53.09,2.13|604,53.09,2.13|605,53.08,2.19|606,53.1,2.16|607,53.08,2.18|608,53.08,2.18|609,53.09,2.13|610,53.08,2.19|611,53.08,2.18|612,53.1,2.16|613,53.01,2.19|614,53.19,2.86|615,53.19,2.86|616,53.61,1.53|617,53.61,1.53|618,52.95,2.15|619,57.19,1|620,57.17,0.95|621,57.16,0.98|622,57.14,0.96|623,57.32,1.08|624,61.03,1.71|625,61.1,1.72|626,61.13,1.74|627,61.05,1.76|628,61.05,1.67|629,57.2,0.92|630,57.18,0.92|631,61.28,0.92|632,61.36,1.16|633,61.1,1.07|634,61.16,1.11|635,61.32,1.55|636,61.33,1.55|637,28.17,-89.22|638,29.06,-88.09|639,28.28,-88.73|640,27.79,-90.65|641,28.16,-89.24|642,26.13,-94.9|643,28.15,-89.1|644,27.76,-90.73|645,-13.79,123.32;Experiment=rcp85;TimePeriod=2035&ResponseForm=RawDataOutput"""      


        url = """http://wps-web1.ceda.ac.uk/wps?Request=Execute&Identifier=GetClimateStats&Format=text/xml&Inform=true&Store=false&Status=false&DataInputs=Locations=6,-19.59,116.14|7,-19.59,116.14|8,-19.65,115.93|9,-19.5,116.6|252,4.2,8.42|253,4.22,8.42|254,4.31,8.67|255,4.24,8.66|256,4.19,8.47|257,4.22,8.38|258,4.2,8.45|259,4.2,8.47|260,4.18,8.48|261,4.18,8.41|262,4.18,8.41|263,4.18,8.4|264,4.19,8.39|265,4.18,8.41|472,4.76,5.36|473,4.8,5.36|474,4.81,5.35|477,5.36,5.32|478,5.41,5.3|480,5.39,5.3|481,5.4,5.3|482,5.46,5.17|483,5.45,5.17|484,4.26,7.32|485,4.26,7.32|486,4.26,7.33|487,4.45,7.6|488,5.39,5.31|489,5.37,5.33|490,5.38,5.29|491,5.4,5.32|492,5.39,5.31|493,5.42,5.27|494,5.38,5.29|495,5.39,5.28|496,5.39,5.32|497,5.41,5.29|498,5.38,5.31|499,5.38,5.3|521,26.07,51.99|522,26.16,52.07|524,52.74,143.59|525,52.92,143.54|526,51.47,143.6|527,40.39,0.72|528,53.47,2.92|532,53.45,1.89|534,53.46,1.73|535,53.46,1.73|551,53.23,2.62|552,53.4,2.66|553,53.44,2.7|563,53.47,1.91|570,57.03,1.96|572,53.06,2.23|573,52.95,2.15|574,52.97,2.23|575,53.05,2.19|578,53.57,1.63|580,53.23,2.83|585,53.52,1.8|587,53.58,2.79|588,58,-0.38|596,53.11,2.07|597,53.12,2.1|599,53.46,1.73|600,53.46,1.73|601,53.46,1.73|602,53.09,2.13|603,53.09,2.13|604,53.09,2.13|605,53.08,2.19|606,53.1,2.16|607,53.08,2.18|608,53.08,2.18|609,53.09,2.13|610,53.08,2.19|611,53.08,2.18|612,53.1,2.16|613,53.01,2.19|614,53.19,2.86|615,53.19,2.86|617,53.61,1.53|619,57.19,1|624,61.03,1.71|631,61.28,0.92|632,61.36,1.16|639,28.28,-88.73;Experiment=rcp45;TimePeriod=2055&ResponseForm=RawDataOutput"""

        locs = url.split("&")[-2].split(";")[0].split("=")[2].split("|")

        for loc in locs:
            print loc

        locs = [Location(loc) for loc in locs]
        res = x.extractData("rcp85", "2055", locs)
        sys.exit()

        if type(res) is str:
            print res
        else:
            for key in res:
                print key
                for sect in res[key]["Locations"]:
                    for k in ['RequestedLocations', 'ModelLocation']:
                        print k, "--->"
                        pprint.pprint(sect[k])

    if do_test_regional: 
        for loc_string in """regNone,-21.9,-41.9 regAFR_MNA,0.0,0.0 regAFR,-14.5,25.7 regARC,84.0,18.5 regEUR,46,2.5 regMNA,44,70 regNAM,40,-100""".split():
            loc = Location(loc_string)
            res = x.extractFullSummaryCSV(loc)
            print "LOCATION:", loc
            print res
            print "--------\n" * 5 
            print "\n\n\n"
