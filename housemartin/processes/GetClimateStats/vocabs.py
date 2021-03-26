"""
vocabs.py
=========


"""


class Vocabs(object):
    "A container class for the list of available variables."

    variable_content = """tas,Temperature: daily mean,degC
tasmax,Temperature: daily max,degC
tasmin,Temperature: daily min,degC
pr,Rainfall: daily,%
psl,Surface pressure: daily,hPa
huss,Humidity: daily,%
sfcWind,Wind speed: daily,m/s
sfcWindmax,Wind speed: daily max,m/s
tos,Sea temperature: daily,degC
sfcWindDir,Wind direction: daily,degrees
zos,Sea Surface Height Above Geoid: monthly,mm""".split("\n")

    global_models_per_variable_string = """tas  14      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR               MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
tasmax  15      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
tasmin  15      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
pr      15      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
psl     15      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
huss    13      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2                   CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES                         NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5
sfcWind     11      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR               MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                                        NOAA-GFDL/GFDL-ESM2M
sfcWindDir     11      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS           CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR               MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                                        NOAA-GFDL/GFDL-ESM2M
sfcWindmax      8               BNU/BNU-ESM             CMCC/CMCC-CMS                   CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0               IPSL/IPSL-CM5A-MR               MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                                        NOAA-GFDL/GFDL-ESM2M
tos     15      BCC/bcc-csm1-1-m        BNU/BNU-ESM     CCCma/CanESM2   CMCC/CMCC-CMS   CNRM-CERFACS/CNRM-CM5   CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR                       MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR                        NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M
zos     17      BCC/bcc-csm1-1-m                CCCma/CanESM2   CMCC/CMCC-CMS   CNRM-CERFACS/CNRM-CM5   CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR        NASA-GISS/GISS-E2-R     NASA-GISS/GISS-E2-R-CC  NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5""".split("\n")

    all_global_models = "BCC/bcc-csm1-1-m	BNU/BNU-ESM	CCCma/CanESM2   CMCC/CMCC-CMS   CNRM-CERFACS/CNRM-CM5   CSIRO-BOM/ACCESS1-3     CSIRO-QCCCE/CSIRO-Mk3-6-0       INM/inmcm4      IPSL/IPSL-CM5A-MR       LASG-CESS/FGOALS-g2     MOHC/HadGEM2-ES MPI-M/MPI-ESM-MR        NASA-GISS/GISS-E2-R     NASA-GISS/GISS-E2-R-CC  NCAR/CCSM4      NCC/NorESM1-M   NOAA-GFDL/GFDL-ESM2M    NSF-DOE-NCAR/CESM1-CAM5".split()

    regional_inst_models = ('ICHEC-EC-EARTH/AFR-44', 'ICHEC-EC-EARTH/ARC-44', 'ICHEC-EC-EARTH/EUR-44', 'ICHEC-EC-EARTH/MNA-44', 'ICHEC-EC-EARTH/NAM-44')
    regional_variables = "tas tasmax tasmin pr psl huss sfcWind sfcWindDir sfcWindmax".split()

    stats = "avg min max var 01p 05p 95p 99p".split()
    reduced_stats = "avg min max".split()
    meaning_periods = "jan feb mar apr may jun jul aug sep oct nov dec ann".split()

    vital_statistics = "tas:avg tas:99p tas:01p tasmax:avg tasmax:99p tasmax:01p tasmin:avg tasmin:99p tasmin:01p pr:avg pr:99p pr:01p psl:avg psl:99p psl:01p huss:avg huss:99p huss:01p sfcWind:avg sfcWind:99p sfcWind:01p sfcWindmax:avg sfcWindmax:99p sfcWindmax:01p tos:avg sfcWindDir:avg zos:avg".split()

    def __init__(self):
        "Build a dictionary of mappings."
        self.variables = {}
        self.var_keys = []

        for line in self.variable_content:
            id, name, units = line.strip().split(",")
            self.var_keys.append(id)
            self.variables[id] = (name, units)

        self._setupModelMappings()
        self._setupVariableMappings()

    def _setupModelMappings(self):
        """
        Creates ``self.models_by_variable`` dictionary.
        """
        self.models_by_variable = {}
        self.global_models = []

        for domain_type in ("Global", "Regional"):

            self.models_by_variable.setdefault(domain_type, {})

            if domain_type == "Global":

                for line in self.global_models_per_variable_string:

                    items = line.strip().split()
                    var_id = items[0]

                    inst_models = items[2:]
                    self.models_by_variable[domain_type][var_id] = inst_models
        
                    for model in inst_models:

                        if model not in self.global_models:
                            self.global_models.append(model)

                self.global_models.sort()

            else:
                for var_id in self.regional_variables:
                    self.models_by_variable[domain_type][var_id] = self.regional_inst_models 

    def _setupVariableMappings(self):
        """
        Creates ``self.variables_by_model`` dictionary.
        """
        self.variables_by_model = {}

        for domain_type in ("Global", "Regional"):

            self.variables_by_model.setdefault(domain_type, {})

            if domain_type == "Global":

                for line in self.global_models_per_variable_string:

                    items = line.strip().split()
                    var_id = items[0]
                    inst_models = items[2:]

                    for inst_model in inst_models:

                        self.variables_by_model[domain_type].setdefault(inst_model, [])
                        self.variables_by_model[domain_type][inst_model].append(var_id) 
            else:

                for inst_model in self.regional_inst_models:
                    self.variables_by_model[domain_type][inst_model] = self.regional_variables 

    def getModelList(self, domain_type, var_id):
        """
        Returns a list of models for a given domain_type and variable.
        """
        return self.models_by_variable[domain_type][var_id]

    def getAllModels(self, domain_type):
        "Returns a list of all models that could be available for domain type."
        if domain_type == "Global":
            return self.all_global_models
        else:
            return self.regional_inst_models  

    def getVariableList(self, domain_type, inst_model):
        """
        Returns a list of var_ids for a given domain_type and variable.
        """
        return self.variables_by_model[domain_type][inst_model]

    def getStatsList(self, var_id):
        """
        Return a list of statistics available for each variable.
        """
        if var_id in ("sfcWindDir", "zos"):
            return self.reduced_stats
        else:
            return self.stats

    def getStatisticIds(self, domain_type):
        """
        Returns a list of statistic ids <var_id>:<stat> for domain type.
        """
        stats = self.vital_statistics[:]
        if domain_type == "Regional":
            stats = [stat for stat in stats if stat.split(":")[0] not in ("tos", "zos")]

        return stats 


vocabs = Vocabs()

