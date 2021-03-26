"""
get_data_at_point.py
====================

Script to quickly extract data for a certain location.

Usage: e.g.:

    python processes/local/GetClimateStats/get_data_at_point.py 2035 rcp45 BCC/bcc-csm1-1-m Global  dailymin 0 0 

"""

import os, sys, re, glob

import .axis_utils
from roocs_utils.xarray_utils.xarray_utils import get_coord_by_type

from processes.local.GetClimateStats.lib import *

fmt = "%8.5f"


def main(args):

    print("(time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon)")
    (time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon) = args
    print("Extracting:", time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon)
    location = Location("ASSETID,%s,%s" % (lat, lon))
    x = ClimateStatsExtractor()
    data = x.extractDataAtPoint(domain_type, inst_model, experiment, time_period, var_id, statistic, location)

    v = data["values"]

    for (i, m) in enumerate("jan feb mar apr may jun jul aug sep oct nov dec ann".split()):
        try:
            print(("%s:  " + fmt) % (m, v[i]))
        except Exception as err:
            print("NO DATA?", v)
            raise Exception(err)

    print("\nLocation: %s, %s" % location.global_gb)

    print( "\nTrying independent verification via file...")
    verify(time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon)



def _getNearestLatLon(lat, lon, da):
    if len(da.shape) != 3:
        raise Exception(f'3 axes expected in DataArray but not found.')

    glat = axis_utils.nudgeSingleValuesToAxisValues(lat, get_coord_by_type(da, 'latitude'))
    glon = axis_utils.nudgeSingleValuesToAxisValues(lon, get_coord_by_type(da, 'longitude'))
    return (glat, glon)

def verify(time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon, baseline = False):
    model = inst_model.split("/")[1]
    dtl = domain_type.lower()
    data = []

    if baseline:
        items = ["stats"]
        experiment = "historical"
        time_period = "1995"
    else: 
        verify(time_period, experiment, inst_model, domain_type, var_id, statistic, lat, lon, baseline = True)
        items = ["stats", "diffs"]
        if domain_type == "Global":
            items.append("1_deg")
        else:
            items.append("0.5_deg")

    dirs = []

    for item in items:
        data.append([])
        dirs.append(item)

    for (a, dr) in enumerate(dirs):
 
      for (i, tmp) in enumerate(("mon", "ann")):
        pattern = "/gws/nopw/j04/acclim/outputs/data/%(dtl)s/%(var_id)s/%(experiment)s/%(inst_model)s/%(time_period)s/%(dr)s/%(var_id)s_*%(model)s*%(experiment)s_r*_*_%(tmp)s_%(statistic)s*.nc" % vars()
        try:
            fn = glob.glob(pattern)[0] 
        except Exception as exc:
            print("Check: %s" % pattern)
            raise Exception(exc)

        print(fn)
        da = xr.open_dataset(fn, use_cftime=True)[var_id]
#        vm = f[var_id]

        y, x = _getNearestLatLon(float(lat), float(lon), da)
#        ax_ids = (vm.getAxisIds()[1], vm.getAxisIds()[2])
#        kwargs = {ax_ids[0]: y, ax_ids[1]: x, "squeeze": 1}
#        v = f(var_id, **kwargs)

        lat_id = get_coord_by_type(da, 'latitude')
        lon_id = get_coord_by_type(da, 'longitude')

        v = da.loc[{lat_id: y, lon_id: x}]

        if tmp == "mon":
            data[a].extend([v[i] for i in range(12)])
        else:
            data[a].append(v)

        print("Dir: %s;  Point: y = %8.5f, x = %8.5f" % (dr, y, x))

    print("Month/Ann stats\t\tdiffs\t\t1_deg")
    for (i, m) in enumerate("jan feb mar apr may jun jul aug sep oct nov dec ann".split()):
        s = ("%02d:\t" + fmt) % (i, data[0][i])
        for dt in data[1:]:
            s += ("\t" + fmt) % dt[i]

        print(s)
    

if __name__ == "__main__":

    main(sys.argv[1:])
