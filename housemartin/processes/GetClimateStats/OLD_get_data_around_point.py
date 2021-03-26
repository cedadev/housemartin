from cdms_utils.cdms_compat import *
import sys, re, os


def extract_data_around_point(fpath, lat, lon):
    "Extracts 9 points centred on lat/lon."
    lat = float(lat)
    lon = float(lon)

    f = cdms.open(fpath)
    var_id = os.path.split(fpath)[1].split("_")[0]

    vm = f[var_id]
    assert len(vm.getAxisIds()) == 3

    v = f(var_id, time=slice(0, 1), squeeze=1)
    axes = vm.getAxisList()
    print "Axis ids: ", [ax.id for ax in axes]
    y_ax, x_ax = [round(float(i), 4) for i in axes[1]], [float(i) for i in axes[2]]

    if lat not in y_ax:
        print "FIND y in y axis and re-submit: ", y_ax, lat
        sys.exit()

    if lon not in x_ax:
        print "FIND x in x axis and re-submit: ", x_ax, lon

    yi = y_ax.index(float(lat))
    xi = x_ax.index(float(lon))

    for y in (yi - 1, yi, yi + 1):
        for x in (xi - 1, xi, xi + 1):
            print "%.3f" % v[y][x], "        ",  y, x

    print "Second check...all time steps."
    v = f(var_id, **{axes[1].id: lat, axes[2].id: lon, "squeeze": 1})
    print  "%.3f, %.3f" % (min(v), max(v))
    
if __name__ == "__main__":

    args = sys.argv[1:]
    extract_data_around_point(*args)
