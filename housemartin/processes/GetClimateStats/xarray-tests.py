import xarray as xr

da = xr.DataArray([1, 2, 3], [("x", [0, 1, 2])])
x = da.sel(x=[1.1, 1.9], method="nearest")

ds = xr.Dataset(data_vars={'tas': da})
y = ds.sel(x=1.9, method="nearest")



def find_nearest(array, value):
    n = [abs(i-value) for i in array]
    idx = n.index(min(n))
    return array[idx]


def nudgeSingleValuesToAxisValues(value, array):
    return find_nearest(value, array)
