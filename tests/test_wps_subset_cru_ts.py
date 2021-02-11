from pywps import Service
from pywps.tests import client_for, assert_response_success, assert_process_exception

from .common import get_output, PYWPS_CFG
from housemartin.processes.wps_subset_cru_ts import SubsetCRUTS


def test_wps_subset_cru_ts(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=cru_ts.4.04;variable=wet;time=1951-01-01/2005-12-15;area=1,1,300,89"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=subset_cru_ts&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]
