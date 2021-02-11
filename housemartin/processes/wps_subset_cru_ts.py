from daops.ops.subset import subset

from pywps import LiteralInput, Process, FORMATS, Format, ComplexOutput
from pywps.app.Common import Metadata
from pywps.app.exceptions import ProcessError

from ..utils.input_utils import parse_wps_input
from ..utils.metalink_utils import build_metalink
from ..utils.response_utils import populate_response


class SubsetCRUTS(Process):
    def __init__(self):
        inputs = [
            LiteralInput(
                "dataset_version",
                "Dataset Version",
                abstract="Example: cru_ts.4.04",
                data_type="string",
                min_occurs=1,
                max_occurs=1,
            ),
            LiteralInput(
                "variable",
                "Variable",
                abstract="Example: tmn",
                data_type="string",
                min_occurs=1,
            ),
            LiteralInput(
                "time",
                "Time Period",
                abstract="The time period to subset over separated by /"
                "Example: 1960-01-01/2000-12-30",
                data_type="string",
                min_occurs=0,
                max_occurs=1,
            ),
            LiteralInput(
                "area",
                "Area",
                abstract="The area to subset over as 4 comma separated values."
                "Example: 0.,49.,10.,65",
                data_type="string",
                min_occurs=0,
                max_occurs=1,
            ),
        ]

        outputs = [
            ComplexOutput(
                "output",
                "METALINK v4 output",
                abstract="Metalink v4 document with references to NetCDF files.",
                as_reference=True,
                supported_formats=[FORMATS.META4],
            ),
            ComplexOutput(
                "prov",
                "Provenance",
                abstract="Provenance document using W3C standard.",
                as_reference=True,
                supported_formats=[FORMATS.JSON],
            ),
            ComplexOutput(
                "prov_plot",
                "Provenance Diagram",
                abstract="Provenance document as diagram.",
                as_reference=True,
                supported_formats=[
                    Format("image/png", extension=".png", encoding="base64")
                ],
            ),
        ]

        super(SubsetCRUTS, self).__init__(
            self._handler,
            identifier="subset_cru_ts",
            title="Subset CRU_TS",
            abstract="Run subsetting on cru ts data",
            version="1.0",
            inputs=inputs,
            outputs=outputs
        )


    def _handler(self, request, response):
        dataset_version = parse_wps_input(request.inputs, 'dataset_version', must_exist=True)
        variable = parse_wps_input(request.inputs, 'variable', must_exist=True)

        collection = f'{dataset_version}.{variable}'

        inputs = {
            "collection": collection,
            "time": parse_wps_input(request.inputs, 'time', default=None),
            "area": parse_wps_input(request.inputs, 'area', default=None),
            "apply_fixes": False,
            "output_dir": self.workdir,
            "file_namer": "simple",
            "output_type": "netcdf"
        }

        output_uris = subset(**inputs).file_uris

        ml4 = build_metalink(
            "subset-cru_ts-result",
            "Subsetting result as NetCDF files.",
            self.workdir,
            output_uris
        )

        populate_response(response, "subset", self.workdir, inputs, collection, ml4)

        return response
