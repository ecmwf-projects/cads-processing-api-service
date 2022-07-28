# Adaptation design

Long-term implicit adapter code:

```python
import cads


@cads.cacheable
def adapter(request, config, metadata):
    """Implicit MARS adapter"""

    # parse input options
    with cads.add_step_metrics("process inputs", metadata):
        request, format = cads.extract_format_options(request, config)
        request, reduce = cads.extract_reduce_options(request, config)
        mars_request = cads.map_to_mars_request(request, config)

    # retrieve data
    with cads.add_step_metrics("download data", metadata):
        data = cads.mars_retrieve(mars_request)

    # post-process data
    if reduce is not None:
        with cads.add_step_metrics("reduce data", metadata):
            data = cads.apply_reduce(data, reduce)

    if format is not None:
        with cads.add_step_metrics("reformat data", metadata):
            data = cads.translate(data, format)

    return data

```

Short-term simple adapter code:

```python
import cacholote
import cadsapi
import cdscdm
import xarray as xr


@cacholote.cacheable
def adapter(request, config, metadata):

    # parse input options
    collection_id = request.pop("collection_id", None)
    if collection_id:
        raise ValueError(f"collection_id is required in request")
    data_format = request.pop("format", "grib")
    if data_format not in {"netcdf", "grib"}:
        raise ValueError(f"{data_format=} is not supported")

    # retrieve data
    client = cdsapi.Client()
    client.retrieve(collection_id, request, "download.grib")  # TODO
    data = xr.open_dataset("download.grib")

    # post-process data
    if data_format == "netcdf":
        data = cdscdm.open_dataset("download.grib")

    return data

```

The compute service would look like:

```python

def run_code(
    setup_code: str,
    entry_point: str,
    kwargs: Dict[str, Any],
    metadata: Dict[str, Any]
) -> Dict[str, Any]:
    exec(setup_code)
    return eval(f"{entry_point}(**kwargs, metadata=metadata)")

```