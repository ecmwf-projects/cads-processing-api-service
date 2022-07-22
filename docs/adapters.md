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
def adapter(collection_id, request, metadata):

    # parse input options
    format = request.pop("format", "grib")
    if format not in {"netcdf", "grib"}:
        raise ValueError(f"{format=} is not supported")

    # retrieve data
    client = cdsapi.Client()
    client.retrieve(collection_id, request, "download.grib")  # TODO
    data = xr.open_dataset("download.grib")

    # post-process data
    if format == "netcdf":
        data = cdscdm.open_dataset("download.grib")

    return data
```
