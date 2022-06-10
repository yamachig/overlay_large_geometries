# `overlay_large_geometries`: Fast and efficient overlay of large geometries

This package performs [overlay](https://geopandas.org/en/stable/docs/user_guide/set_operations.html) (such as intersection or union) of two sets of geometries fast and efficiently, utilizing [grid indexing](https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/) and parallel computation.

## Getting started

You can download the source codes and create a conda environment by the following commands.

```bash
git clone git@github.com:yamachig/overlay_large_geometries.git
cd overlay_large_geometries
conda env create -n overlay_large_geometries -f environment.yml
conda activate overlay_large_geometries
```

## How to use

### 1. General overlay

You can use the `python overlay_large_geometries` command to take an overlay of two geometries. The format of the geometries can be one that [`geopandas.read_file`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html) accepts or a [`Parquet`](https://geopandas.org/en/stable/docs/reference/api/geopandas.read_parquet.html) file.

The resulting geometry will be saved as `overlay.parquet` in the specified destination directory. Please note that the overlayed geometry is split into grid cells, that can be identified by the column `cell_id`.

```
> python overlay_large_geometries -h

usage: overlay_large_geometries [-h] [--dest-dir DEST_DIR] [--extent-path EXTENT_PATH]
                                [--left-path LEFT_PATH] [--right-path RIGHT_PATH] [--how HOW]
                                [--left-columns LEFT_COLUMNS [LEFT_COLUMNS ...]]
                                [--right-columns RIGHT_COLUMNS [RIGHT_COLUMNS ...]]
                                [--longitude-range LONGITUDE_RANGE]
                                [--latitude-range LATITUDE_RANGE]

Process overlay of geometries.

options:
  -h, --help            show this help message and exit
  --dest-dir DEST_DIR   Destination directory for dataset
  --extent-path EXTENT_PATH
                        Path to extent geometries file
  --left-path LEFT_PATH
                        Path to left geometries file
  --right-path RIGHT_PATH
                        Path to right geometries file
  --how HOW             Type of overlay to perform. Please see the 'how' parameter of
                        'GeoDataFrame.overlay()' at https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.overlay.html
  --left-columns LEFT_COLUMNS [LEFT_COLUMNS ...]
                        Columns to group left geometries
  --right-columns RIGHT_COLUMNS [RIGHT_COLUMNS ...]
                        Columns to group right geometries
  --longitude-range LONGITUDE_RANGE
                        Longitude range to filter (ex. '-115:-118')
  --latitude-range LATITUDE_RANGE
                        Latitude range to filter (ex. '32:35')
```

### 2. Downloading FCC Form 477 datasets

This package is originally designed to process the [Form 477 dataset from FCC](https://www.fcc.gov/mobile-deployment-form-477-data). You can use the `python overlay_large_geometries/fcc_f477` command to download the datasets containing the following geometries.

- [Form 477 from FCC](https://www.fcc.gov/mobile-deployment-form-477-data)
- [U.S. ZIP code tabulated area (ZCTA) boundaries from the Census Bureau](https://catalog.data.gov/dataset/tiger-line-shapefile-2019-2010-nation-u-s-2010-census-5-digit-zip-code-tabulation-area-zcta5-na)
- [U.S. State boundaries from the Census Bureau](https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html)

Before running the command, you have to download a [Box keypair JSON](https://developer.box.com/guides/authentication/jwt/jwt-setup/) and save it as `.box.json` at the root directory of the package.

Unless specified `--download-only`, the command calculates the mobile coverage ratio for each zipcode and saves the data as `coverage.csv` in the specified destination directory.

```
> python overlay_large_geometries/fcc_f477 -h

usage: fcc_f477 [-h] [--dest-dir DEST_DIR] [--periods PERIODS [PERIODS ...]]
                [--mnos MNOS [MNOS ...]] [--techcodes TECHCODES [TECHCODES ...]]
                [--download-only DOWNLOAD_ONLY]

Download and process FCC Form 477

options:
  -h, --help            show this help message and exit
  --dest-dir DEST_DIR   Destination directory for dataset
  --periods PERIODS [PERIODS ...]
                        Periods to download (ex. '2020-06 2019-12')
  --mnos MNOS [MNOS ...]
                        MNOs to download (ex. 'AT_T_Mobility VerizonWireless')
  --techcodes TECHCODES [TECHCODES ...]
                        Techcodes to download (ex. '83')
  --download-only DOWNLOAD_ONLY
                        Download only, do not process
```

## Methodology

Taking an overlay of geometries needs to find intersecting geometries for each geometry. Therefore, naively taking an overlay of massive and complex geometries will result in an exponential computation time.

To optimize the intersection calculation, this package adopted a [spatial indexing approach using a grid](https://snorfalorpagus.net/blog/2016/03/13/splitting-large-polygons-for-faster-intersections/). This method splits each geometry into a globally indexed grid so that geometries in a particular grid cell can be computed independently of other grid cells. We can drastically reduce the number of involved geometries considered for calculating overlapping with indexing with the grid.

When applying the grid, there would sometimes be extremely large polygons in the dataset that prevent `GeoDataframe.overlay()` with the grid cells from finishing in a reasonable time. Such large polygons typically contain a large number of interiors (holes), and we cannot simply cut the polygon (the computation is equivalent to applying the grid). Therefore, in this package, we manipulated the coordinates in the polygon separately and applied the grid as follows. First, we extracted the interiors as a set of polygons. Second, we took the overlay (identity) of the interiors with the grid, leveraging the spatial indexing. Third, we took the overlay (identity) of the exterior (contour) of the original large polygon. Finally, for each grid cell, we took the difference between the gridded exterior and the gridded interiors.

To utilize parallel computing to leverage the full computing resource, we assign the grid cells into separate threads and call `GeoDataframe.overlay()` in each thread because the original `GeoDataframe.overlay()` does not run in parallel on its own.

## Performance

You can test the performance with the command `python tests/test_basic.py`.

The performance depends on the dataset and computation environment. For example, with an environment below, the proposed method ran about 50 times as fast as the naive method (applying `GeoDataframe.overlay` without any preprocessing).

```bash
> python tests/test_basic.py

... omitted output ...

TEST RESULT
===========

- total time: 22 sec
- total time (naive): 1029 sec

- mismatch rows: 0.0%
- area error rate: 8.3e-08

- CPU freq: scpufreq(current=3600.0, min=0.0, max=3600.0)
- CPU count: logical: 20; physical: 12
- Total RAM: 31.75GB
```
