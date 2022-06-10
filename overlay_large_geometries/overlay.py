from random import seed

seed(10)
import traceback
from colorama import Fore, Back, Style
from matplotlib import pyplot as plt
import psutil
import shapely.errors
from shapely.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    GeometryCollection,
)

from typing import Callable
from tqdm import tqdm

from pathlib import Path

from overlay_large_geometries.common import (
    CustomThreadPoolExecutor,
    with_time,
)

import geopandas as gpd
import pandas as pd
from datetime import datetime
import warnings
import time

import pygeos


def keep_polygon(
    geometry: Point
    | MultiPoint
    | LineString
    | MultiLineString
    | Polygon
    | MultiPolygon
    | GeometryCollection,
):
    if geometry.is_empty:
        return Polygon([])
    elif geometry.geom_type in ["Point", "MultiPoint", "LineString", "MultiLineString"]:
        return Polygon([])
    elif geometry.geom_type in ["Polygon", "MultiPolygon"]:
        return geometry
    elif geometry.geom_type in ["GeometryCollection"]:
        geoms = list(map(keep_polygon, geometry.geoms))
        polygons = []
        for geom in geoms:
            if geom.is_empty:
                continue
            elif geom.geom_type in ["Polygon"]:
                polygons.append(geom)
            elif geom.geom_type in ["MultiPolygon"]:
                polygons.extend(geom.geoms)
            else:
                print(Fore.MAGENTA)
                print(geom)
                print(Style.RESET_ALL)
                raise ValueError(f"{geom.geom_type} is not supported")
        if len(polygons) == 0:
            return Polygon([])
        elif len(polygons) == 1:
            return polygons[0]
        else:
            return MultiPolygon(polygons)
    else:
        print(Fore.MAGENTA)
        print(geometry)
        print(Style.RESET_ALL)
        raise ValueError(f"{geometry.geom_type} is not supported")


def _in_cell_overlay_worker(params):
    input_gdfs: list[gpd.GeoDataFrame] = params["input_gdfs"]
    cell_id = params["cell_id"]
    how = params["how"]
    ts = {}

    try:

        with with_time("worker", ts):
            initial_gdf, *other_gdfs = input_gdfs

            agg_gdf = initial_gdf[initial_gdf["cell_id"] == cell_id].drop(
                columns=["cell_id"]
            )
            with with_time("worker -> keep_polygon", ts):
                is_not_polygon = ~agg_gdf.geom_type.isin(["Polygon", "MultiPolygon"])
                if is_not_polygon.any():
                    agg_gdf.geometry.loc[is_not_polygon] = agg_gdf.geometry.loc[
                        is_not_polygon
                    ].apply(keep_polygon)
                    agg_gdf = agg_gdf[~agg_gdf.geometry.is_empty]

            with with_time("worker -> set_precision", ts):
                agg_gdf.geometry = pygeos.set_precision(
                    agg_gdf.geometry.values.data, 0.000001
                )

            for other_gdf in other_gdfs:
                gdf: gpd.GeoDataFrame = other_gdf[other_gdf["cell_id"] == cell_id].drop(
                    columns=["cell_id"]
                )

                with with_time("worker -> keep_polygon", ts):
                    is_not_polygon = ~gdf.geom_type.isin(["Polygon", "MultiPolygon"])
                    if is_not_polygon.any():
                        gdf.geometry.loc[is_not_polygon] = gdf.geometry.loc[
                            is_not_polygon
                        ].apply(keep_polygon)
                        gdf = gdf[~gdf.geometry.is_empty]

                with with_time("worker -> set_precision", ts):
                    gdf.geometry = pygeos.set_precision(
                        gdf.geometry.values.data, 0.000001
                    )

                with with_time("worker -> make_valid", ts):
                    invalid = ~agg_gdf.is_valid
                    if invalid.any():
                        agg_gdf.loc[invalid, "geometry"] = agg_gdf.loc[
                            invalid, "geometry"
                        ].buffer(0)
                    invalid = ~gdf.is_valid
                    if invalid.any():
                        gdf.loc[invalid, "geometry"] = gdf.loc[
                            invalid, "geometry"
                        ].buffer(0)

                with with_time("worker -> overlay", ts):
                    # assert gdf.is_valid.all()
                    # assert agg_gdf.is_valid.all()
                    try:
                        try:
                            agg_gdf: gpd.GeoDataFrame = agg_gdf.overlay(
                                gdf,
                                how=how,
                                keep_geom_type=False,
                                make_valid=False,
                            )
                        except shapely.errors.TopologicalError as e:
                            if (
                                "This operation could not be performed. Reason: unknown"
                                in str(e)
                            ):
                                print("\r\n======================\r\n")
                                print(f"[{datetime.now()}]")
                                print(e)
                                print(f"_in_cell_overlay_worker({cell_id=}, {how=})")
                                print(
                                    f"CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
                                )
                                print(f"Retrying with exploded geometries")
                                print("\r\n======================\r\n")

                                gdf = gdf.explode(ignore_index=True)

                                print("\r\n======================\r\n")
                                print(
                                    f"agg_gdf: len={agg_gdf.shape[0]} invalid={agg_gdf[~agg_gdf.is_valid].shape[0]}"
                                )
                                print(agg_gdf.head(10))
                                print(
                                    agg_gdf[
                                        agg_gdf.columns[agg_gdf.columns != "geometry"]
                                    ].value_counts(dropna=False)
                                )
                                print("\r\n======================\r\n")
                                print(
                                    f"exploded gdf: len={gdf.shape[0]} invalid={gdf[~gdf.is_valid].shape[0]}"
                                )
                                print(gdf.head(10))
                                print(
                                    gdf[
                                        gdf.columns[gdf.columns != "geometry"]
                                    ].value_counts(dropna=False)
                                )
                                print("\r\n======================\r\n")

                                agg_gdf: gpd.GeoDataFrame = agg_gdf.overlay(
                                    gdf,
                                    how=how,
                                    keep_geom_type=False,
                                    make_valid=False,
                                )
                                dissolve_columns = agg_gdf.columns[
                                    agg_gdf.columns != "geometry"
                                ].tolist()
                                agg_gdf: gpd.GeoDataFrame = agg_gdf.dissolve(
                                    by=dissolve_columns,
                                    dropna=False,
                                ).reset_index()

                                print("\r\n======================\r\n")
                                print(f"[{datetime.now()}]")
                                print(
                                    f" - Succeeded: retrying with exploded geometries"
                                )
                                print(f" - Dissolved by: {dissolve_columns}")
                                print(f" - _in_cell_overlay_worker({cell_id=}, {how=})")
                                print(
                                    f"CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
                                )
                                print(
                                    f"agg_gdf: len={agg_gdf.shape[0]} invalid={agg_gdf[~agg_gdf.is_valid].shape[0]}"
                                )
                                print(agg_gdf.head(10))
                                print(
                                    agg_gdf[
                                        agg_gdf.columns[agg_gdf.columns != "geometry"]
                                    ].value_counts(dropna=False)
                                )
                                print("\r\n======================\r\n")

                            else:
                                raise e
                    except Exception as e:
                        print("\r\n\r\n\r\n")
                        print(Fore.RED)
                        print(e)
                        print(Style.RESET_ALL)
                        traceback.print_exc()
                        print("\r\n\r\n")
                        print("\r\n======================\r\n")
                        print(f"[{datetime.now()}]")
                        print(f"_in_cell_overlay_worker({cell_id=}, {how=})")
                        print(
                            f"CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
                        )
                        print("\r\n======================\r\n")
                        print(
                            f"agg_gdf: len={agg_gdf.shape[0]} invalid={agg_gdf[~agg_gdf.is_valid].shape[0]}"
                        )
                        print(agg_gdf.head())
                        print("\r\n======================\r\n")
                        print(
                            f"gdf: len={gdf.shape[0]} invalid={gdf[~gdf.is_valid].shape[0]}"
                        )
                        print(gdf.head())
                        print("\r\n======================\r\n")
                        raise e

                with with_time("worker -> keep_polygon", ts):
                    is_not_polygon = ~agg_gdf.geom_type.isin(
                        ["Polygon", "MultiPolygon"]
                    )
                    if is_not_polygon.any():
                        agg_gdf.geometry.loc[is_not_polygon] = agg_gdf.geometry.loc[
                            is_not_polygon
                        ].apply(keep_polygon)
                        agg_gdf = agg_gdf[~agg_gdf.geometry.is_empty]

                del gdf

            agg_gdf["cell_id"] = cell_id
            agg_gdf = agg_gdf[
                ["cell_id"] + agg_gdf.columns[agg_gdf.columns != "cell_id"].tolist()
            ]

    except Exception as e:
        print(f"\r\n\r\n{e}")
        print(f"_in_cell_overlay_worker({cell_id=}, {how=})")
        traceback.print_exc()
        raise e

    return ts, agg_gdf


def get_input_gdfs(
    *,
    input_parquet_paths: list[Path],
    inindex_column_names: list[str] | None = None,
):
    if inindex_column_names is None:
        inindex_column_names = [None] * len(input_parquet_paths)
    gdfs = []
    all_cell_ids = set()
    for (parquet_path, inindex_column_name) in tqdm(
        list(zip(input_parquet_paths, inindex_column_names))
    ):
        gdf = gpd.read_parquet(parquet_path, use_threads=False)
        if "cell_id" not in gdf.columns:
            gdf = gdf.reset_index()
        if inindex_column_name:
            gdf[inindex_column_name] = True
        gdfs.append(gdf)
        cell_ids = gdf["cell_id"].unique().tolist()
        all_cell_ids.update(cell_ids)
    return gdfs, list(all_cell_ids)


import gc


def overlay(
    *,
    input_parquet_paths: list[Path],
    inindex_column_names: list[str] | None = None,
    how: str,
) -> gpd.GeoDataFrame:

    assert (inindex_column_names is None) or (
        len(inindex_column_names) == len(input_parquet_paths)
    )

    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )
    print(f"[{datetime.now()}] - Loading parquets...")

    input_gdfs, cell_ids = get_input_gdfs(
        input_parquet_paths=input_parquet_paths,
        inindex_column_names=inindex_column_names,
    )
    print(
        f"[{datetime.now()}] - {len(cell_ids)} cell ids from {len(input_parquet_paths)} files..."
    )
    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    input_gdf_crs = input_gdfs[0].crs
    print(f"[{datetime.now()}] - {input_gdf_crs.name = }...")

    params_generator = (
        {
            "input_gdfs": input_gdfs,
            "cell_id": cell_id,
            "how": how,
        }
        for cell_id in cell_ids
    )

    print(f"[{datetime.now()}] - Overlaying...")

    ts_executor = {}
    ts_worker = {}
    gdfs = []
    with with_time("executor", ts_executor):

        with with_time("executor -> gc", ts_executor):
            gc.collect()

        with CustomThreadPoolExecutor() as executor:
            map_generator = executor.race_map(
                _in_cell_overlay_worker,
                params_generator,
            )
            bar = tqdm(map_generator, total=len(cell_ids), colour="cyan")
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)
            t_wait_0 = time.time()
            # t_yappi_start = time.time()
            print(
                f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
            )
            for i, (part_ts, agg_gdf) in enumerate(bar):
                bar.set_postfix(
                    {
                        "thds": len(executor._threads),
                        "CPU": f"{psutil.cpu_percent():.0f}%",
                        "RAM": f"{psutil.virtual_memory()[2]:.0f}%",
                    }
                )
                bar.update(0)
                t_wait_1 = time.time()
                ts_executor["executor -> wait"] = (
                    ts_executor.get("executor -> wait", 0) + t_wait_1 - t_wait_0
                )
                gdfs.append(agg_gdf)

                for key, value in part_ts.items():
                    ts_worker[key] = ts_worker.get(key, 0) + value

                t_wait_0 = time.time()
            print(f"[{datetime.now()}] - Workers finished...")
            print(
                f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
            )

    print(f"[{datetime.now()}] - Executor terminated...")

    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    del input_gdfs

    with with_time("executor -> gc", ts_executor):
        gc.collect()

    print(f"[{datetime.now()}] - Concatenating...")
    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    ret = pd.concat(gdfs, ignore_index=True).set_crs(input_gdf_crs)
    del gdfs
    with with_time("executor -> gc", ts_executor):
        gc.collect()

    if inindex_column_names is not None:
        ret[inindex_column_names] = ret[inindex_column_names].fillna(False)

    print(f"[{datetime.now()}] - Sorting...")
    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    if inindex_column_names is not None:
        ret = ret.sort_values(
            by=["cell_id", *inindex_column_names],
            ascending=[True, *([False] * len(inindex_column_names))],
            ignore_index=True,
        )
    else:
        ret = ret.sort_values(by=["cell_id"], ascending=[True], ignore_index=True)
    with with_time("executor -> gc", ts_executor):
        gc.collect()

    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    for total_name, ts in [
        ("executor", ts_executor),
        ("worker", ts_worker),
    ]:
        t_total = ts[total_name]
        t_ratio = {key: value / t_total for key, value in ts.items()}
        for key in sorted(ts.keys()):
            print(f" - {key}: {t_ratio[key]:.2%} ({ts[key]:.2f}s)")

    print(
        f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
    )

    return ret
