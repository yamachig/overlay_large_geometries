from math import ceil, floor
from random import shuffle
from tqdm import tqdm

from pathlib import Path

from overlay_large_geometries.common import (
    rename_with_hash,
    check_area,
    with_time,
    CustomThreadPoolExecutor,
)

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon, MultiPolygon
from datetime import datetime
import multiprocessing
import time
import pygeos


class Grid:
    def __init__(
        self,
        extent_gdf: gpd.GeoDataFrame,
        dest_dir: Path,
    ):
        self.extent_gdf = extent_gdf
        self.dest_dir = dest_dir

    def item_parquet_path(self):
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def generate(self):
        if self.item_parquet_path():
            print(f"[{datetime.now()}] All Grid files already generated.")
            return
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        print(f"Generating grid...")

        extent_gdf = self.extent_gdf.to_crs("EPSG:4326")
        print(f"CRS: {extent_gdf.crs.name}")
        print(f"Obs: {extent_gdf.shape[0]}")
        print(extent_gdf.head(2))

        buffered: gpd.GeoDataFrame = extent_gdf.copy()
        buffered.geometry = extent_gdf.buffer(0.1)

        dissolved_gdf: gpd.GeoDataFrame = buffered.dissolve()
        dissolved: MultiPolygon = dissolved_gdf.unary_union

        all_grid_data: list[dict] = []

        xmin, ymin, xmax, ymax = dissolved_gdf.total_bounds
        xs = range(floor(xmin), ceil(xmax) + 1)
        ys = range(floor(ymin), ceil(ymax) + 1)
        for x in xs:
            for y in ys:
                cell_id = f"{x},{y}"
                polygon = Polygon([(x, y), (x + 1, y), (x + 1, y + 1), (x, y + 1)])
                all_grid_data.append(
                    {
                        "cell_id": cell_id,
                        "bottom": y,
                        "top": y + 1,
                        "left": x,
                        "right": x + 1,
                        "geometry": polygon,
                    }
                )
        all_grid = gpd.GeoDataFrame(all_grid_data, crs="EPSG:4326")

        grid = all_grid[all_grid.intersects(dissolved)]
        print(f"CRS: {grid.crs.name}")
        print(f"Obs: {grid.shape[0]}")
        print(grid.head(2))

        target_file_path = self.dest_dir / "grid.parquet"
        grid.to_parquet(target_file_path)
        rename_with_hash(target_file_path)

    def get_cells(self):
        grid_gdf = gpd.read_parquet(self.item_parquet_path())
        cells = {}
        for _, row in grid_gdf.iterrows():
            if row["cell_id"] in cells:
                raise Exception(
                    f"[{datetime.now()}] Duplicated cell_id: {row['cell_id']}"
                )
            cells[row["cell_id"]] = row.to_dict()
        return cells


def apply_grid_many_interiors(
    orig: gpd.GeoDataFrame, grid: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    orig = (
        orig.reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "global_orig_index"})
    )

    exteriors_rows: list[dict] = []
    interiors_rows: list[dict] = []
    global_orig_attrs: dict[int, dict] = {}

    print(f"[{datetime.now()}] - Splitting exteriors and interiors")

    for _, orig_row in tqdm(list(orig.iterrows())):
        orig_dict = orig_row.to_dict()
        del orig_dict["geometry"]
        del orig_dict["global_orig_index"]
        global_orig_index = orig_row["global_orig_index"]
        global_orig_attrs[global_orig_index] = orig_dict
        orig_geom: Polygon = orig_row["geometry"]
        exteriors_rows.append(
            {
                "global_orig_index": global_orig_index,
                "geometry": Polygon(orig_geom.exterior).buffer(0),
            }
        )
        for ring in orig_geom.interiors:
            row = {
                "global_orig_index": global_orig_index,
                "geometry": Polygon(ring).buffer(0),
            }
            interiors_rows.append(row)

    orig_exteriors = gpd.GeoDataFrame(exteriors_rows, crs=orig.crs)
    orig_interiors = gpd.GeoDataFrame(interiors_rows, crs=orig.crs)

    print(f"[{datetime.now()}] - Processing exteriors")
    exteriors_out = apply_grid_few_interiors(orig_exteriors, grid, batch_size=4)

    print(f"[{datetime.now()}] - Processing interiors")
    interiors_out = apply_grid_few_interiors(orig_interiors, grid)

    print(f"[{datetime.now()}] - Checking key consistency...")

    exteriors_keys = [
        (int(row["global_orig_index"]), row["cell_id"])
        for _, row in exteriors_out.iterrows()
    ]

    interiors_keys = [
        (int(row["global_orig_index"]), row["cell_id"])
        for _, row in interiors_out.iterrows()
    ]

    keys = list(set(exteriors_keys) | set(interiors_keys))

    for _, row in exteriors_out.iterrows():
        key = (int(row["global_orig_index"]), row["cell_id"])
        if key not in keys:
            raise Exception(f"[{datetime.now()}] - Exterior key not found: {key}")

    for _, row in interiors_out.iterrows():
        key = (int(row["global_orig_index"]), row["cell_id"])
        if key not in keys:
            raise Exception(f"[{datetime.now()}] - Interior key not found: {key}")

    rows: list[dict] = []

    print(f"[{datetime.now()}] - Combining exteriors and interiors")

    with CustomThreadPoolExecutor() as executor:

        batch_size = 32

        print(
            f"[{datetime.now()}] - CPU count: {multiprocessing.cpu_count()}, Batch size: {batch_size}"
        )
        keys_range_list = [(i, i + batch_size) for i in range(0, len(keys), batch_size)]

        def worker(keys_range: tuple[int, int]):
            rows = []
            for key in keys[keys_range[0] : keys_range[1]]:
                global_orig_index, cell_id = key
                exteriors: gpd.GeoDataFrame = exteriors_out[
                    (exteriors_out.cell_id == cell_id)
                    & (exteriors_out.global_orig_index == global_orig_index)
                ]
                interiors: gpd.GeoDataFrame = interiors_out[
                    (interiors_out.cell_id == cell_id)
                    & (interiors_out.global_orig_index == global_orig_index)
                ]
                if exteriors.shape[0] == 0:
                    raise Exception(f"[{datetime.now()}] - Empty exterior for {key=}")
                exterior_union = exteriors.unary_union
                check_area(
                    gpd.GeoDataFrame(geometry=[exterior_union], crs=orig.crs),
                    exteriors,
                    threshold=1e-10,
                    print_message=False,
                    name=f"apply_grid_many_interiors worker({keys_range=}) exteriors",
                )
                interior_union = (
                    None if interiors.shape[0] == 0 else interiors.unary_union
                )

                if interior_union:
                    check_area(
                        gpd.GeoDataFrame(geometry=[interior_union], crs=orig.crs),
                        interiors,
                        threshold=1e-10,
                        print_message=False,
                        name=f"apply_grid_many_interiors worker({keys_range=}) interiors",
                    )
                    geom = exterior_union.difference(interior_union)

                    exterior_area = exterior_union.area
                    interior_area = interior_union.area
                    geom_area = geom.area
                    area_diff_ratio = (geom_area - (exterior_area - interior_area)) / (
                        (abs(exterior_area - interior_area) + 1e-14)
                    )
                    if abs(area_diff_ratio) > 1e-10:
                        raise Exception(
                            f"[{datetime.now()}] - Area is not equal: {area_diff_ratio}"
                        )
                else:
                    geom = exterior_union

                if geom:
                    rows.append(
                        {
                            **global_orig_attrs[global_orig_index],
                            "cell_id": cell_id,
                            "geometry": geom,
                        }
                    )

            return rows

        map_generator = executor.race_map(worker, keys_range_list)
        bar = tqdm(map_generator, total=len(keys_range_list))
        bar.set_postfix({"thds": len(executor._threads)})
        bar.update(0)
        for out_rows in bar:
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)
            if out_rows is not None:
                rows.extend(out_rows)
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)

    ret = gpd.GeoDataFrame(rows, crs=orig.crs)

    return ret


def _apply_grid_few_interiors_worker(params):
    ts = {}

    orig: gpd.GeoDataFrame = params["orig"]
    grid: gpd.GeoDataFrame = params["grid"]
    orig_range: tuple[int, int] = params["orig_range"]

    with with_time("worker", ts):

        with with_time("worker -> filter_orig", ts):
            filtered_orig: gpd.GeoDataFrame = orig.iloc[
                orig_range[0] : orig_range[1]
            ].copy(deep=False)

        with with_time("worker -> aggregate", ts):
            columns = list(filtered_orig.columns[filtered_orig.columns != "geometry"])
            if columns:
                filtered_orig: gpd.GeoDataFrame = (
                    filtered_orig.explode(ignore_index=True)
                    .groupby(columns)
                    .agg({"geometry": lambda df: MultiPolygon(df.geometry.tolist())})
                    .reset_index()
                )
                filtered_orig.set_geometry("geometry", inplace=True)
            else:
                pass
                geometry = filtered_orig.explode(ignore_index=True).geometry.tolist()
                filtered_orig = gpd.GeoDataFrame(
                    geometry=[MultiPolygon(geometry)], crs=filtered_orig.crs
                )

        xmin, ymin, xmax, ymax = filtered_orig.total_bounds

        with with_time("worker -> filter_grid", ts):
            filtered_grid: gpd.GeoDataFrame = grid[
                (xmin <= grid.right)
                & (grid.left <= xmax)
                & (ymin <= grid.top)
                & (grid.bottom <= ymax)
            ][["cell_id", "geometry"]].copy(deep=False)

        if filtered_grid.shape[0] == 0:
            print(
                f"[{datetime.now()}] - No grid found for {xmin=:.2f} {xmax=:.2f} {ymin=:.2f} {ymax=:.2f}"
            )
            return orig_range, None

        filtered_orig = filtered_orig.set_crs(orig.crs)
        filtered_grid = filtered_grid.set_crs(grid.crs)

        with with_time("worker -> overlay", ts):
            overlayed: gpd.GeoDataFrame = filtered_orig.overlay(
                filtered_grid, how="identity"
            )

        null_cells = overlayed[overlayed.cell_id.isnull()]
        if null_cells.shape[0] > 0:
            ignore_bounds = [
                [
                    -75.231467,
                    19.88796,
                    -75.087575,
                    19.973395,
                ],  # (2020, 6), T_Mobile, 83
            ]

            def similar(bounds1, bounds2):
                return (
                    abs(bounds1[0] - bounds2[0]) < 1e-5
                    and abs(bounds1[1] - bounds2[1]) < 1e-5
                    and abs(bounds1[2] - bounds2[2]) < 1e-5
                    and abs(bounds1[3] - bounds2[3]) < 1e-5
                )

            if any(
                similar(null_cells.total_bounds, bounds) for bounds in ignore_bounds
            ):
                print(f"[{datetime.now()}] - null_cells ignored.")
                overlayed = overlayed[~overlayed.cell_id.isnull()]
            else:
                print(f"[{datetime.now()}] - null_cells =")
                print(null_cells)
                print(f"[{datetime.now()}] - null_cells.geometry =")
                for geom in null_cells.geometry:
                    print(geom)
                print(f"[{datetime.now()}] - {null_cells.total_bounds = }")
                print(
                    f"[{datetime.now()}] - filtered_orig.total_bounds = {xmin, ymin, xmax, ymax}"
                )
                print(
                    f"[{datetime.now()}] - filtered_grid.cell_id = {filtered_grid['cell_id'].to_list()}"
                )
                print(f"[{datetime.now()}] - filtered_grid =")
                print(filtered_grid)
                print(f"[{datetime.now()}] - (filtered_grid.geometry =")
                print(filtered_grid.geometry)
                raise Exception(f"[{datetime.now()}] - Some cells are not in grid.")

        if overlayed.shape[0] == 0:
            print(f"[{datetime.now()}] - Empty overlay ")
            return orig_range, None

    return ts, overlayed


def apply_grid_few_interiors(
    orig: gpd.GeoDataFrame, grid: gpd.GeoDataFrame, batch_size=None
) -> gpd.GeoDataFrame:
    gdfs: list[gpd.GeoDataFrame] = []

    batch_size = batch_size or min(
        4096, ceil(orig.shape[0] / (multiprocessing.cpu_count() * 2 + 5))
    )

    print(
        f"[{datetime.now()}] - CPU count: {multiprocessing.cpu_count()}, Batch size: {batch_size}"
    )
    orig_range_list = [(i, i + batch_size) for i in range(0, orig.shape[0], batch_size)]

    ts_executor = {}
    ts_worker = {}
    with with_time("executor", ts_executor):
        with CustomThreadPoolExecutor() as executor:

            params_generator = (
                {"orig_range": orig_range, "orig": orig, "grid": grid}
                for orig_range in orig_range_list
            )
            map_generator = executor.race_map(
                _apply_grid_few_interiors_worker,
                params_generator,
            )
            bar = tqdm(map_generator, total=len(orig_range_list))
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)
            t_wait_0 = time.time()
            for part_ts, out_part in bar:
                t_wait_1 = time.time()
                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)
                ts_executor["executor -> wait"] = (
                    ts_executor.get("executor -> wait", 0) + t_wait_1 - t_wait_0
                )
                if out_part is not None:
                    gdfs.append(out_part)
                for key, value in part_ts.items():
                    ts_worker[key] = ts_worker.get(key, 0) + value
                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)
                t_wait_0 = time.time()

    for total_name, ts in [("executor", ts_executor), ("worker", ts_worker)]:
        t_total = ts[total_name]
        t_ratio = {key: value / t_total for key, value in ts.items()}
        for key in sorted(ts.keys()):
            print(f" - {key}: {t_ratio[key]:.2%} ({ts[key]:.2f}s)")

    ret = pd.concat(gdfs).set_crs(orig.crs)

    return ret


def _unary_union_by_cell_worker(params):
    ts = {}
    with with_time("worker", ts):

        cell_range = params["cell_range"]
        orig = params["orig"]
        by = params["by"]
        cell_ids = params["cell_ids"]

        filtered_cell_ids = cell_ids[cell_range[0] : cell_range[1]]

        gdfs: list[gpd.GeoDataFrame] = []

        for cell_id in filtered_cell_ids:

            with with_time("worker -> select", ts):
                orig_in_cell: gpd.GeoDataFrame = orig.loc[
                    orig.cell_id == cell_id,
                    ~orig.columns.isin(["__orig_index", "global_orig_index"]),
                ].copy(deep=False)

            with with_time("worker -> precision", ts):
                orig_in_cell.geometry = pygeos.set_precision(
                    orig_in_cell.geometry.values.data, 0.000001
                )

            with with_time("worker -> valid", ts):
                invalid_orig = orig_in_cell[~orig_in_cell.geometry.is_valid]
                if invalid_orig.shape[0] > 0:
                    print(invalid_orig)

            if orig_in_cell.shape[0] == 0:
                continue

            with with_time("worker -> dissolve", ts):
                dissolved = orig_in_cell.dissolve(by=by).reset_index()

            gdfs.append(dissolved)

    return ts, gdfs


def apply_grid(
    grid: Grid,
    all_orig: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:

    grid: gpd.GeoDataFrame = gpd.read_parquet(grid.item_parquet_path())
    print(f"CRS: {grid.crs.name}")
    print(f"Obs: {grid.shape[0]}")
    print(grid.head(2))

    print(f"[{datetime.now()}] - Projecting to {grid.crs.name} ...")
    all_orig = all_orig.to_crs(grid.crs)

    print(f"[{datetime.now()}] - Exploding...")
    all_orig = all_orig.explode(ignore_index=True)

    print(f"[{datetime.now()}] - Counting interiors ...")
    many_interiors = all_orig.interiors.apply(len) > 8
    print(
        f"[{datetime.now()}] - {many_interiors[~many_interiors].shape[0]} polygons with few interiors"
    )
    print(
        f"[{datetime.now()}] - {many_interiors[many_interiors].shape[0]} polygons with many interiors"
    )

    gdfs = []

    print(f"[{datetime.now()}] - Processing polygons with many interiors...")
    orig_many_interiors = all_orig[many_interiors]
    many_out = (
        apply_grid_many_interiors(orig_many_interiors, grid)
        if orig_many_interiors.shape[0] > 0
        else None
    )
    if many_out is not None:
        gdfs.append(many_out)
    del orig_many_interiors

    print(f"[{datetime.now()}] - Processing polygons with few interiors...")
    orig_few_interiors = all_orig[~many_interiors]
    few_out = (
        apply_grid_few_interiors(orig_few_interiors, grid)
        if orig_few_interiors.shape[0] > 0
        else None
    )
    if few_out is not None:
        gdfs.append(few_out)
    del orig_few_interiors

    out = pd.concat(gdfs).set_crs(grid.crs)

    return out


def unary_union_by_cell(
    grid: Grid,
    orig: gpd.GeoDataFrame,
    by: list[str],
) -> gpd.GeoDataFrame:

    if "cell_id" not in by:
        by = ["cell_id"] + by

    print(f"[{datetime.now()}] - Unary union by {by}...")

    grid: gpd.GeoDataFrame = gpd.read_parquet(grid.item_parquet_path())
    print(f"[{datetime.now()}] - {orig.crs.name = }...")

    gdfs = []

    cell_ids = grid.cell_id.unique().tolist()
    shuffle(cell_ids)

    batch_size = ceil(len(cell_ids) / (multiprocessing.cpu_count() * 8))

    print(
        f"[{datetime.now()}] - CPU count: {multiprocessing.cpu_count()}, Batch size: {batch_size}"
    )
    cell_range_list = [(i, i + batch_size) for i in range(0, len(cell_ids), batch_size)]

    params_generator = (
        {
            "cell_range": cell_range,
            "orig": orig,
            "by": by,
            "cell_ids": cell_ids,
        }
        for cell_range in cell_range_list
    )

    ts_executor = {}
    ts_worker = {}
    with with_time("executor", ts_executor):
        with CustomThreadPoolExecutor() as executor:
            map_generator = executor.race_map(
                _unary_union_by_cell_worker,
                params_generator,
            )
            bar = tqdm(map_generator, total=len(cell_range_list))
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)
            t_wait_0 = time.time()
            for part_ts, part_gdfs in bar:
                t_wait_1 = time.time()
                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)
                ts_executor["executor -> wait"] = (
                    ts_executor.get("executor -> wait", 0) + t_wait_1 - t_wait_0
                )

                with with_time("executor -> extend", ts_executor):
                    gdfs.extend(part_gdfs)

                for key, value in part_ts.items():
                    ts_worker[key] = ts_worker.get(key, 0) + value

                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)

                t_wait_0 = time.time()

    print(f"[{datetime.now()}] - Workers finished.")

    for total_name, ts in [("executor", ts_executor), ("worker", ts_worker)]:
        t_total = ts[total_name]
        t_ratio = {key: value / t_total for key, value in ts.items()}
        for key in sorted(ts.keys()):
            print(f" - {key}: {t_ratio[key]:.2%} ({ts[key]:.2f}s)")

    ret = pd.concat(gdfs).set_crs(orig.crs)

    return ret
