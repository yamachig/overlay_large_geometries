from pathlib import Path
import pandas as pd
import geopandas as gpd
from colorama import Fore, Style
from datetime import datetime

from overlay_large_geometries.grid import Grid, apply_grid, unary_union_by_cell
from overlay_large_geometries.overlay import overlay as _overlay
from overlay_large_geometries.common import with_time


def overlay(
    *,
    dest_dir: Path,
    extent_path: Path,
    left_path: Path,
    right_path: Path,
    how: str,
    left_columns: list[str] = [],
    right_columns: list[str] = [],
    longitude_range: tuple[float, float] | None = None,
    latitude_range: tuple[float, float] | None = None,
):
    dest_dir.mkdir(exist_ok=True, parents=True)

    times = {}

    extent_gdf = (
        gpd.read_parquet(extent_path)
        if extent_path.suffix == ".parquet"
        else gpd.read_file(extent_path)
    )
    grid = Grid(
        extent_gdf=extent_gdf,
        dest_dir=dest_dir / "Grid/",
    )
    grid.generate()

    del extent_gdf

    left_gdf = (
        gpd.read_parquet(left_path)
        if left_path.suffix == ".parquet"
        else gpd.read_file(left_path)
    )
    right_gdf = (
        gpd.read_parquet(right_path)
        if right_path.suffix == ".parquet"
        else gpd.read_file(right_path)
    )

    if longitude_range or latitude_range:
        longitude_slice = slice(*longitude_range) if longitude_range else slice(None)
        latitude_slice = slice(*latitude_range) if latitude_range else slice(None)
        left_gdf = left_gdf.cx[longitude_slice, latitude_slice]
        right_gdf = right_gdf.cx[longitude_slice, latitude_slice]

    print(f"{Fore.GREEN}[{datetime.now()}] overlay: Applying grid...{Style.RESET_ALL}")
    with with_time("apply_grid", times):
        left_gdf = apply_grid(grid, left_gdf)
        right_gdf = apply_grid(grid, right_gdf)

    print(
        f"{Fore.GREEN}[{datetime.now()}] overlay: Taking unary union...{Style.RESET_ALL}"
    )
    with with_time("unary_union_by_cell", times):
        left_gdf = unary_union_by_cell(grid, left_gdf, left_columns)
        right_gdf = unary_union_by_cell(grid, right_gdf, right_columns)

    left_grid_path = dest_dir / "left_grid.parquet"
    right_grid_path = dest_dir / "right_grid.parquet"

    left_gdf.to_parquet(left_grid_path)
    right_gdf.to_parquet(right_grid_path)

    del left_gdf, right_gdf

    print(f"{Fore.GREEN}[{datetime.now()}] overlay: Overlaying...{Style.RESET_ALL}")
    with with_time("overlay", times):
        overlay_gdf = _overlay(
            input_parquet_paths=[left_grid_path, right_grid_path],
            how=how,
        )

    return overlay_gdf, times
