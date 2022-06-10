from pathlib import Path
import sys
import pandas as pd
import geopandas as gpd
from colorama import Fore, Style
from datetime import datetime
import psutil
import json

TESTS_ROOT = Path(__file__).parent

if __name__ == "__main__":
    path = TESTS_ROOT
    while len(list(path.glob("__init__.py"))) > 0:
        path = path.parent
    sys.path.append(str(path))

from overlay_large_geometries.common import read_typed_csv, to_typed_csv, with_time
from overlay_large_geometries.fcc_f477 import Dataset
from overlay_large_geometries.__main__ import main as overlay_main


class Inputs:
    def __init__(self):
        dataset = Dataset(
            dest_dir=TESTS_ROOT / "orig_data",
            periods=[(2014, 12)],
            mnos=["AT_T_Mobility"],
        ).download()

        self.extent_path = dataset.states.item_parquet_path()
        self.left_path = dataset.zcta5.item_parquet_path()
        self.right_path = dataset.fcc_f477.item_parquet_path(
            period=dataset.fcc_f477.periods[0],
            mno=dataset.fcc_f477.mnos[0],
            techcode=dataset.fcc_f477.techcodes[0],
        )


def get_naive_overlay_aggregate(left_parquet_path, right_parquet_path):
    times = {}

    left_gdf = gpd.read_parquet(left_parquet_path).cx[-115:-118, 32:35]
    right_gdf = gpd.read_parquet(right_parquet_path).cx[-115:-118, 32:35]

    right_gdf = right_gdf.assign(covered=True)

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Naive overlaying...{Style.RESET_ALL}")
    with with_time("naive_overlay", times):
        overlay_gdf = left_gdf.overlay(right_gdf, how="identity")

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Calculating area...{Style.RESET_ALL}")
    overlay_gdf: gpd.GeoDataFrame = overlay_gdf.pipe(
        lambda df: df.fillna({"covered": False})
    )
    overlay_gdf = overlay_gdf.to_crs("ESRI:102008")
    overlay_gdf["area"] = overlay_gdf.geometry.area

    overlay_gdf.to_parquet(TESTS_ROOT / "out_basic/overlay_basic_naive.parquet")

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Aggregating...{Style.RESET_ALL}")
    df: pd.DataFrame = (
        overlay_gdf.groupby(["ZCTA5CE10", "covered"]).agg({"area": "sum"}).reset_index()
    )

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Saving...{Style.RESET_ALL}")
    df.to_stata(TESTS_ROOT / "out_basic/coverage_basic_naive.dta", write_index=False)

    return df, times


def main():
    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Generating inputs...{Style.RESET_ALL}")
    inputs = Inputs()

    # python overlay_large_geometries --dest-dir="tests/out_basic" --extent-path="tests\orig_data\States\cb_2018_us_state_500k(S_B37B0AEDE437)(H_BACD70DBD27B).parquet" --left-path="tests\orig_data\ZCTA5\tl_2019_us_zcta510(S_64CB2443430F)(H_17CFCC53A69A).parquet" --right-path="tests\orig_data\FCC_F477\AT_T_Mobility_2014_12_83\AT_T_Mobility_2014_12_83(S_0A1DB842563C)(H_8F9469B79E77).parquet" --how=identity --left-columns ZCTA5CE10 --longitude-range=-115:-118 --latitude-range=32:35
    overlay_main(
        [
            "--dest-dir",
            str(TESTS_ROOT / "out_basic"),
            "--extent-path",
            str(inputs.extent_path),
            "--left-path",
            str(inputs.left_path),
            "--right-path",
            str(inputs.right_path),
            "--how",
            "identity",
            "--left-columns",
            "ZCTA5CE10",
            "--longitude-range=-115:-118",
            "--latitude-range=32:35",
        ]
    )
    overlay_gdf = gpd.read_parquet(TESTS_ROOT / "out_basic/overlay.parquet")
    with (TESTS_ROOT / "out_basic/times.json").open("r") as f:
        times = json.load(f)

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Calculating area...{Style.RESET_ALL}")
    overlay_gdf = overlay_gdf.assign(covered=~overlay_gdf["DBA"].isna())
    overlay_gdf = overlay_gdf.to_crs("ESRI:102008")
    overlay_gdf["area"] = overlay_gdf.geometry.area

    print(f"{Fore.GREEN}[{datetime.now()}] TEST: Aggregating...{Style.RESET_ALL}")
    aggregate_df: pd.DataFrame = (
        overlay_gdf.groupby(["ZCTA5CE10", "covered"]).agg({"area": "sum"}).reset_index()
    )
    to_typed_csv(aggregate_df, TESTS_ROOT / "out_basic/coverage.csv")

    total_time = sum(times.values())
    naive_aggregate_df, naive_times = get_naive_overlay_aggregate(
        left_parquet_path=inputs.left_path,
        right_parquet_path=inputs.right_path,
    )
    naive_total_time = sum(naive_times.values())

    df = pd.concat(
        [
            aggregate_df.set_index(["ZCTA5CE10", "covered"]),
            naive_aggregate_df.set_index(["ZCTA5CE10", "covered"]).rename(
                columns={"area": "area_naive"}
            ),
        ],
        axis=1,
    )
    mismatch_df = df[df["area"].isna() | df["area_naive"].isna()]
    mismatch_rate = mismatch_df.shape[0] / df.shape[0]

    df = df.dropna()
    df["area_diff"] = df["area"] - df["area_naive"]
    total_abs_error = df["area_diff"].abs().sum()
    total_area = (df["area"].sum() + df["area_naive"].sum()) / 2
    error_rate = total_abs_error / total_area

    print()
    print(f"TEST RESULT")
    print(f"===========")
    print()
    print(f"- total time: {total_time:.0f} sec")
    print(f"- total time (naive): {naive_total_time:.0f} sec")
    print()
    print(f"- mismatch rows: {mismatch_rate * 100}%")
    print(f"- area error rate: {error_rate:.2g}")
    print()
    print(f"- CPU freq: {psutil.cpu_freq()}")
    print(
        f"- CPU count: logical: {psutil.cpu_count()}; physical: {psutil.cpu_count(logical=False)}"
    )
    print(f"- Total RAM: {psutil.virtual_memory().total / (2 ** 30):,.2f}GB")
    print()


if __name__ == "__main__":
    main()
