from math import ceil
from random import shuffle, seed

seed(10)
from colorama import Fore, Style
import psutil

from tqdm import tqdm

from overlay_large_geometries.fcc_f477.overlay import ZCTA5_FCC_F477, FCC_F477_ALL_MNOs
from overlay_large_geometries.common import (
    to_typed_csv,
    rename_with_hash,
    get_hash_from_name,
    read_typed_csv,
    CustomThreadPoolExecutor,
)

from pathlib import Path

import geopandas as gpd
import pandas as pd
from datetime import datetime
import multiprocessing

ε = 2**-40


def _aggregate_worker(params):
    zipcodes: list[str] = params["zipcodes"]
    gdf: gpd.GeoDataFrame = params["gdf"]
    covered_colnames: list[str] = params["covered_colnames"]

    zc_gdf: gpd.GeoDataFrame = gdf[gdf["ZCTA5CE10"].isin(zipcodes)]
    zc_gdf = zc_gdf.to_crs("ESRI:102008")
    zc_gdf["area"] = zc_gdf.geometry.area

    df = (
        zc_gdf.groupby(["ZCTA5CE10", *covered_colnames])
        .agg({"area": "sum"})
        .reset_index()
    )
    return df


class ZCTA5_FCC_F477_table:
    def __init__(
        self,
        zcta5_fcc_f477: ZCTA5_FCC_F477,
        dest_dir: Path,
    ):
        self.zcta5_fcc_f477 = zcta5_fcc_f477
        self.dest_dir = dest_dir

    def periods(self):
        return self.zcta5_fcc_f477.periods()

    def item_dir_name(self, period):
        return self.zcta5_fcc_f477.item_dir_name(period)

    def item_dir_path(self, period) -> Path | None:
        return self.dest_dir / self.item_dir_name(period)

    def mno_techcode_column_name(self, mno, techcode):
        return self.zcta5_fcc_f477.mno_techcode_column_name(mno, techcode)

    def mno_techcodes(self):
        return self.zcta5_fcc_f477.mno_techcodes()

    def agg_dir_path(self) -> Path | None:
        return self.dest_dir / "aggregate"

    def item_csv_path(self, period) -> Path | None:
        dir = self.item_dir_path(period)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".csv":
                    return p

    def backup_item_csv(self, period) -> Path | None:
        item_path = self.item_csv_path(period)
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_item_src_hash(self, period):
        src_hash: list[str] = []

        orig_path = self.zcta5_fcc_f477.item_parquet_path(period)
        if orig_path is None:
            raise FileNotFoundError(f"ZCTA5_FCC_F477 not found: {period=}")
        orig_hash, _, _ = get_hash_from_name(orig_path)
        src_hash.append(orig_hash)

        return src_hash

    def agg_csv_path(self) -> Path | None:
        dir = self.agg_dir_path()
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".csv":
                    return p

    def generate(self):
        not_exist_keys = []

        for period in self.periods():
            dest_path = self.item_csv_path(period)
            if not dest_path or not dest_path.exists():
                not_exist_keys.append(period)
            else:
                _, src_hash, _ = get_hash_from_name(dest_path)
                if src_hash != self.get_item_src_hash(period):
                    not_exist_keys.append(period)

        if not not_exist_keys:
            print(
                f"[{datetime.now()}] All ZCTA5_FCC_F477_table files already generated."
            )

        for period in tqdm(not_exist_keys, colour="yellow"):
            self.backup_item_csv(period)

            dest_dir = self.item_dir_path(period)

            print(
                f"{Fore.YELLOW}[{datetime.now()}] Generating ZCTA5_FCC_F477_table for {period}...{Style.RESET_ALL}"
            )

            parquet_path = self.zcta5_fcc_f477.item_parquet_path(period)

            print(
                f"[{datetime.now()}] - Reading parquet ({ceil(parquet_path.stat().st_size / 1024 / 1024)}MB)..."
            )
            print(
                f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
            )
            gdf = gpd.read_parquet(parquet_path)

            zipcodes: list[str] = gdf["ZCTA5CE10"].unique().tolist()
            shuffle(zipcodes)
            batch_size = ceil(len(zipcodes) / (multiprocessing.cpu_count() * 2 + 5))

            print(
                f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
            )
            print(
                f"[{datetime.now()}] - CPU count: {multiprocessing.cpu_count()}, Batch size: {batch_size}"
            )
            covered_colnames = [
                self.mno_techcode_column_name(mno, techcode)
                for mno, techcode in self.mno_techcodes()
            ]
            zipcode_range_list = [
                (i, i + batch_size) for i in range(0, len(zipcodes), batch_size)
            ]
            params_generator = (
                {
                    "zipcodes": zipcodes[zc_start:zc_end],
                    "gdf": gdf,
                    "covered_colnames": covered_colnames,
                }
                for zc_start, zc_end in zipcode_range_list
            )

            dfs: list[pd.DataFrame] = []

            with CustomThreadPoolExecutor() as executor:

                map_generator = executor.race_map(_aggregate_worker, params_generator)
                bar = tqdm(map_generator, total=len(zipcode_range_list))
                bar.set_postfix(
                    {
                        "thds": len(executor._threads),
                        "CPU": f"{psutil.cpu_percent():.0f}%",
                        "RAM": f"{psutil.virtual_memory()[2]:.0f}%",
                    }
                )
                for part_df in bar:
                    bar.set_postfix(
                        {
                            "thds": len(executor._threads),
                            "CPU": f"{psutil.cpu_percent():.0f}%",
                            "RAM": f"{psutil.virtual_memory()[2]:.0f}%",
                        }
                    )
                    bar.update(0)
                    dfs.append(part_df)

            df = pd.concat(dfs, ignore_index=True).reset_index(drop=True)
            df = df.sort_values(
                by=["ZCTA5CE10", *covered_colnames],
                ascending=[True, *([False] * len(covered_colnames))],
                ignore_index=True,
            )

            print("\r\n======================\r\n")
            print(f"Result obs: {df.shape[0]}")
            print(df.head(10))
            print("\r\n======================\r\n")

            _, _, stem = get_hash_from_name(parquet_path)
            dest_csv_path = dest_dir / f"{stem}.csv"
            csvt_path = to_typed_csv(df, dest_csv_path)
            dest_csv_path = rename_with_hash(
                dest_csv_path, src_hash=self.get_item_src_hash(period)
            )
            csvt_path.rename(csvt_path.with_stem(dest_csv_path.stem))

    def generate_aggregate(self):
        dfs = []
        for period in self.periods():
            item_csv_path = self.item_csv_path(period)
            df: pd.DataFrame = read_typed_csv(item_csv_path)
            df["year_month"] = f"{period[0]}-{period[1]:02d}"
            df["year"] = period[0]
            df["month"] = period[1]
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)

        dest_csv_path = self.agg_dir_path() / f"ZCTA5_FCC_F477_aggregate.csv"
        to_typed_csv(df, dest_csv_path)
