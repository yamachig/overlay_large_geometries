from random import seed

seed(10)
from colorama import Fore, Style
import psutil
from overlay_large_geometries.fcc_f477.grid import ZCTA5_grid

from overlay_large_geometries.overlay import overlay

from tqdm import tqdm

from pathlib import Path

from overlay_large_geometries.fcc_f477.grid import FCC_F477_grid

from overlay_large_geometries.common import (
    rename_with_hash,
    get_hash_from_name,
)

import geopandas as gpd
from datetime import datetime


class FCC_F477_ALL_MNOs:
    def __init__(
        self,
        fcc_f477_grid: FCC_F477_grid,
        dest_dir: Path,
    ):
        self.fcc_f477_grid = fcc_f477_grid
        self.dest_dir = dest_dir

    def periods(self):
        return self.fcc_f477_grid.fcc_f477.periods

    def mno_techcode_column_name(self, mno, techcode):
        return f"{mno}_{techcode}"

    def mno_techcodes(self):
        mno_techcodes: list[tuple[str, str]] = []
        for mno in self.fcc_f477_grid.fcc_f477.mnos:
            for techcode in self.fcc_f477_grid.fcc_f477.techcodes:
                mno_techcodes.append((mno, techcode))
        return mno_techcodes

    def item_dir_name(self, period):
        return f"{period[0]}_{period[1]:02d}"

    def item_dir_path(self, period) -> Path | None:
        return self.dest_dir / self.item_dir_name(period)

    def item_parquet_path(self, period) -> Path | None:
        dir = self.item_dir_path(period)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def backup_item_parquet(self, period) -> Path | None:
        item_path = self.item_parquet_path(period)
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_src_hash(self, period):
        src_hash: list[str] = []
        for mno in self.fcc_f477_grid.fcc_f477.mnos:
            for techcode in self.fcc_f477_grid.fcc_f477.techcodes:
                fcc_f477_path = self.fcc_f477_grid.item_parquet_path(
                    period, mno, techcode
                )
                if fcc_f477_path is None:
                    raise FileNotFoundError(
                        f"FCC_F477_grid not found: {period=} {mno=} {techcode=}"
                    )
                orig_hash, _, _ = get_hash_from_name(fcc_f477_path)
                src_hash.append(orig_hash)
        return src_hash

    def generate_for_key(self, period):
        dest_gdf: gpd.GeoDataFrame | None = None

        parquet_paths = []
        for mno, techcode in self.mno_techcodes():
            parquet_paths.append(
                self.fcc_f477_grid.item_parquet_path(period, mno, techcode)
            )
        print(f"{parquet_paths=}")
        inindex_column_names = [
            self.mno_techcode_column_name(mno, techcode)
            for mno, techcode in self.mno_techcodes()
        ]
        dest_gdf = overlay(
            input_parquet_paths=parquet_paths,
            inindex_column_names=inindex_column_names,
            how="union",
        )

        print(f"[{datetime.now()}]   - Saving...")
        print(
            f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
        )

        dest_dir = self.item_dir_path(period)
        dest_parquet_path: Path = (
            dest_dir / f"FCC_F477_ALL_MNOs_{period[0]}_{period[1]:02d}.parquet"
        )
        dest_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        dest_gdf.to_parquet(dest_parquet_path)
        dest_parquet_path = rename_with_hash(
            dest_parquet_path, src_hash=self.get_src_hash(period)
        )

        del dest_gdf

        print(
            f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
        )
        print(f"[{datetime.now()}]   - Done.")

    def generate(self):
        not_exist_keys = []

        for period in self.periods():
            dest_path = self.item_parquet_path(period)
            if not dest_path or not dest_path.exists():
                not_exist_keys.append(period)
            else:
                _, src_hash, _ = get_hash_from_name(dest_path)
                if src_hash != self.get_src_hash(period):
                    not_exist_keys.append(period)

        if not not_exist_keys:
            print(f"[{datetime.now()}] All FCC_F477_ALL_MNOs files already generated.")
            return

        key_bar = tqdm(not_exist_keys, colour="yellow")
        for period in key_bar:
            self.backup_item_parquet(period)

            print(
                f"{Fore.YELLOW}[{datetime.now()}] Generating FCC_F477_ALL_MNOs for {period}...{Style.RESET_ALL}"
            )
            self.generate_for_key(period)


class ZCTA5_FCC_F477:
    def __init__(
        self,
        fcc_f477_all_mnos: FCC_F477_ALL_MNOs,
        zcta5_grid: ZCTA5_grid,
        dest_dir: Path,
    ):
        self.fcc_f477_all_mnos = fcc_f477_all_mnos
        self.zcta5_grid = zcta5_grid
        self.dest_dir = dest_dir

    def periods(self):
        return self.fcc_f477_all_mnos.periods()

    def item_dir_name(self, period):
        return f"{period[0]}_{period[1]:02d}"

    def item_dir_path(self, period) -> Path | None:
        return self.dest_dir / self.item_dir_name(period)

    def mno_techcode_column_name(self, mno, techcode):
        return self.fcc_f477_all_mnos.mno_techcode_column_name(mno, techcode)

    def mno_techcodes(self):
        return self.fcc_f477_all_mnos.mno_techcodes()

    def item_parquet_path(self, period) -> Path | None:
        dir = self.item_dir_path(period)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def backup_item_parquet(self, period) -> Path | None:
        item_path = self.item_parquet_path(period)
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_src_hash(self, period):
        src_hash: list[str] = []

        fcc_f477_path = self.fcc_f477_all_mnos.item_parquet_path(period)
        if fcc_f477_path is None:
            raise FileNotFoundError(f"FCC_F477_ALL_MNOs not found: {period=}")
        fcc_f477_hash, _, _ = get_hash_from_name(fcc_f477_path)
        src_hash.append(fcc_f477_hash)

        zcta5_path = self.zcta5_grid.item_parquet_path()
        if zcta5_path is None:
            raise FileNotFoundError(f"ZCTA5_grid not found.")
        zcta5_hash, _, _ = get_hash_from_name(zcta5_path)
        src_hash.append(zcta5_hash)

        return src_hash

    def generate_for_key(self, period):
        dest_gdf: gpd.GeoDataFrame | None = None

        parquet_paths = [
            self.zcta5_grid.item_parquet_path(),
            self.fcc_f477_all_mnos.item_parquet_path(period),
        ]

        dest_gdf = overlay(input_parquet_paths=parquet_paths, how="identity")

        inindex_column_names = [
            self.mno_techcode_column_name(mno, techcode)
            for mno, techcode in self.mno_techcodes()
        ]
        dest_gdf[inindex_column_names] = dest_gdf[inindex_column_names].fillna(False)

        print(f"[{datetime.now()}]   - Saving...")
        print(
            f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
        )

        dest_dir = self.item_dir_path(period)
        dest_parquet_path: Path = (
            dest_dir / f"ZCTA5_FCC_F477_{period[0]}_{period[1]:02d}.parquet"
        )
        dest_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        dest_gdf.to_parquet(dest_parquet_path)
        dest_parquet_path = rename_with_hash(
            dest_parquet_path, src_hash=self.get_src_hash(period)
        )

        del dest_gdf

        print(
            f"[{datetime.now()}] - CPU: {psutil.cpu_percent():.0f}%, RAM: {psutil.virtual_memory()[2]:.0f}%"
        )
        print(f"[{datetime.now()}]   - Done.")

    def generate(self):
        not_exist_keys = []

        for period in self.periods():
            dest_path = self.item_parquet_path(period)
            if not dest_path or not dest_path.exists():
                not_exist_keys.append(period)
            else:
                _, src_hash, _ = get_hash_from_name(dest_path)
                if src_hash != self.get_src_hash(period):
                    not_exist_keys.append(period)

        if not not_exist_keys:
            print(f"[{datetime.now()}] All ZCTA5_FCC_F477 files already generated.")
            return

        key_bar = tqdm(not_exist_keys, colour="yellow")
        for period in key_bar:
            self.backup_item_parquet(period)

            print(
                f"{Fore.YELLOW}[{datetime.now()}] Generating ZCTA5_FCC_F477 for {period}...{Style.RESET_ALL}"
            )
            self.generate_for_key(period)
