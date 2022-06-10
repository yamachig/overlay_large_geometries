from math import ceil
from random import shuffle
import tempfile
from tqdm import tqdm
from colorama import Fore, Style

from overlay_large_geometries.fcc_f477.orig_data import FCC_F477, ZCTA5
from overlay_large_geometries.grid import (
    Grid,
    apply_grid,
    _unary_union_by_cell_worker,
    unary_union_by_cell,
)

from pathlib import Path

from overlay_large_geometries.common import (
    with_time,
    rename_with_hash,
    get_hash_from_name,
    rename_dir_with_hash,
    CustomThreadPoolExecutor,
)

import geopandas as gpd
import pandas as pd
from datetime import datetime

from pyarrow.parquet import ParquetFile
import pyarrow as pa


def _FCC_F477_grid_union_worker(cell_dir: Path):
    out_worker_ts = {}
    worker_ts = {}
    with with_time("out_worker", out_worker_ts):

        with with_time("out_worker -> read_df", out_worker_ts):
            cell_df = None
            for parquet_path in cell_dir.glob("*.parquet"):
                cell_part_df = gpd.read_parquet(parquet_path)
                if cell_df is None:
                    cell_df = cell_part_df
                else:
                    cell_df = pd.concat([cell_df, cell_part_df])

        cell_ids = cell_df["cell_id"].unique().tolist()
        params = {}
        params["cell_range"] = [0, 1]
        params["orig"] = cell_df
        params["by"] = ["cell_id"]
        params["cell_ids"] = cell_ids

        part_ts, cell_out_parts = _unary_union_by_cell_worker(params)
        for k, v in part_ts.items():
            worker_ts[k] = worker_ts.get(k, 0) + v

    return worker_ts, out_worker_ts, cell_out_parts


class FCC_F477_grid:
    def __init__(
        self,
        fcc_f477: FCC_F477,
        grid: Grid,
        dest_dir: Path,
    ):
        self.fcc_f477 = fcc_f477
        self.grid = grid
        self.dest_dir = dest_dir

    def keys(self):
        return self.fcc_f477.keys()

    def item_dir_name(self, period, mno, techcode):
        return self.fcc_f477.item_dir_name(period, mno, techcode)

    def item_dir_path(self, period, mno, techcode) -> Path | None:
        return self.dest_dir / self.item_dir_name(period, mno, techcode)

    def item_before_union_dir_path(self, period, mno, techcode) -> Path | None:
        dir = self.item_dir_path(period, mno, techcode)
        if dir.exists():
            for p in dir.iterdir():
                if p.is_dir() and p.name.startswith("before_union"):
                    return p

    def backup_item_before_union_dir(self, period, mno, techcode) -> Path | None:
        dir_path = self.item_before_union_dir_path(period, mno, techcode)
        if dir_path and dir_path.exists():
            return dir_path.rename(
                dir_path.with_name(
                    f"old_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{dir_path.name}"
                )
            )

    def item_parquet_path(self, period, mno, techcode) -> Path | None:
        dir = self.item_dir_path(period, mno, techcode)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def backup_item_parquet(self, period, mno, techcode) -> Path | None:
        item_path = self.item_parquet_path(period, mno, techcode)
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_src_hash(self, period, mno, techcode):
        src_hash: list[str] = []

        fcc_f477_path = self.fcc_f477.item_parquet_path(period, mno, techcode)
        if fcc_f477_path is None:
            raise FileNotFoundError(f"FCC_F477 not found: {period=}")
        fcc_f477_hash, _, _ = get_hash_from_name(fcc_f477_path)
        src_hash.append(fcc_f477_hash)

        grid_path = self.grid.item_parquet_path()
        if grid_path is None:
            raise FileNotFoundError(f"Grid not found.")
        grid_hash, _, _ = get_hash_from_name(grid_path)
        src_hash.append(grid_hash)

        return src_hash

    def generate_before_union(self):
        not_exist_keys = []

        for key in self.keys():
            dest_path = self.item_before_union_dir_path(*key)
            if not dest_path or not dest_path.exists():
                not_exist_keys.append(key)
            else:
                _, src_hash, _ = get_hash_from_name(dest_path)
                if src_hash != self.get_src_hash(*key):
                    not_exist_keys.append(key)

        if not not_exist_keys:
            print(
                f"[{datetime.now()}] All FCC_F477_grid before union directories already generated."
            )
            return

        for key in tqdm(not_exist_keys, colour="yellow"):
            period, mno, techcode = key
            self.backup_item_before_union_dir(*key)

            print(
                f"{Fore.YELLOW}[{datetime.now()}] Generating FCC_F477_grid before union directory for {period}, {mno}, {techcode}...{Style.RESET_ALL}"
            )
            orig_parquet_path = self.fcc_f477.item_parquet_path(period, mno, techcode)
            assert orig_parquet_path is not None, "FCC_F477 parquet file not found."

            parquet_size_limit = 100 * 1024 * 1024
            orig_parquet_bytes = orig_parquet_path.stat().st_size
            print(
                f"[{datetime.now()}] File size : {ceil(orig_parquet_bytes / 1024 / 1024)}MB."
            )

            all_cell_ids = set()

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_dir: Path = Path(tmp_dir)
                print(f"[{datetime.now()}] Using temporary directory: {tmp_dir} ...")

                pf = ParquetFile(orig_parquet_path)
                num_rows = pf.metadata.num_rows
                print(f"[{datetime.now()}] Number of rows: {num_rows}")
                target_num_batches = ceil(orig_parquet_bytes / parquet_size_limit)
                batch_size = ceil(num_rows / target_num_batches)
                print(f"[{datetime.now()}] Batch size: {batch_size}")

                bar = tqdm(
                    pf.iter_batches(
                        batch_size,
                        use_pandas_metadata=True,
                        columns=["geometry"],
                    ),
                    total=target_num_batches,
                    colour="cyan",
                )
                for batch_i, batch in enumerate(bar):
                    bar.update(0)
                    print(
                        f"{Fore.CYAN}[{datetime.now()}] Reading parquet for batch {batch_i}...{Style.RESET_ALL}"
                    )
                    pq_table = pa.Table.from_batches([batch])
                    orig = gpd.io.arrow._arrow_to_geopandas(pq_table)
                    print(f"CRS: {orig.crs.name}")
                    print(f"Obs: {orig.shape[0]}")
                    print(orig.head(2))

                    out_before_union: gpd.GeoDataFrame = apply_grid(self.grid, orig)

                    print(f"CRS: {out_before_union.crs.name}")
                    print(f"Obs: {out_before_union.shape[0]}")
                    print(out_before_union.head(2))

                    print(
                        f"[{datetime.now()}] Saving parquet for batch {batch_i} into temporary directory {tmp_dir} ..."
                    )
                    cell_ids = out_before_union["cell_id"].unique().tolist()
                    for cell_id in cell_ids:
                        all_cell_ids.add(cell_id)
                        cell_dir = tmp_dir / f"{cell_id}"
                        in_cell: gpd.GeoDataFrame = out_before_union[
                            out_before_union.cell_id == cell_id
                        ]
                        cell_dir.mkdir(exist_ok=True, parents=True)
                        in_cell.to_parquet(cell_dir / f"batch_{batch_i}.parquet")

                    bar.update(0)

                dest_dir: Path = (
                    self.item_dir_path(period, mno, techcode) / "before_union"
                )
                dest_dir.parent.mkdir(exist_ok=True, parents=True)
                tmp_dir.rename(dest_dir)
                rename_dir_with_hash(dest_dir, src_hash=self.get_src_hash(*key))

    def generate_parquet_for_key(self, key):
        period, mno, techcode = key

        before_union_dir_path = self.item_before_union_dir_path(*key)
        if not before_union_dir_path or not before_union_dir_path.exists():
            raise Exception(
                f"{Fore.YELLOW}[{datetime.now()}] FCC_F477_grid before union directory not found: {key = }{Style.RESET_ALL}"
            )
        _, _src_hash, _ = get_hash_from_name(before_union_dir_path)
        if _src_hash != self.get_src_hash(*key):
            raise Exception(
                f"[{datetime.now()}] Src hash of the before union directory not match: {key = }"
            )

        self.backup_item_parquet(*key)

        print(f"[{datetime.now()}] Taking unary union for each cell...")

        cell_dir_paths = [p for p in before_union_dir_path.glob("*") if p.is_dir()]
        shuffle(cell_dir_paths)

        gdfs = []
        with CustomThreadPoolExecutor() as executor:
            bar = tqdm(
                executor.race_map(_FCC_F477_grid_union_worker, cell_dir_paths),
                total=len(cell_dir_paths),
            )
            bar.set_postfix({"thds": len(executor._threads)})
            bar.update(0)
            out_worker_ts = {}
            worker_ts = {}
            for part_worker_ts, part_out_worker_ts, cell_out_parts in bar:
                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)
                gdfs.extend(cell_out_parts)
                for k, v in part_out_worker_ts.items():
                    out_worker_ts[k] = out_worker_ts.get(k, 0) + v
                for k, v in part_worker_ts.items():
                    worker_ts[k] = worker_ts.get(k, 0) + v
                bar.set_postfix({"thds": len(executor._threads)})
                bar.update(0)

            for total_name, ts in [
                ("out_worker", out_worker_ts),
                ("worker", worker_ts),
            ]:
                t_total = ts[total_name]
                t_ratio = {k: v / t_total for k, v in ts.items()}
                for k in sorted(ts.keys()):
                    print(f" - {k}: {t_ratio[k]:.2%} ({ts[k]:.2f}s)")

        print(f"[{datetime.now()}] Concatenating ...")
        out: gpd.GeoDataFrame = pd.concat(gdfs)

        print(f"[{datetime.now()}]   - Saving...")

        dest_dir = self.item_dir_path(period, mno, techcode)
        dest_dir.mkdir(parents=True, exist_ok=True)
        target_file_path = dest_dir / f"{self.item_dir_name(*key)}.parquet"
        out.to_parquet(target_file_path)
        rename_with_hash(target_file_path, src_hash=self.get_src_hash(*key))

    def generate_parquet(self):
        not_exist_keys = []

        for key in self.keys():
            dest_path = self.item_parquet_path(*key)
            if not dest_path or not dest_path.exists():
                not_exist_keys.append(key)
            else:
                _, src_hash, _ = get_hash_from_name(dest_path)
                if src_hash != self.get_src_hash(*key):
                    not_exist_keys.append(key)

        if not not_exist_keys:
            print(f"[{datetime.now()}] All FCC_F477_grid files already generated.")
            return

        key_bar = tqdm(not_exist_keys, colour="yellow")
        for key in key_bar:
            key_bar.update(0)
            period, mno, techcode = key
            print(
                f"{Fore.YELLOW}[{datetime.now()}] Generating FCC_F477_grid parquet for {period}, {mno}, {techcode}...{Style.RESET_ALL}"
            )
            self.generate_parquet_for_key(key)
            print(f"[{datetime.now()}]   - Done.")


class ZCTA5_grid:
    def __init__(
        self,
        zcta5: ZCTA5,
        grid: Grid,
        dest_dir: Path,
    ):
        self.zcta5 = zcta5
        self.grid = grid
        self.dest_dir = dest_dir

    def item_parquet_path(self):
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def backup_item_parquet(self) -> Path | None:
        item_path = self.item_parquet_path()
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_src_hash(self):
        src_hash: list[str] = []

        zcta5_path = self.zcta5.item_parquet_path()
        if zcta5_path is None:
            raise FileNotFoundError(f"ZCTA5 not found.")
        zcta5_hash, _, _ = get_hash_from_name(zcta5_path)
        src_hash.append(zcta5_hash)

        grid_path = self.grid.item_parquet_path()
        if grid_path is None:
            raise FileNotFoundError(f"Grid not found.")
        grid_hash, _, _ = get_hash_from_name(grid_path)
        src_hash.append(grid_hash)

        return src_hash

    def generate(self):
        not_exist = False

        dest_path = self.item_parquet_path()
        if not dest_path or not dest_path.exists():
            not_exist = True
        else:
            _, src_hash, _ = get_hash_from_name(dest_path)
            if src_hash != self.get_src_hash():
                not_exist = True

        if not not_exist:
            print(f"[{datetime.now()}] All ZCTA5_grid files already saved.")
            return

        self.backup_item_parquet()

        print(
            f"{Fore.YELLOW}[{datetime.now()}] Generating ZCTA5_grid...{Style.RESET_ALL}"
        )
        orig_parquet_path = self.zcta5.item_parquet_path()
        assert orig_parquet_path is not None, "ZCTA5 parquet file not found."

        print(f"[{datetime.now()}]   - Reading parquet file...")
        orig: gpd.GeoDataFrame = gpd.read_parquet(orig_parquet_path)
        print(f"CRS: {orig.crs.name}")
        print(f"Obs: {orig.shape[0]}")
        print(orig.head(2))

        out: gpd.GeoDataFrame = apply_grid(self.grid, orig)

        print(f"CRS: {out.crs.name}")
        print(f"Obs: {out.shape[0]}")
        print(out.head(2))

        out: gpd.GeoDataFrame = unary_union_by_cell(
            grid=self.grid,
            orig=out[["cell_id", "ZCTA5CE10", "geometry"]],
            by=["cell_id", "ZCTA5CE10"],
        )

        print(f"CRS: {out.crs.name}")
        print(f"Obs: {out.shape[0]}")
        print(out.head(2))

        print(f"[{datetime.now()}]   - Saving...")

        dest_dir = self.dest_dir
        dest_dir.mkdir(parents=True, exist_ok=True)
        target_file_path = dest_dir / f"ZCTA5.parquet"
        out.to_parquet(target_file_path)
        rename_with_hash(target_file_path, src_hash=self.get_src_hash())
        print(f"[{datetime.now()}]   - Done.")
