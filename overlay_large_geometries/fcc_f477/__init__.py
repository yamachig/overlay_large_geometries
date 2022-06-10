from pathlib import Path
import geopandas as gpd

from overlay_large_geometries.grid import Grid
from overlay_large_geometries.fcc_f477.orig_data import FCC_F477, ZCTA5, States
from overlay_large_geometries.fcc_f477.grid import FCC_F477_grid, ZCTA5_grid
from overlay_large_geometries.fcc_f477.overlay import FCC_F477_ALL_MNOs, ZCTA5_FCC_F477
from overlay_large_geometries.fcc_f477.aggregate import ZCTA5_FCC_F477_table
from overlay_large_geometries.fcc_f477.coverage import get_coverage_df


class Dataset:
    def __init__(
        self,
        dest_dir: Path,
        *,
        periods: list[tuple[int, int]] = FCC_F477.default_periods,
        mnos: list[str] = FCC_F477.default_mnos,
        techcodes: list[str] = FCC_F477.default_techcodes,
        specific_box_urls: dict[
            tuple[int, int], str
        ] = FCC_F477.default_specific_box_urls,
    ):
        self.fcc_f477 = FCC_F477(
            dest_dir=dest_dir / "FCC_F477/",
            periods=periods,
            mnos=mnos,
            techcodes=techcodes,
            specific_box_urls=specific_box_urls,
        )

        self.zcta5 = ZCTA5(dest_dir=dest_dir / "ZCTA5/")

        self.states = States(dest_dir=dest_dir / "States/")

    def download(self):
        self.fcc_f477.download()
        self.fcc_f477.save_parquet()
        self.zcta5.download()
        self.zcta5.save_parquet()
        self.states.download()
        self.states.save_parquet()
        return self


class Coverage:
    def __init__(
        self,
        dest_dir: Path,
        dataset: Dataset,
    ):
        extent_gdf = gpd.read_parquet(dataset.states.item_parquet_path())
        self.grid = Grid(
            extent_gdf=extent_gdf,
            dest_dir=dest_dir / "Grid/",
        )
        self.fcc_f477_grid = FCC_F477_grid(
            fcc_f477=dataset.fcc_f477,
            grid=self.grid,
            dest_dir=dest_dir / "FCC_F477_grid/",
        )
        self.zcta5_grid = ZCTA5_grid(
            zcta5=dataset.zcta5,
            grid=self.grid,
            dest_dir=dest_dir / "ZCTA5_grid/",
        )
        self.fcc_f477_all_mnos = FCC_F477_ALL_MNOs(
            fcc_f477_grid=self.fcc_f477_grid,
            dest_dir=dest_dir / "FCC_F477_ALL_MNOs/",
        )
        self.zcta5_fcc_f477 = ZCTA5_FCC_F477(
            fcc_f477_all_mnos=self.fcc_f477_all_mnos,
            zcta5_grid=self.zcta5_grid,
            dest_dir=dest_dir / "ZCTA5_FCC_F477/",
        )
        self.zcta5_fcc_f477_table = ZCTA5_FCC_F477_table(
            zcta5_fcc_f477=self.zcta5_fcc_f477,
            dest_dir=dest_dir / "ZCTA5_FCC_F477/",
        )
        self.coverage_df = None

    def calculate(self):
        self.grid.generate()
        self.fcc_f477_grid.generate_before_union()
        self.fcc_f477_grid.generate_parquet()
        self.zcta5_grid.generate()
        self.fcc_f477_all_mnos.generate()
        self.zcta5_fcc_f477.generate()
        self.zcta5_fcc_f477_table.generate()
        self.zcta5_fcc_f477_table.generate_aggregate()
        self.coverage_df = get_coverage_df(
            zcta5_fcc_f477_table=self.zcta5_fcc_f477_table
        )
        return self
