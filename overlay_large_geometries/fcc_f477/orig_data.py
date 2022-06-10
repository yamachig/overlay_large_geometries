from datetime import datetime
from math import floor
from tqdm import tqdm

from zipfile_deflate64 import ZipFile
import tempfile
from pathlib import Path
import re
import requests
from itertools import product
from colorama import Fore, Style

from overlay_large_geometries.common import (
    ROOT_PATH,
    rename_with_hash,
    get_hash_from_name,
)


# - https://www.fcc.gov/mobile-deployment-form-477-data
class FCC_F477:

    default_periods: list[tuple[int, int]] = [
        # (2020, 12),
        (2020, 6),
        (2019, 12),
        (2019, 6),
        (2018, 12),
        (2018, 6),
        (2017, 12),
        (2017, 6),
        (2016, 12),
        # (2016, 6), # Cannot open the original shapefile
        (2015, 12),
        (2014, 12),
    ]

    default_specific_box_urls = {
        (2020, 12): "https://us-fcc.app.box.com/s/ch332k9bd6ymkv96rnorodl5b52rbipx",
    }

    default_mnos = [
        "AT_T_Mobility",
        "VerizonWireless",
        "T_Mobile",
        "Sprint",
    ]

    default_techcodes = [
        "83",  # LTE
    ]

    def __init__(
        self,
        dest_dir: Path,
        *,
        periods: list[tuple[int, int]] = default_periods,
        mnos: list[str] = default_mnos,
        techcodes: list[str] = default_techcodes,
        specific_box_urls: dict[tuple[int, int], str] = default_specific_box_urls,
    ):
        self.dest_dir = dest_dir
        self.__box_client = None

        self.periods = periods
        self.specific_box_urls = specific_box_urls
        self.mnos = mnos
        self.techcodes = techcodes

    def keys(self):
        return list(product(self.periods, self.mnos, self.techcodes))

    def item_dir_name(self, period, mno, techcode):
        return f"{mno}_{period[0]}_{period[1]:02d}_{techcode}"

    def item_dir_path(self, period, mno, techcode) -> Path | None:
        return self.dest_dir / self.item_dir_name(period, mno, techcode)

    def item_parquet_path(self, period, mno, techcode):
        dir = self.item_dir_path(period, mno, techcode)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def item_zip_paths(self, period, mno, techcode) -> list[Path]:
        ret = []
        dir = self.item_dir_path(period, mno, techcode)
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".zip":
                    ret.append(p)
        ret = sorted(ret, key=lambda p: p.name)
        return ret

    def backup_item_parquet(self, period, mno, techcode) -> Path | None:
        item_path = self.item_parquet_path(period, mno, techcode)
        if item_path and item_path.exists():
            return item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

    def get_src_hash(self, period, mno, techcode, *, conv_hash_len: int | None = None):
        src_hash: list[str] = []
        for p in self.item_zip_paths(period, mno, techcode):
            orig_hash, _, _ = get_hash_from_name(p)
            if conv_hash_len:
                orig_hash = orig_hash[:conv_hash_len]
            src_hash.append(orig_hash)
        return src_hash

    def box_client(self):
        if self.__box_client:
            return self.__box_client

        from boxsdk import JWTAuth, Client

        # https://github.com/box/box-python-sdk/tree/main/docs/usage
        # https://app.box.com/developers/console
        box_auth = JWTAuth.from_settings_file(ROOT_PATH / ".box.json")
        box_client = Client(box_auth)
        box_user = box_client.user().get()
        print(f"[{datetime.now()}] Service Account user ID is {box_user.id}")

        self.__box_client = box_client
        return box_client

    def _download_item(self, period, mno, techcode, tempDir: str):
        from bs4 import BeautifulSoup

        dest_dir = self.item_dir_path(period, mno, techcode)

        box_client = self.box_client()
        print(f"Downloading FCC_F477 for {period[0]}/{period[1]} {mno} {techcode}")
        dest_dir.mkdir(parents=True, exist_ok=True)

        mno_zip_path = Path(tempDir) / f"{mno}_{period[0]}_{period[1]:02d}.zip"

        if not mno_zip_path.exists():
            print(
                f"[{datetime.now()}]   - Downloading intermediate zip file into {mno_zip_path}"
            )
            if period in self.specific_box_urls:
                href1 = self.specific_box_urls[period]
            else:
                res = requests.get(
                    "https://www.fcc.gov/mobile-deployment-form-477-data"
                )
                doc = BeautifulSoup(res.text, "html.parser")
                link1s = doc.select(
                    "#content > div.panel-pane.pane-node-content > div > div > article > div.field.field-name-body.field-type-text-with-summary.field-label-hidden > div > div > div:nth-child(8) > table > tbody > tr > td:nth-child(1) > a"
                )
                month_dict = {6: "June", 12: "Dec."}
                for link1 in link1s:
                    if link1.string == f"{month_dict[period[1]]} {period[0]}":
                        break
                href1 = link1["href"]

            folder1 = box_client.get_shared_item(href1).get()
            folder2 = None
            while folder2 is None:
                if folder1.name in [
                    "Mobile Broadband Deployment by Provider",
                    "Mobile Broadband Coverage by Provider",
                ]:
                    folder2 = folder1
                    break
                for item1 in folder1.get_items():
                    if item1.type == "folder" and item1.name in [
                        "Mobile Broadband Deployment by Provider",
                        "Mobile Broadband Coverage by Provider",
                    ]:
                        folder2 = item1
                        break
                if folder2 is None:
                    next_folder1 = None
                    for item1 in folder1.get_items():
                        if item1.type == "folder" and item1.name in [
                            "Shapefiles",
                        ]:
                            next_folder1 = item1
                            break
                    if next_folder1 is None:
                        break
                    folder1 = next_folder1

            for item2 in folder2.get_items():
                if item2.type == "file" and mno in item2.name:
                    file3 = item2.get()

            with mno_zip_path.open("wb") as f:
                file3.download_to(f)

        target_filename = None
        with ZipFile(mno_zip_path) as zf:
            targetMembers = []
            for member in zf.infolist():
                if not member.is_dir() and re.search(
                    rf"_{techcode}[_.]", member.filename
                ):
                    targetMembers.append(member)
            if not targetMembers:
                print(zf.namelist())
            for member in targetMembers:
                print(f"[{datetime.now()}]   - Extracting file {member.filename}")
                target_filename = zf.extract(member, dest_dir)
                rename_with_hash(target_filename)

        print(f"[{datetime.now()}]   - Done.")

    def download(self):

        not_exist_keys = [
            key
            for key in self.keys()
            for item_zip_path in [self.item_zip_paths(*key)]
            if not item_zip_path
        ]

        if not not_exist_keys:
            print(f"[{datetime.now()}] All FCC_F477 zip files already downloaded.")
            return

        print(f"{Fore.YELLOW}[{datetime.now()}] Downloading FCC_F477{Style.RESET_ALL}")

        with tempfile.TemporaryDirectory() as tempDir:
            print(f"[{datetime.now()}] Using temporary directory {tempDir}")

            for period, mno, techcode in not_exist_keys:
                self._download_item(period, mno, techcode, tempDir)

    def save_parquet(self):
        not_exist_keys = []

        for key in self.keys():
            item_path = self.item_parquet_path(*key)
            if not item_path or not item_path.exists():
                not_exist_keys.append(key)
            else:
                _, src_hash, _ = get_hash_from_name(item_path)
                conv_hash_len = len(src_hash[0]) if src_hash else None
                if src_hash != self.get_src_hash(*key, conv_hash_len=conv_hash_len):
                    not_exist_keys.append(key)

        if not not_exist_keys:
            print(f"[{datetime.now()}] All FCC_F477 parquet files already saved.")
            return

        import pandas as pd
        import geopandas as gpd

        for key in not_exist_keys:
            period, mno, techcode = key
            self.backup_item_parquet(*key)
            dest_dir = self.item_dir_path(*key)
            print(
                f"[{datetime.now()}] Saving parquet of FCC_F477 for {period[0]}/{period[1]} {mno} {techcode}"
            )
            parquet_path = dest_dir / f"{self.item_dir_name(*key)}.parquet"

            ret_gdf = None
            zip_item_paths = self.item_zip_paths(*key)
            for zip_path in zip_item_paths:
                print(f"[{datetime.now()}]   - Reading zipped shapefile...")
                try:
                    gdf: gpd.GeoDataFrame = gpd.read_file(
                        zip_path, driver="ESRI Shapefile"
                    )
                except ValueError:
                    print(
                        f"[{datetime.now()}]   - Reading zipped shapefile failed. Decompressing..."
                    )
                    with tempfile.TemporaryDirectory() as tempDir:
                        with ZipFile(zip_path) as zf:
                            zf.extractall(tempDir)
                        shp_path = Path(tempDir).glob("*.shp").__next__()
                        print(
                            f"[{datetime.now()}]   - Reading decompressed shapefile..."
                        )
                        gdf: gpd.GeoDataFrame = gpd.read_file(
                            shp_path, driver="ESRI Shapefile"
                        )
                print(f"[{datetime.now()}]   - Manipulating GeoDataFrame...")
                gdf: gpd.GeoDataFrame = (
                    gdf.reset_index()
                    .rename(columns={"index": "__orig_index"})
                    .explode(ignore_index=True)
                    .to_crs("EPSG:4326")
                )
                ret_gdf = (
                    gdf
                    if ret_gdf is None
                    else pd.concat([ret_gdf, gdf], ignore_index=True)
                )
                del gdf
            print(f"[{datetime.now()}]   - Saving parquet...")
            ret_gdf.to_parquet(parquet_path)
            conv_hash_len = floor(12 * min(1, 4 / len(zip_item_paths)))
            rename_with_hash(
                parquet_path,
                src_hash=self.get_src_hash(*key, conv_hash_len=conv_hash_len),
            )
            print(f"[{datetime.now()}]   - Done.")


# - https://catalog.data.gov/dataset/tiger-line-shapefile-2019-2010-nation-u-s-2010-census-5-digit-zip-code-tabulation-area-zcta5-na
class ZCTA5:
    def __init__(self, dest_dir: Path = ROOT_PATH / "orig_data/ZCTA5/"):
        self.dest_dir = dest_dir

    def item_parquet_path(self) -> Path | None:
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def item_zip_path(self) -> Path | None:
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".zip":
                    return p

    def download(self):

        if self.item_zip_path():
            print(f"[{datetime.now()}] All ZCTA5 zip files already downloaded.")
            return

        print(f"{Fore.YELLOW}[{datetime.now()}] Downloading ZCTA5{Style.RESET_ALL}")
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        url = (
            f"https://www2.census.gov/geo/tiger/TIGER2019/ZCTA5/tl_2019_us_zcta510.zip"
        )
        target_file_path = self.dest_dir / url.split("/")[-1]
        total_bytes = int(requests.head(url).headers["Content-Length"])
        print(f"[{datetime.now()}]   - {total_bytes = :,d}...")
        with requests.get(url, stream=True) as res:
            with tqdm(total=total_bytes, unit="B", unit_scale=True) as bar:
                with target_file_path.open("wb") as f:
                    for chunk in res.iter_content(chunk_size=2**20):
                        bar.update(len(chunk))
                        f.write(chunk)
        rename_with_hash(target_file_path)
        print(f"[{datetime.now()}]   - Done.")

    def save_parquet(self):
        not_exist = False

        item_path = self.item_parquet_path()
        if not item_path or not item_path.exists():
            not_exist = True
        else:
            zip_path = self.item_zip_path()
            _, src_hash, stem = get_hash_from_name(item_path)
            zip_hash, _, stem = get_hash_from_name(zip_path)
            if src_hash != [zip_hash]:
                not_exist = True

        if not not_exist:
            print(f"[{datetime.now()}] All ZCTA5 parquet files already saved.")
            return

        if item_path:
            item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

        import geopandas as gpd

        dest_dir = self.dest_dir
        zip_path = self.item_zip_path()
        zip_hash, _, stem = get_hash_from_name(zip_path)
        print(f"[{datetime.now()}] Saving parquet of ZCTA5")
        parquet_path = dest_dir / f"{stem}.parquet"
        gdf: gpd.GeoDataFrame = (
            gpd.read_file(zip_path)
            .reset_index()
            .rename(columns={"index": "__orig_index"})
            # .explode(ignore_index=True)
            .to_crs("EPSG:4326")
        )
        gdf.to_parquet(parquet_path)
        rename_with_hash(parquet_path, src_hash=[zip_hash])
        print(f"[{datetime.now()}]   - Done.")


# - https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.html
class States:
    def __init__(self, dest_dir: Path = ROOT_PATH / "orig_data/States/"):
        self.dest_dir = dest_dir

    def item_parquet_path(self) -> Path | None:
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".parquet":
                    return p

    def item_zip_path(self) -> Path | None:
        dir = self.dest_dir
        if dir.exists():
            for p in dir.iterdir():
                if p.suffix == ".zip":
                    return p

    def download(self):

        if self.item_zip_path():
            print(f"[{datetime.now()}] All States zip files already downloaded.")
            return

        print(f"{Fore.YELLOW}[{datetime.now()}] Downloading States{Style.RESET_ALL}")
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        url = (
            f"https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_500k.zip"
        )
        target_file_path = self.dest_dir / url.split("/")[-1]
        total_bytes = int(requests.head(url).headers["Content-Length"])
        print(f"[{datetime.now()}]   - {total_bytes = :,d}...")
        with requests.get(url, stream=True) as res:
            with tqdm(total=total_bytes, unit="B", unit_scale=True) as bar:
                with target_file_path.open("wb") as f:
                    for chunk in res.iter_content(chunk_size=2**20):
                        bar.update(len(chunk))
                        f.write(chunk)
        rename_with_hash(target_file_path)
        print(f"[{datetime.now()}]   - Done.")

    def save_parquet(self):
        not_exist = False

        item_path = self.item_parquet_path()
        if not item_path or not item_path.exists():
            not_exist = True
        else:
            zip_path = self.item_zip_path()
            _, src_hash, stem = get_hash_from_name(item_path)
            zip_hash, _, stem = get_hash_from_name(zip_path)
            if src_hash != [zip_hash]:
                not_exist = True

        if not not_exist:
            print(f"[{datetime.now()}] All States parquet files already saved.")
            return

        if item_path:
            item_path.rename(
                item_path.with_suffix(
                    f"{item_path.suffix}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.old"
                )
            )

        import geopandas as gpd

        dest_dir = self.dest_dir
        zip_path = self.item_zip_path()
        zip_hash, _, stem = get_hash_from_name(zip_path)
        print(f"[{datetime.now()}] Saving parquet of States")
        parquet_path = dest_dir / f"{stem}.parquet"
        gdf: gpd.GeoDataFrame = (
            gpd.read_file(zip_path)
            .reset_index()
            .rename(columns={"index": "__orig_index"})
            .to_crs("EPSG:4326")
        )
        gdf.to_parquet(parquet_path)
        rename_with_hash(parquet_path, src_hash=[zip_hash])
        print(f"[{datetime.now()}]   - Done.")
