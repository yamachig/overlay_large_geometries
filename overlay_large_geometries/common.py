import concurrent.futures
from contextlib import contextmanager
from pathlib import Path
import re
import hashlib
from datetime import datetime
import tempfile
import time

ROOT_PATH = Path().resolve()
while len(list(ROOT_PATH.glob("__init__.py"))) > 0:
    ROOT_PATH = ROOT_PATH.parent


type_pairs = [
    ("object", "String"),
    ("float64", "Real"),
    ("int32", "Integer"),
    ("Int64", "Integer"),
    ("int64", "Integer"),
    ("bool", "Boolean"),
    ("datetime64[ns]", "DateTime"),
]
pd_to_csvt_dict = dict(type_pairs)
csvt_to_pd_dict = {k: v for v, k in type_pairs}


def to_typed_csv(df, csv_path: Path | str):
    import pandas as pd

    if isinstance(csv_path, str):
        csv_path = Path(csv_path)

    df: pd.DataFrame = df

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    csvt_df = pd.DataFrame([df.dtypes.astype(str).replace(pd_to_csvt_dict)])
    csvt_path = csv_path.with_suffix(".csvt")
    csvt_df.to_csv(csvt_path, index=False, header=False)

    return csvt_path


def read_typed_csv(csv_path: Path | str):
    import pandas as pd

    if isinstance(csv_path, str):
        csv_path = Path(csv_path)

    head_df: pd.DataFrame = pd.read_csv(csv_path, nrows=0)
    csvt_df: pd.DataFrame = pd.read_csv(csv_path.with_suffix(".csvt"), header=None)
    dtypes = {
        col: "Int64" if "int" in dtype.lower() else dtype
        for col, csvt_type in zip(head_df.columns, csvt_df.iloc[0])
        for dtype in [csvt_to_pd_dict[csvt_type]]
    }
    df: pd.DataFrame = pd.read_csv(csv_path, dtype=dtypes)
    df = df.astype(
        {
            col: "int64"
            for col, csvt_type in zip(head_df.columns, csvt_df.iloc[0])
            for dtype in [csvt_to_pd_dict[csvt_type]]
            if "int" in dtype.lower() and df[df[col].isna()].shape[0] == 0
        }
    )
    return df


def get_sha256(file_path: Path | str):
    if isinstance(file_path, str):
        file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"{file_path} not found")

    sha256 = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest().upper()


def get_hash_from_name(file_path: Path | str) -> tuple[str, list[str], str]:
    if isinstance(file_path, str):
        file_path = Path(file_path)
    hash = ""
    src_hash = []
    if m := re.search(r"\(H_([0-9a-fA-F]+)\)", file_path.stem):
        hash = m.group(1).upper()
    if m := re.search(r"\(S_([0-9a-fA-F,]+)\)", file_path.stem):
        src_hash = m.group(1).upper().split(",")
    stem = re.sub(r"\(H_[0-9a-fA-F]+\)", "", file_path.stem)
    stem = re.sub(r"\(S_[0-9a-fA-F,]+\)", "", stem)
    return hash, src_hash, stem


def rename_with_hash(file_path: Path | str, *, src_hash=[], hash_len=12):
    if isinstance(file_path, str):
        file_path = Path(file_path)
    hash = get_sha256(file_path)[:hash_len]
    stem = re.sub(r"\(H_[0-9a-fA-F]+\)", "", file_path.stem)
    stem = re.sub(r"\(S_[0-9a-fA-F,]+\)", "", stem)
    if src_hash:
        if isinstance(src_hash, str):
            raise TypeError("src_hash must be a list")
        new_path = file_path.rename(
            file_path.with_stem(
                f"{stem}(S_{','.join(src_hash).upper()})(H_{hash.upper()})"
            )
        )
    else:
        new_path = file_path.rename(file_path.with_stem(f"{stem}(H_{hash.upper()})"))
    return new_path


def rename_dir_with_hash(dir_path: Path | str, *, src_hash=[]):
    if isinstance(dir_path, str):
        dir_path = Path(dir_path)
    stem = re.sub(r"\(S_[0-9a-fA-F,]+\)", "", dir_path.stem)
    if src_hash:
        if isinstance(src_hash, str):
            raise TypeError("src_hash must be a list")
        new_path = dir_path.rename(
            dir_path.with_stem(f"{stem}(S_{','.join(src_hash).upper()})")
        )
    else:
        new_path = dir_path.rename(dir_path.with_stem(f"{stem}"))
    return new_path


def check_area(
    out,
    orig,
    *,
    threshold=1e-13,
    print_message=True,
    name="",
):
    import geopandas as gpd

    out: gpd.GeoDataFrame
    orig: gpd.GeoDataFrame
    if print_message:
        print(f"[{datetime.now()}] - Checking if areas are equal...")

    out_sum_area = out.area.sum()
    orig_sum_area = orig.area.sum()

    area_diff_ratio = (
        (out_sum_area - orig_sum_area) * 2 / (out_sum_area + orig_sum_area)
    )
    if print_message:
        print(f"[{datetime.now()}] - Area diff ratio: {area_diff_ratio}")

    if abs(area_diff_ratio) > threshold:
        error_message = f"Area sum is not equal: {area_diff_ratio} {name}"
        print(f"[{datetime.now()}] - {error_message}")
        with tempfile.TemporaryFile(delete=False, suffix=".gpkg") as f:
            out.to_file(f, driver="GPKG")
            print(f"[{datetime.now()}] - {f.name}")

        raise Exception(error_message)


def check_self_area(
    df,
    *,
    threshold=1e-13,
    print_message=True,
    name="",
):
    import geopandas as gpd

    df: gpd.GeoDataFrame = df
    if print_message:
        print(f"[{datetime.now()}] - Checking if self area is consistent...")

    sum_area = df.area.sum()
    union_area = df.unary_union.area

    area_diff_ratio = (sum_area - union_area) * 2 / (sum_area + union_area)
    if print_message:
        print(f"[{datetime.now()}] - Self area diff ratio: {area_diff_ratio}")

    if abs(area_diff_ratio) > threshold:
        error_message = f"Self area is not equal: {area_diff_ratio} {name}"
        print(f"[{datetime.now()}] - {error_message}")
        with tempfile.TemporaryFile(delete=False, suffix=".gpkg") as f:
            df.to_file(f, driver="GPKG")
            print(f"[{datetime.now()}] - {f.name}")

        raise Exception(error_message)


@contextmanager
def with_time(name: str, db: dict):
    time_0 = time.time()
    try:
        yield None
    finally:
        time_1 = time.time()
        db[name] = db.get(name, 0) + time_1 - time_0


class CustomThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def race_map(self, fn, *iterables, timeout=None):
        if timeout is not None:
            end_time = timeout + time.monotonic()

        pending = {self.submit(fn, *args) for args in (zip(*iterables))}

        def result_iterator():
            nonlocal pending
            try:
                while pending:

                    done, pending = concurrent.futures.wait(
                        pending,
                        timeout=(
                            (end_time - time.monotonic())
                            if timeout is not None
                            else None
                        ),
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )

                    for future in done:
                        yield future.result()
                        del future
                    del done
            finally:
                for future in pending:
                    future.cancel()
                    del future
                del pending

        return result_iterator()
