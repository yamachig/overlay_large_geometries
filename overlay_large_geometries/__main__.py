import argparse
import sys
from pathlib import Path
import json

if __name__ == "__main__":
    path = Path(__file__).parent
    while len(list(path.glob("__init__.py"))) > 0:
        path = path.parent
    sys.path.append(str(path))


from overlay_large_geometries.fcc_f477.orig_data import FCC_F477
from overlay_large_geometries.fcc_f477 import Coverage, Dataset
from overlay_large_geometries import overlay
from overlay_large_geometries.common import to_typed_csv


def get_argument_parser():
    parser = argparse.ArgumentParser(description="Process overlay of geometries.")

    parser.add_argument(
        "--dest-dir",
        type=str,
        help="Destination directory for dataset",
    )

    parser.add_argument(
        "--extent-path",
        type=str,
        help="Path to extent geometries file",
    )

    parser.add_argument(
        "--left-path",
        type=str,
        help="Path to left geometries file",
    )

    parser.add_argument(
        "--right-path",
        type=str,
        help="Path to right geometries file",
    )

    parser.add_argument(
        "--how",
        type=str,
        help="Type of overlay to perform. Please see the 'how' parameter of 'GeoDataFrame.overlay()' at https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.overlay.html",
    )

    parser.add_argument(
        "--left-columns",
        type=str,
        nargs="+",
        default=[],
        help="Columns to group left geometries",
    )

    parser.add_argument(
        "--right-columns",
        type=str,
        nargs="+",
        default=[],
        help="Columns to group right geometries",
    )

    parser.add_argument(
        "--longitude-range",
        type=str,
        default="",
        help="Longitude range to filter (ex. '-115:-118')",
    )

    parser.add_argument(
        "--latitude-range",
        type=str,
        default="",
        help="Latitude range to filter (ex. '32:35')",
    )

    return parser


def main(argv: list[str] | None = None):

    args = get_argument_parser().parse_args(argv)

    dest_dir = Path(args.dest_dir).resolve()
    extent_path = Path(args.extent_path).resolve()
    left_path = Path(args.left_path).resolve()
    right_path = Path(args.right_path).resolve()
    how = args.how
    left_columns = args.left_columns
    right_columns = args.right_columns
    longitude_range = (
        tuple(int(s) for s in args.longitude_range.split(":"))
        if args.longitude_range
        else None
    )
    latitude_range = (
        tuple(int(s) for s in args.latitude_range.split(":"))
        if args.latitude_range
        else None
    )

    print(f"- {dest_dir = }")
    print(f"- {extent_path = }")
    print(f"- {left_path = }")
    print(f"- {right_path = }")
    print(f"- {how = }")
    print(f"- {left_columns = }")
    print(f"- {right_columns = }")
    print(f"- {longitude_range = }")
    print(f"- {latitude_range = }")

    gdf, times = overlay(
        dest_dir=dest_dir,
        extent_path=extent_path,
        left_path=left_path,
        right_path=right_path,
        how=how,
        left_columns=left_columns,
        right_columns=right_columns,
        longitude_range=longitude_range,
        latitude_range=latitude_range,
    )
    gdf.to_parquet(str(dest_dir / "overlay.parquet"))
    with (dest_dir / "times.json").open("w") as f:
        json.dump(times, f)


if __name__ == "__main__":
    main()
