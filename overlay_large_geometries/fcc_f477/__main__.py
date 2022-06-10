import argparse
import sys
from pathlib import Path

if __name__ == "__main__":
    path = Path(__file__).parent
    while len(list(path.glob("__init__.py"))) > 0:
        path = path.parent
    sys.path.append(str(path))


from overlay_large_geometries.fcc_f477.orig_data import FCC_F477
from overlay_large_geometries.fcc_f477 import Coverage, Dataset
from overlay_large_geometries.common import to_typed_csv


def get_argument_parser():
    parser = argparse.ArgumentParser(description="Download and process FCC Form 477")

    parser.add_argument(
        "--dest-dir",
        type=str,
        help="Destination directory for dataset",
    )

    parser.add_argument(
        "--periods",
        type=str,
        nargs="+",
        default=" ".join(
            ["-".join(map(str, period)) for period in FCC_F477.default_periods]
        ),
        help="Periods to download (ex. '2020-06 2019-12')",
    )

    parser.add_argument(
        "--mnos",
        type=str,
        nargs="+",
        default=" ".join(FCC_F477.default_mnos),
        help="MNOs to download (ex. 'AT_T_Mobility VerizonWireless')",
    )

    parser.add_argument(
        "--techcodes",
        type=str,
        nargs="+",
        default=" ".join(FCC_F477.default_techcodes),
        help="Techcodes to download (ex. '83')",
    )

    parser.add_argument(
        "--download-only",
        type=bool,
        default=False,
        help="Download only, do not process",
    )

    return parser


def main(argv: list[str] | None = None):

    args = get_argument_parser().parse_args(argv)

    dest_dir = Path(args.dest_dir).resolve()
    periods = [tuple(int(s) for s in ps.split("-")) for ps in args.periods]
    mnos = args.mnos
    techcodes = args.techcodes
    download_only = args.download_only

    print(f"- {dest_dir = }")
    print(f"- {periods = }")
    print(f"- {mnos = }")
    print(f"- {techcodes = }")
    print(f"- {download_only = }")

    dataset = Dataset(
        dest_dir=dest_dir,
        periods=periods,
        mnos=mnos,
        techcodes=techcodes,
    ).download()

    if not download_only:
        coverage = Coverage(
            dest_dir=dest_dir,
            dataset=dataset,
        ).calculate()
        to_typed_csv(coverage.coverage_df, dest_dir / "coverage.csv")


if __name__ == "__main__":
    main()
