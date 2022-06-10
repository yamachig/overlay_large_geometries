from pathlib import Path
import sys

TESTS_ROOT = Path(__file__).parent

if __name__ == "__main__":
    path = TESTS_ROOT
    while len(list(path.glob("__init__.py"))) > 0:
        path = path.parent
    sys.path.append(str(path))

from overlay_large_geometries.fcc_f477 import Dataset, Coverage
from overlay_large_geometries.fcc_f477.__main__ import main as fcc_f477_main


def main():

    fcc_f477_main(
        [
            "--dest-dir",
            str(TESTS_ROOT / "out_fcc_f477"),
            "--periods",
            "2014-12",
            "--mnos",
            "AT_T_Mobility",
            "--techcodes",
            "83",
        ]
    )


if __name__ == "__main__":
    main()
