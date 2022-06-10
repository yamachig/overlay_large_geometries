import pandas as pd

from overlay_large_geometries.fcc_f477.aggregate import ZCTA5_FCC_F477_table
from overlay_large_geometries.common import read_typed_csv


def get_coverage_df(
    zcta5_fcc_f477_table: ZCTA5_FCC_F477_table,
):

    mno_techcode_colnames = [
        zcta5_fcc_f477_table.mno_techcode_column_name(*key)
        for key in zcta5_fcc_f477_table.mno_techcodes()
    ]

    zcta5_covered_df: pd.DataFrame = (
        read_typed_csv(zcta5_fcc_f477_table.agg_csv_path())
        .assign(day=1)
        .assign(date=lambda df: pd.to_datetime(df[["year", "month", "day"]]))
        .rename(columns={"ZCTA5CE10": "zipcode"})
        .drop(columns=["year", "month", "day", "year_month"])
    )

    unit_area_colnames = [
        *mno_techcode_colnames,
        *[f"mno_N_{i}u" for i in range(1, len(mno_techcode_colnames))],
        *[f"mno_N_{i}" for i in range(1, len(mno_techcode_colnames) + 1)],
    ]
    zcta5_unit_area_df: pd.DataFrame = (
        zcta5_covered_df.assign(
            mno_count=lambda df: df[mno_techcode_colnames].sum(axis=1)
        )
        .assign(
            **{
                column_name: func_gen(column_name)
                for func_gen in [
                    lambda column_name: (
                        lambda df: df["area"].where(df[column_name], 0)
                    )
                ]
                for column_name in mno_techcode_colnames
            }
        )
        .assign(any_mno=lambda df: df["area"].where(df["mno_count"] > 0, 0))
        .assign(
            **{
                f"mno_N_{i}u": func_gen(i)
                for func_gen in [
                    lambda i: lambda df: df["area"].where(df["mno_count"] >= i, 0)
                ]
                for i in range(1, len(mno_techcode_colnames))
            }
        )
        .assign(
            **{
                f"mno_N_{i}": func_gen(i)
                for func_gen in [
                    lambda i: lambda df: df["area"].where(df["mno_count"] == i, 0)
                ]
                for i in range(1, len(mno_techcode_colnames) + 1)
            }
        )
        .rename(columns={"area": "ZCTA_total"})
    )

    zcta5_mno_area_df: pd.DataFrame = zcta5_unit_area_df.groupby(
        ["zipcode", "date"]
    ).agg(
        **{
            **{
                column_name: (column_name, "sum")
                for column_name in ["ZCTA_total", *unit_area_colnames]
            },
        }
    )

    coverage_df: pd.DataFrame = (
        zcta5_mno_area_df.assign(
            **{
                column_name: func_gen(column_name)
                for func_gen in [
                    lambda column_name: (
                        lambda df: df[column_name].div(zcta5_mno_area_df["ZCTA_total"])
                    )
                ]
                for column_name in unit_area_colnames
            }
        )
        .rename(columns={"ZCTA_total": "ZCTA_total_area"})
        .assign(month=lambda df: df.index.get_level_values("date").month)
        .assign(year=lambda df: df.index.get_level_values("date").year)
        .reset_index()
        .drop(columns=["date"])
        .set_index(["year", "month", "zipcode"], verify_integrity=True)
        .sort_index()
        .reset_index()
    )

    return coverage_df
