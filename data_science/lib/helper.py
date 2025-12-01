import sys
import enum
from functools import lru_cache
import logging
import os
from pathlib import Path
import re
# Imports above are standard Python
# Imports below are 3rd-party
import altair as alt
import pandas as pd
import polars as pl


class C(enum.Enum):
    # Datatypes
    NUMBER, DATETIME, STRING = "NUMBER", "DATETIME", "STRING"
    UTC = "UTC"


@lru_cache
def get_datatype(v: pl.Series) -> str:
    """
    | Return one of: C.STRING.value, C.NUMBER.value, C.DATETIME.value
    |
    | Examples:
    | "hi joe." --> C.STRING.value
    | True --> C.STRING.value
    | 0 --> C.NUMBER.value
    | 1.1 --> C.NUMBER.value
    | 2025-11-29 -> C.DATETIME.value
    | 2025-11-29 22:56:54.060000+00:00 -> C.DATETIME.value

    :param v: the column we are trying to type
    :return: the type
    """
    if v.dtype in pl.NUMERIC_DTYPES:
        return C.NUMBER.value
    elif v.dtype in (pl.Date, pl.Datetime, pl.Duration, pl.Time):
        return C.DATETIME.value
    else:
        return C.STRING.value


@lru_cache
def is_column_string(v: pl.Series) -> bool:
    return get_datatype(v) == C.STRING.value


@lru_cache
def is_column_number(v: pl.Series) -> bool:
    return get_datatype(v) == C.NUMBER.value


@lru_cache
def is_column_datetime(v: pl.Series) -> bool:
    return get_datatype(v) == C.DATETIME.value


@lru_cache
def get_plotting_data(data: pl.Series) -> pd.DataFrame:
    column_name = data.name
    if is_column_datetime(data):
        # Altair can plot Datetimes, but only if they are naive
        s_naive = (
            data
            .dt.convert_time_zone(C.UTC.value)  # ensure everything is UTC
            .dt.replace_time_zone(None)  # drop tz info (now naive)
        )
        return s_naive.to_frame().to_pandas()
    else:
        return data.to_frame().to_pandas()


def make_histogram(
    data: pl.Series,
    save_to_path: Path,
    width: int,
    height: int,
) -> (Path, int):
    column_name = data.name
    plot_data_df = get_plotting_data(data)
    plot_output_path = save_to_path / f"{column_name}.histogram.png"
    num_bins = min(20, len(data))
    if is_column_datetime(data):
        chart = (
            alt.Chart(plot_data_df, width=width, height=width)
            .mark_bar()
            .encode(
                x=alt.X(
                    f"{column_name}:T",
                    bin=alt.Bin(maxbins=num_bins),
                    title=column_name,
                    axis=alt.Axis(format="%Y-%m-%d", labelAngle=45)
                ),
                y=alt.Y("count()", title="count"),
            )
        )
    else:  # Numeric
        chart = (
            alt.Chart(plot_data_df, width=width, height=height)
            .mark_bar()
            .encode(
                x=alt.X(
                    f"{column_name}:Q",
                    bin=alt.Bin(maxbins=num_bins),
                    title=column_name,
                    axis=alt.Axis(labelAngle=45)
                ),
                y=alt.Y("count()", title="count"),
            )
        )
    chart.save(plot_output_path)
    return plot_output_path, os.stat(plot_output_path).st_size


def make_box_plot(
    data: pl.Series,
    save_to_path: Path,
    is_include_outliers: bool,
) -> (Path, int):
    pass