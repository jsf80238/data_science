import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime
from pathlib import Path
import os
import random
import re
import shutil
from statistics import mean, quantiles, stdev
import sys
import tempfile

import numpy as np
# Imports above are standard Python
# Imports below are 3rd-party
from argparse_range import range_action
import dateutil.parser
from dotenv import dotenv_values
import openpyxl
from openpyxl.styles import Border, Side, Alignment, Font, borders
from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
# Imports below are custom
from lib.base import C, Database, Logger, get_line_count

# Excel limitation
MAX_SHEET_NAME_LENGTH = 31
# Excel output
ROUNDING = 1  # 5.4% for example
# Headings for Excel output
VALUE, COUNT = "Value", "Count"
# Datatypes
NUMBER, DATETIME, STRING = "NUMBER", "DATETIME", "STRING"
# When producing a list of detail values and their frequency of occurrence
DEFAULT_MAX_DETAIL_VALUES = 35
DETAIL_ABBR = " det"
# When analyzing the patterns of a string column
DEFAULT_LONGEST_LONGEST = 50  # Don't display more than this
DEFAULT_MAX_PATTERN_LENGTH = 50
PATTERN_ABBR = " pat"
# Don't plot histograms/boxes if there are fewer than this number of distinct values
PLOT_MIN_VALUES = 6
# Categorical plots should have no more than this number of distinct values
CATEGORICAL_PLOT_MAX_VALUES = 5
# When determining the datatype of a CSV column examine (up to) this number of records
DATATYPE_SAMPLING_SIZE = 500
# Plotting visual effects
PLOT_SIZE_X, PLOT_SIZE_Y = 11, 8.5
PLOT_FONT_SCALE = 0.75
# Good character for spreadsheet-embedded histograms U+25A0
BLACK_SQUARE = "■"
# Output can be to Excel or HTML
EXCEL = "EXCEL"
HTML = "HTML"
OPEN, CLOSE = "{", "}"


DATATYPE_MAPPING_DICT = {
    "BIGINT": NUMBER,
    "BINARY": NUMBER,
    "BIT": NUMBER,
    "BOOLEAN": NUMBER,
    "DECIMAL": NUMBER,
    "DOUBLE": NUMBER,
    "FLOAT": NUMBER,
    "INTEGER": NUMBER,
    "NUMERIC": NUMBER,
    "REAL": NUMBER,
    "SMALLINT": NUMBER,
    "TINYINT": NUMBER,
    "VARBINARY": NUMBER,

    "DATE": DATETIME,
    "TIMESTAMP": DATETIME,

    "BLOB": STRING,
    "CHAR": STRING,
    "CLOB": STRING,
    "LONGNVARCHAR": STRING,
    "LONGVARBINARY": STRING,
    "LONGVARCHAR": STRING,
    "NCHAR": STRING,
    "NCLOB": STRING,
    "NVARCHAR": STRING,
    "OTHER": STRING,
    "SQLXML": STRING,
    "TIME": STRING,
    "VARCHAR": STRING,
}

ROW_COUNT = "count"
NULL_COUNT = "null"
NULL_PERCENT = "%null"
UNIQUE_COUNT = "unique"
UNIQUE_PERCENT = "%unique"
MOST_COMMON = "most_common"
MOST_COMMON_PERCENT = "%most_common"
LARGEST = "largest"
SMALLEST = "smallest"
LONGEST = "longest"
SHORTEST = "shortest"
MEAN = "mean"
PERCENTILE_25TH = "percentile_25th"
MEDIAN = "median"
PERCENTILE_75TH = "percentile_75th"
STDDEV = "stddev"

ANALYSIS_LIST = (
    ROW_COUNT,
    NULL_COUNT,
    NULL_PERCENT,
    UNIQUE_COUNT,
    UNIQUE_PERCENT,
    MOST_COMMON,
    MOST_COMMON_PERCENT,
    LARGEST,
    SMALLEST,
    LONGEST,
    SHORTEST,
    MEAN,
    PERCENTILE_25TH,
    MEDIAN,
    PERCENTILE_75TH,
    STDDEV,
)


def convert_str_to_float(value: str) -> float:
    """
    Convert CSV strings to a useful data type.

    :param value: the value from the CSV column
    :return: the data converted to float
    """
    if value:
        return float(value)
    else:
        return None


def convert_str_to_datetime(value: str) -> datetime:
    """
    Convert CSV strings to a useful data type.

    :param values: the value from the CSV column
    :return: the data converted to float
    """
    if value:
        return dateutil.parser.parse(value)
    else:
        return None


def make_sheet_name(s: str, max_length: int, filler: str = "...") -> str:
    """
    For example, make_sheet_name("Hello world!", 7) returns:
    "Hell..."
    """
    # Remove []:*?/\ as these are not valid for sheet names
    illegal_chars = "[]:*?/\\"
    translation_map = str.maketrans(illegal_chars, "_"*len(illegal_chars))
    s = s.translate(translation_map)
    excess_count = len(s) - max_length
    if excess_count <= 0:
        return s
    else:
        return s[:max_length - len(filler)] + filler


def get_pattern(l: list) -> dict:
    """
    | Return a Counter where the keys are the observed patterns and the values are how often they appear.
    |
    | Examples:
    | "hi joe." --> "CC_C(3)"
    | "hello4abigail" --> "C(5)9C(7)"
    | "this+unexpected-   9" --> "C(4)?C(10)?_(3)9"

    :param l: a list of strings
    :return: a pattern analysis
    """
    counter = Counter()
    for value in l:
        if not value or len(value) > max_pattern_length:
            continue
        value = re.sub("[a-zA-Z]", "C", value)  # Replace letters with 'C'
        value = re.sub(r"\d", "9", value)  # Replace numbers with '9'
        value = re.sub(r"\s+", "_", value)  # Replace whitespace with '_'
        value = re.sub(r"\W", "?", value)  # Replace anything else with '?'
        # Group long sequences of letters or numbers
        # See https://stackoverflow.com/questions/76230795/replace-characters-with-a-count-of-characters
        # The number below (2) means sequences of 3 or more will be grouped
        value = re.sub(r'(.)\1{2,}', lambda m: f'{m.group(1)}({len(m.group())})', value)
        counter[value] += 1
    return counter


def make_html_header(title: str, root_output_file: str = None) -> str:
    """
    | Creates the first part of an HTML file.
    | Used when creating HTML output.

    :param title: displayed by the browser
    :param root_output_file: path to home page, None if creating the home page
    :return: <!DOCTYPE html> <html lang="en-US"> <head> ...
    """
    if root_output_file:
        home_link = f'<a href="../{root_output_file.name}">Home</a>'
    else:
        home_link = ""
    return f"""
    <!DOCTYPE html>
    <html lang="en-US">
        <head>
        <meta charset="utf-8">
        <title>Exploratory Data Analysis for {title}</title>
        <style>
            html {OPEN}
                font-family: monospace;
                font-size: smaller;
            {CLOSE}
            h1, h2 {OPEN}
                text-align: center;
            {CLOSE}
            table, tr, td {OPEN}
                border: 1px solid #000;
            {CLOSE}
        </style>
        </head>
        <body>
        {home_link}
    """


def make_html_footer() -> str:
    """
    | Helper function

    :return: </body> </html>
    """
    return f"""
            </body>
        </html>
    """


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Profile the data in a database or CSV file. Generates an analysis consisting tables and images stored in an Excel workbook or HTML pages. For string columns provides a pattern analysis with C replacing letters, 9 replacing numbers, underscore replacing spaces, and question mark replacing everything else. For numeric and datetime columns produces a histogram and box plots.')

    parser.add_argument('input',
                        metavar="/path/to/input_data_file.csv | query-against-database",
                        help="An example query is 'select a, b, c from t where x>7'.")
    parser.add_argument('--header-lines',
                        type=int,
                        metavar="NUM",
                        action=range_action(1, sys.maxsize),
                        default=0,
                        help="When reading from a file specifies the number of rows to skip UNTIL the header row. Ignored when getting data from a database. Default is 0.")
    parser.add_argument('--sample-rows-file',
                        type=int,
                        metavar="NUM",
                        action=range_action(1, sys.maxsize),
                        help=f"When reading from a file randomly choose this number of rows. If greater than or equal to the number of data rows will use all rows. Ignored when getting data from a database.")
    parser.add_argument('--max-detail-values',
                        type=int,
                        metavar="NUM",
                        action=range_action(1, sys.maxsize),
                        default=DEFAULT_MAX_DETAIL_VALUES,
                        help=f"Produce this many of the top/bottom value occurrences, default is {DEFAULT_MAX_DETAIL_VALUES}.")
    parser.add_argument('--max-pattern-length',
                        type=int,
                        metavar="NUM",
                        action=range_action(1, sys.maxsize),
                        default=DEFAULT_MAX_PATTERN_LENGTH,
                        help=f"When segregating strings into patterns leave untouched strings of length greater than this, default is {DEFAULT_MAX_PATTERN_LENGTH}.")
    parser.add_argument('--max-longest-string',
                        type=int,
                        metavar="NUM",
                        action=range_action(50, sys.maxsize),
                        default=DEFAULT_LONGEST_LONGEST,
                        help=f"When displaying long strings show a summary if string exceeds this length, default is {DEFAULT_LONGEST_LONGEST}.")
    parser.add_argument('--target-dir',
                        metavar="/path/to/dir",
                        default=Path.cwd(),
                        help="Default is the current directory. Will make intermediate directories as necessary.")
    parser.add_argument('--html',
                        action='store_true',
                        help="Also produce a zip file containing the results in HTML format.")
    parser.add_argument('--db-host-name',
                        metavar="HOST_NAME",
                        help="Overrides HOST_NAME environment variable. Ignored when getting data from a file.")
    parser.add_argument('--db-port-number',
                        metavar="PORT_NUMBER",
                        help="Overrides PORT_NUMBER environment variable. Ignored when getting data from a file.")
    parser.add_argument('--db-name',
                        metavar="DATABASE_NAME",
                        help="Overrides DATABASE_NAME environment variable. Ignored when getting data from a file.")
    parser.add_argument('--db-user-name',
                        metavar="USER_NAME",
                        help="Overrides USER_NAME environment variable. Ignored when getting data from a file.")
    parser.add_argument('--db-password',
                        metavar="PASSWORD",
                        help="Overrides PASSWORD environment variable. Ignored when getting data from a file.")
    parser.add_argument('--environment-file',
                        metavar="/path/to/file",
                        help="An additional source of database connection information. Overrides environment settings.")

    logging_group = parser.add_mutually_exclusive_group()
    logging_group.add_argument('--verbose', action='store_true')
    logging_group.add_argument('--terse', action='store_true')

    args = parser.parse_args()
    if args.input.endswith(C.CSV_EXTENSION):
        input_path = Path(args.input)
        input_query = ""
    else:
        input_path = ""
        input_query = args.input
    host_name = args.db_host_name
    port_number = args.db_port_number
    database_name = args.db_name
    user_name = args.db_user_name
    password = args.db_password
    if args.environment_file:
        environment_file = Path(args.environment_file)
    else:
        environment_file = ""
    header_lines = args.header_lines
    sample_rows_file = args.sample_rows_file
    max_detail_values = args.max_detail_values
    max_pattern_length = args.max_pattern_length
    max_longest_string = args.max_longest_string
    is_html_output = args.html
    is_excel_output = True
    target_dir = Path(args.target_dir)

    environment_settings_dict = {
        **os.environ,
        **dotenv_values(environment_file),
    }
    if not target_dir.parent.exists():
        parser.error("Directory '{output_dir.parent}' does not exist.")
    else:
        os.makedirs(target_dir, exist_ok=True)

    if input_query:
        # Verify we have the information we need to connect to the database
        host_name = host_name or environment_settings_dict.get("HOST_NAME")
        port_number = port_number or environment_settings_dict.get("PORT_NUMBER")
        database_name = database_name or environment_settings_dict.get("DATABASE_NAME")
        user_name = user_name or environment_settings_dict.get("USER_NAME")
        password = password or environment_settings_dict.get("PASSWORD")
        if not (host_name and port_number and database_name and user_name and password):
            parser.error("Connecting to a database requires environment variables and/or environment file and/or --db-host-name, --db-port-number, --db-name, --db-user-name, --db-password")
    elif input_path:
        if not input_path.exists():
            parser.error(f"Could not find input file '{input_path}'.")
    else:
        raise Exception("Programming error.")

    if args.verbose:
        logger = Logger("DEBUG").get_logger()
    elif args.terse:
        logger = Logger("WARNING").get_logger()
    else:
        logger = Logger().get_logger()

    # Now, read the data
    data_dict = defaultdict(list)
    # ↑ Keys are column_names, values are a list of values from the data.
    non_null_data_dict = dict()
    # ↑ A list of non-null values is commonly of interest, calculate it once.
    # Keys are column_names, values are a list of non-null values (if any) from the data.
    datatype_dict = dict()  #
    # ↑ Keys are column_names, values are the type of data (NUMBER, DATETIME, STRING)
    if input_query:
        # Data is coming from a database query
        mydb = Database(
            host_name=host_name,
            port_number=port_number,
            database_name=database_name,
            user_name=user_name,
            password=password
        )
        cursor, column_list = mydb.execute(input_query)
        for r in cursor.fetchall():
            row = dict(zip(column_list, r))
            # Store data
            for column_name, value in row.items():
                data_dict[column_name].append(value)
        # Determine datatype
        logger.info("Data read.")
        for item in cursor.description:
            column_name, dbapi_type_code, display_size, internal_size, precision, scale, null_ok = item
            type_code_desc = str(dbapi_type_code).upper()
            # ↑ Converts things like DBAPITypeObject('BOOLEAN', 'BIGINT', 'BIT', 'INTEGER', 'SMALLINT', 'TINYINT') to
            # "DBAPITypeObject('BOOLEAN', 'BIGINT', 'BIT', 'INTEGER', 'SMALLINT', 'TINYINT')", which is good enough
            # to determine the datatype.
            for key in DATATYPE_MAPPING_DICT:
                if key in type_code_desc:
                    datatype_dict[column_name] = DATATYPE_MAPPING_DICT[key]
                    logger.info(f"Read column '{column_name}' as {DATATYPE_MAPPING_DICT[key]}.")
                    break
            else:
                logger.error(f"Could not determine data type for column '{column_name}' based on the JDBC metadata: {str(item)}")
                datatype_dict[column_name] = STRING
        # Non-null data is useful for later calculations
        for column_name, values in data_dict.items():
            non_null_data_dict[column_name] = [x for x in values if x]
        # Sometimes the JDBC API returns strings for values its metadata says are dates/datetimes.
        # Convert these as necessary from Python strings to Python datetimes
        for column_name, values in data_dict.items():
            if datatype_dict[column_name] == DATETIME:
                # Check the type of the first non-null value
                if type(non_null_data_dict[column_name][0]) == str:
                    data_dict[column_name] = list(map(lambda x: dateutil.parser.parse(x), data_dict[column_name]))
                    non_null_data_dict[column_name] = [x for x in data_dict[column_name] if x]

    elif input_path:
        # Data is coming from a file
        logger.info(f"Reading from '{input_path}' ...")
        # Manage sampling, if any
        # For example, suppose the file contains 100 lines and we want to sample 20 of them.
        # The ratio is 20/100, or 0.2, and if a random number between 0 and 1 is less than 0.2
        # then we will include that row.
        if sample_rows_file:
            ratio = sample_rows_file / get_line_count(input_path)
        else:
            ratio = 1  # Include all rows
        with open(input_path, newline="", encoding="utf-8") as csvfile:
            csvreader = csv.DictReader(csvfile)
            for i, row in enumerate(csvreader, 1):
                if i <= header_lines:
                    continue
                if ratio >= 1 or random.random() < ratio:
                    for column_name, value in row.items():
                        column_name = column_name.replace("/", "_")  # slashes don't work well in paths
                        data_dict[column_name].append(value)
        # Set best type for each column of data
        for column_name, values in data_dict.items():
            # Sample up to DATATYPE_SAMPLING_SIZE non-null values
            non_null_list = [x for x in values if x]
            sampled_list = random.sample(non_null_list, min(DATATYPE_SAMPLING_SIZE, len(non_null_list)))
            is_parse_error = False
            for item in sampled_list:
                if len(str(item)) < 6:  # dateutil.parser.parse seems to interpret things like 2.0 as dates
                    is_parse_error = True
                    logger.debug(f"Cannot cast column '{column_name}' as a datetime.")
                    break
                try:
                    dateutil.parser.parse(item)
                except:
                    is_parse_error = True
                    logger.debug(f"Cannot cast column '{column_name}' as a datetime.")
                    break
            if not is_parse_error:
                logger.info(f"Casting column '{column_name}' as a datetime.")
                data_dict[column_name] = list(map(convert_str_to_datetime, values))
                datatype_dict[column_name] = DATETIME
            else:
                # Not a datetime, try number
                is_parse_error = False
                for item in sampled_list:
                    try:
                        float(item)
                    except ValueError:
                        is_parse_error = True
                        logger.debug(f"Cannot cast column '{column_name}' as a number.")
                        break
                if not is_parse_error:
                    logger.info(f"Casting column '{column_name}' as a number.")
                    data_dict[column_name] = list(map(convert_str_to_float, values))
                    datatype_dict[column_name] = NUMBER
                else:
                    logger.info(f"Casting column '{column_name}' as a string.")
                    datatype_dict[column_name] = STRING
            # Non-null data is useful for later calculations
            non_null_data_dict[column_name] = [x for x in data_dict[column_name] if x]

    # Data has been read into input_df, now process it
    # To temporarily hold plots and html files
    tempdir = tempfile.TemporaryDirectory()
    tempdir_path = Path(tempdir.name)
    # To keep track of which columns have histogram plots
    histogram_plot_list = list()
    box_plot_list = list()

    summary_dict = dict()  # To be converted into the summary worksheet
    detail_dict = dict()  # Each element to be converted into a detail worksheet
    pattern_dict = dict()  # For each string column calculate the frequency of patterns
    for column_name, values in data_dict.items():  # values is a list of the values for this column
        if False and not column_name.startswith("L"):  # For testing
            continue
        if not len(values):
            logger.critical(f"There is no data in '{input_path+input_query}'.")
            exit()
        datatype = datatype_dict[column_name]
        # A list of non-null values are useful for some calculations below
        non_null_values = non_null_data_dict[column_name]
        datatype = datatype_dict[column_name]
        logger.info(f"Working on column '{column_name}' ...")
        column_dict = dict.fromkeys(ANALYSIS_LIST)
        # Row count
        row_count = len(values)
        column_dict[ROW_COUNT] = row_count
        # Null
        null_count = row_count - len(non_null_values)
        column_dict[NULL_COUNT] = null_count
        # Null%
        column_dict[NULL_PERCENT] = round(100 * null_count / row_count, ROUNDING)
        # Unique
        unique_count = len(set(values))
        column_dict[UNIQUE_COUNT] = unique_count
        # Unique%
        column_dict[UNIQUE_PERCENT] = round(100 * unique_count / row_count, ROUNDING)

        if null_count != row_count:
            # Largest & smallest
            column_dict[LARGEST] = max(non_null_values)
            column_dict[SMALLEST] = min(non_null_values)

            if datatype == STRING:
                # Longest & shortest
                column_dict[SHORTEST] = min(non_null_values, key=len)
                longest_string = max(non_null_values, key=len)
                if longest_string and len(longest_string) > max_longest_string:
                    # This string is really long and won't display nicely ... so adjust, for example:
                    #   I'm unhappy with my collection of boring clothes. In striving to cut back on wasteful purchases, and to keep a tighter closet, I have sucked all of the fun out of my closet.
                    # Will be replaced with:
                    # I'm u...(actual length is 173 characters)...oset.
                    placeholder = f"...(actual length is {len(longest_string)} characters)..."
                    prefix = longest_string[:5]
                    suffix = longest_string[-5:]
                    longest_string = prefix + placeholder + suffix
                column_dict[LONGEST] = longest_string
                # No mean/quartiles/stddev statistics for strings
            elif datatype == NUMBER:
                # No longest/shortest for numbers and dates
                column_dict[SHORTEST] = np.nan
                column_dict[LONGEST] = np.nan
                # Mean/quartiles/stddev statistics
                column_dict[MEAN] = mean(non_null_values)
                column_dict[STDDEV] = stdev(non_null_values)
                column_dict[PERCENTILE_25TH], column_dict[MEDIAN], column_dict[PERCENTILE_75TH] = quantiles(non_null_values)
            elif datatype == DATETIME:
                # No longest/shortest for numbers and dates
                column_dict[SHORTEST] = np.nan
                column_dict[LONGEST] = np.nan
                # Mean/quartiles/stddev statistics
                values_as_epoch_seconds = [x.timestamp() for x in non_null_values]
                column_dict[MEAN] = datetime.fromtimestamp(mean(values_as_epoch_seconds))
                column_dict[STDDEV] = stdev(
                    values_as_epoch_seconds) / 24 / 60 / 60  # Report standard deviation of datetimes in units of days
                twenty_five, fifty, seventy_five = quantiles(values_as_epoch_seconds)
                column_dict[PERCENTILE_25TH], column_dict[MEDIAN], column_dict[PERCENTILE_75TH] = datetime.fromtimestamp(
                    twenty_five), datetime.fromtimestamp(fifty), datetime.fromtimestamp(seventy_five)
            else:
                raise Exception("Programming error.")

            summary_dict[column_name] = column_dict

            # Value counts
            # Collect no more than number of values available or what was given on the command-line
            # whichever is less
            counter = Counter(values)
            max_length = min(max_detail_values, len(non_null_values))
            most_common_list = counter.most_common(max_length)
            most_common, most_common_count = most_common_list[0]
            column_dict[MOST_COMMON] = most_common
            column_dict[MOST_COMMON_PERCENT] = round(100 * most_common_count / row_count, ROUNDING)
            detail_df = pd.DataFrame()
            # Create 3-column descending visual
            detail_df["rank"] = list(range(1, len(most_common_list) + 1))
            detail_df["value"] = [x[0] for x in most_common_list]
            detail_df["count"] = [x[1] for x in most_common_list]
            detail_df["%total"] = [round(x[1] * 100 / row_count, ROUNDING) for x in most_common_list]
            detail_df["histogram"] = [BLACK_SQUARE * round(x[1] * 100 / row_count) for x in most_common_list]
            detail_dict[column_name] = detail_df
        else:
            logger.warning(f"Column '{column_name}' is empty.")

        # Produce a pattern analysis for strings
        if datatype == STRING and row_count:
            pattern_counter = get_pattern(non_null_values)
            max_length = min(max_detail_values, len(non_null_values))
            most_common_pattern_list = pattern_counter.most_common(max_length)
            pattern_df = pd.DataFrame()
            # Create 3-column descending visual
            pattern_df["rank"] = list(range(1, len(most_common_pattern_list) + 1))
            pattern_df["pattern"] = [x[0] for x in most_common_pattern_list]
            pattern_df["count"] = [x[1] for x in most_common_pattern_list]
            pattern_df["%total"] = [round(x[1] * 100 / row_count, ROUNDING) for x in most_common_pattern_list]
            pattern_df["histogram"] = [BLACK_SQUARE * round(x[1] * 100 / row_count) for x in most_common_pattern_list]
            pattern_dict[column_name] = pattern_df
        else:  # Numeric/datetime data
            values = pd.Series(values)
            plot_data = values.value_counts(normalize=True)
            if len(plot_data) >= PLOT_MIN_VALUES:
                logger.debug("Creating a histogram plot ...")
                sns.set_theme()
                sns.set(font_scale=PLOT_FONT_SCALE)
                g = sns.displot(values)
                plot_output_path = tempdir_path / f"{column_name}.histogram.png"
                g.set_axis_labels(column_name, COUNT, labelpad=10)
                g.figure.set_size_inches(PLOT_SIZE_X, PLOT_SIZE_Y)
                g.ax.margins(.15)
                g.savefig(plot_output_path)
                logger.info(f"Wrote {os.stat(plot_output_path).st_size} bytes to '{plot_output_path}'.")
                histogram_plot_list.append(column_name)
            if len(non_null_values) > PLOT_MIN_VALUES:
                plot_output_path = tempdir_path / f"{column_name}.box.png"
                fig, axs = plt.subplots(
                    nrows=2,
                    ncols=1,
                    figsize=(PLOT_SIZE_X, PLOT_SIZE_Y)
                )
                sns.boxplot(
                    ax=axs[0],
                    data=None,
                    x=non_null_values,
                    showfliers=True,
                    orient="h"
                )
                axs[0].set_xlabel(f"'{column_name}' with outliers")
                sns.boxplot(
                    ax=axs[1],
                    data=None,
                    x=non_null_values,
                    showfliers=False,
                    orient="h"
                )
                axs[1].set_xlabel(f"'{column_name}' without outliers")
                #plt.subplots_adjust(left=1, right=4, bottom=0.75, top=3, wspace=0.5, hspace=3)
                plt.savefig(plot_output_path)
                plt.close('all')  # Save memory
                logger.info(f"Wrote {os.stat(plot_output_path).st_size} bytes to '{plot_output_path}'.")
                box_plot_list.append(column_name)
            else:
                logger.debug("Not enough distinct values to create plots.")

    # Convert the summary_dict dictionary of dictionaries to a DataFrame
    result_df = pd.DataFrame.from_dict(summary_dict, orient='index')

    # Output
    if is_excel_output:
        logger.info("Writing summary ...")
        output_file = (target_dir / f"analysis{C.EXCEL_EXTENSION}")
        writer = pd.ExcelWriter(output_file, engine='xlsxwriter')
        result_df.to_excel(writer, sheet_name="Summary")
        # And generate a detail sheet, and optionally a pattern sheet and diagrams, for each column
        for column_name, detail_df in detail_dict.items():
            logger.debug(f"Examining column '{column_name}' ...")
            target_sheet_name = make_sheet_name(column_name, MAX_SHEET_NAME_LENGTH-4) + DETAIL_ABBR
            logger.info(f"Writing detail for column '{column_name}' to sheet '{target_sheet_name}' ...")
            detail_df.to_excel(writer, index=False, sheet_name=target_sheet_name)
            if column_name in pattern_dict:
                target_sheet_name = make_sheet_name(column_name, MAX_SHEET_NAME_LENGTH-4) + PATTERN_ABBR
                logger.info(f"Writing pattern information for string column '{column_name}' to sheet '{target_sheet_name}' ...")
                pattern_df = pattern_dict[column_name]
                pattern_df.to_excel(writer, index=False, sheet_name=target_sheet_name)
        writer.close()

        # Add the plots and size bars to the Excel file
        workbook = openpyxl.load_workbook(output_file)

        # Plots
        # Look for sheet names corresponding to the plot filename
        sheet_number = -1
        for sheet_name in workbook.sheetnames:
            sheet_number += 1
            if sheet_number == 0:  # Skip summary sheet (first sheet, zero-based-index)
                continue
            column_name = sheet_name[:-len(DETAIL_ABBR)]  # remove " det" from sheet name to get column name
            if column_name in histogram_plot_list:
                target_sheet_name = make_sheet_name(column_name, MAX_SHEET_NAME_LENGTH-4) + " dst"
                workbook.create_sheet(target_sheet_name, sheet_number+1)
                worksheet = workbook.worksheets[sheet_number+1]
                image_path = tempdir_path / (column_name + ".histogram.png")
                logger.info(f"Adding {image_path} to {output_file} as sheet {target_sheet_name} ...")
                image = openpyxl.drawing.image.Image(image_path)
                image.anchor = "A1"
                worksheet.add_image(image)
                sheet_number += 1
            if column_name in box_plot_list:
                target_sheet_name = make_sheet_name(column_name, MAX_SHEET_NAME_LENGTH-4) + " box"
                workbook.create_sheet(target_sheet_name, sheet_number+1)
                worksheet = workbook.worksheets[sheet_number+1]
                image_path = tempdir_path / (column_name + ".box.png")
                logger.info(f"Adding {image_path} to {output_file} as sheet {target_sheet_name} ...")
                image = openpyxl.drawing.image.Image(image_path)
                image.anchor = "A1"
                worksheet.add_image(image)
                sheet_number += 1

        # # Size a histogram column for each worksheet which contains the ranks of values or patterns
        # for i, sheet_name in enumerate(workbook.sheetnames):
        #     if sheet_name.endswith(DETAIL_ABBR) or sheet_name.endswith(PATTERN_ABBR):
        #         worksheet = workbook.worksheets[i]
        #         worksheet[f'E1'] = "Histogram"
        #         for row_number in range(2, worksheet.max_row+1):
        #             value_to_convert = worksheet[f'D{row_number}'].value  # C = 3rd column, convert from percentage
        #             bar_representation = "█" * round(value_to_convert)
        #             worksheet[f'E{row_number}'] = bar_representation
        #         # And set some visual formatting while we are here
        #         worksheet.column_dimensions['B'].width = 25
        #         # worksheet.column_dimensions['C'].number_format = "0.0"

        # Formatting for the summary sheet
        worksheet = workbook.worksheets[0]
        worksheet.column_dimensions['A'].width = 25  # Column names
        worksheet.column_dimensions['G'].width = 15  # Most common value
        worksheet.column_dimensions['I'].width = 15  # Largest value
        worksheet.column_dimensions['J'].width = 15  # Smallest value
        worksheet.column_dimensions['K'].width = 15  # Longest value

        for row in range(1, worksheet.max_row+1):
            worksheet.cell(row=row, column=1).alignment = Alignment(horizontal='right')
        for row in range(1, worksheet.max_row+1):
            for col in range(1, 17):
                worksheet.cell(row=row, column=col).border = Border(outline=Side(border_style=borders.BORDER_THICK, color='FFFFFFFF'))

        workbook.save(output_file)
        logger.info(f"Wrote {os.stat(output_file).st_size} bytes to '{output_file}'.")


    if is_html_output:
        root_output_dir = tempdir_path / "analysis"
        columns_dir = root_output_dir / "columns"
        images_dir = root_output_dir / "images"
        os.makedirs(columns_dir)
        os.makedirs(images_dir)
        # Move images
        for _, _, files in tempdir_path.walk():
            for file in files:
                os.rename(tempdir_path / file, images_dir / file)
            break
        # Generate a detail page, and optionally a pattern page and diagrams, for each column
        for column_name, detail_df in detail_dict.items():
            logger.debug(f"Examining column '{column_name}' ...")
            target_file = columns_dir / (column_name + DETAIL_ABBR + ".html")
            logger.info(f"Writing detail for column '{column_name}' to '{target_file}' ...")
            with open(target_file, "w") as writer:
                writer.write(make_html_header(f"Exploratory Data Analysis for column: {column_name}", root_output_file=root_output_dir))
                writer.write(f"<h1>Detail analysis for column '{column_name}'</h1>")
                writer.write(f"<h2>Value frequency</h2>")
                writer.write(detail_df.to_html(justify="center", na_rep="", index=False))
                if column_name in pattern_dict:
                    logger.info(f"Writing pattern information for string column '{column_name}' to '{target_file}' ...")
                    writer.write(f"<h2>Pattern frequency</h2>")
                    pattern_df = pattern_dict[column_name]
                    writer.write(pattern_df.to_html(justify="center", na_rep="", index=False))
                if column_name in histogram_plot_list:
                    logger.info(f"Adding histogram plot for column '{column_name}' to '{target_file}' ...")
                    writer.write(f"<h2>Histogram</h2>")
                    writer.write(f'<img src="../images/{column_name}.histogram.png" alt = "Histogram for column :{column_name}:">')
                if column_name in box_plot_list:
                    logger.info(f"Adding box plots for column '{column_name}' to '{target_file}' ...")
                    writer.write(f"<h2>Box plots</h2>")
                    writer.write(f'<img src="../images/{column_name}.box.png" alt = "Box plots for column :{column_name}:">')
                writer.write(make_html_footer())
        with open(root_output_dir / "analysis.html", "w") as writer:
            logger.info("Writing summary ...")
            writer.write(make_html_header(f"Exploratory Data Analysis for {input}"))
            # Replace column names in summary dataframe with URL links
            replacement_list = [f'<a href="columns/{x} det.html">{x}</a>' for x in result_df.index]
            replacement_dict = dict(zip(result_df.index, replacement_list))
            result_df = result_df.rename(index=replacement_dict)
            writer.write(result_df.to_html(justify="center", na_rep="", escape=False))
            writer.write(make_html_footer())
        output_file = shutil.make_archive(
            base_name=target_dir / "analysis",
            format="zip",
            root_dir=tempdir_path,
            base_dir=".",
        )
        logger.info(f"Wrote {os.stat(output_file).st_size} bytes to '{output_file}'.")
