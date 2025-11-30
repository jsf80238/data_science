import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime
from decimal import Decimal
import pickle
from pathlib import Path
import os
import random
import re
import shutil
from statistics import mean, quantiles, stdev
import sys
import tempfile
# Imports above are standard Python
# Imports below are 3rd-party
from argparse_range import range_action
import dateutil.parser
from dotenv import dotenv_values
from matplotlib import pyplot as plt
import polars as pl
import seaborn as sns

# Imports below are custom
from lib.base import C, Database, Logger, get_line_count

# Excel output
ROUNDING = 1  # 5.4% for example
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
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
# And don't make pie charts if there are more than this number of distinct values
DEFAULT_PLOT_VALUES_LIMIT = 8
# When determining whether a string column could be considered datetime or numeric examine (up to) this number of records
DEFAULT_OBJECT_SAMPLING_COUNT = 500
DEFAULT_OBJECT_CONVERSION_ALLOWED_ERROR_RATE = 5  # %
# Plotting visual effects
PLOT_SIZE_X, PLOT_SIZE_Y = 11, 8.5
PLOT_FONT_SCALE = 0.75
HISTOGRAM, BOX, PIE = "histogram", "box", "pie"
# Good character for spreadsheet-embedded histograms U+25A0
BLACK_SQUARE = "■"
OPEN, CLOSE = "{", "}"
DEFAULT_FILE_BASE_NAME = "profiled_data"

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
FLOAT = "float"

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


def format_long_string(s: str, cutoff: int) -> str:
    """
    | This string is really long and won't display nicely ... so adjust, for example:
    |   I'm unhappy with my collection of boring clothes. In striving to cut back on wasteful purchases, and to keep a tighter closet, I have sucked all of the fun out of my closet.
    | Will be replaced with:
    | I'm u...(actual length is 173 characters)...oset.

    :param s: the long string
    :param cutoff: how long is too long
    :return: formatted string
    """
    if not s:
        return ""
    if not isinstance(s, str):
        s = str(s)
    if len(s) <= cutoff:
        return s
    placeholder = f"...(actual length is {len(s)} characters)..."
    prefix = s[:5]
    suffix = s[-5:]
    return prefix + placeholder + suffix


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


def convert_seconds(seconds: float) -> str:
    """
    Convert seconds to days, hours, minutes & seconds

    :param seconds: e.g. 12938714
    :return: 149:18:05:14
    """
    seconds = int(seconds)
    min, sec = divmod(seconds, 60)
    hour, min = divmod(min, 60)
    day, hour = divmod(hour, 24)
    return f"{day}:{hour:02}:{min:02}:{sec:02}"


def get_datatype(v, dialect: str = "Snowflake") -> str:
    """
    | Return one of: STRING, NUMBER, DATETIME
    |
    | Examples:
    | "hi joe." --> STRING
    | 0 --> NUMBER
    | 1.1 --> NUMBER
    | 2025-11-29 -> DATETIME
    | 2025-11-29 22:56:54.060000+00:00 -> DATETIME

    :param v: the value we are trying to type
    :param dialect: the database type, in case it matters
    :return: the type
    """
    if isinstance(v, str):
        return STRING
    elif isinstance(v, int):
        return NUMBER
    elif isinstance(v, float):
        return NUMBER
    elif isinstance(v, Decimal):
        return NUMBER
    elif isinstance(v, date):
        return DATETIME
    elif isinstance(v, datetime):
        return DATETIME
    else:
        logger.warning(f"Categorizing this value as a string: {v}")
        return STRING

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
        home_link = f'<a href="../{root_output_file.name}.html">Home</a>'
    else:
        home_link = ""
    if input_query:
        title = input_query
    elif input_path:
        title = input_path.name
    else:
        raise Exception("Programming error")
    return f"""
    <!DOCTYPE html>
    <html lang="en-US">
        <head>
        <meta charset="utf-8">
        <title>Exploratory Data Analysis</title>
        <style>
            html {OPEN}
                font-family: monospace;
                font-size: smaller;
            {CLOSE}
            h1, h2, h3, p, img, div {OPEN}
                text-align: center;
            {CLOSE}
            table, tr, td {OPEN}
                border: 1px solid #000;
                margin-left: auto;
                margin-right: auto;
            {CLOSE}
        </style>
        </head>
        <body>
            <h1>Exploratory Data Analysis for {title}</h1>
            <p>{home_link}</p>
            <div>
    """


def make_html_footer() -> str:
    """
    | Helper function

    :return: </body> </html>
    """
    return f"""
            </div>
            </body>
        </html>
    """


def is_data_file(input_argument: str) -> bool:
    """
    Use the text of the input (select statement or file name) to determine if this is a file or a select statement
    :param input_argument: what the user provided
    :return: True if we think this is a data file, else False
    """
    for suffix in C.PARQUET_EXTENSION.value, C.CSV_EXTENSION.value, ".dat", ".txt", ".dsv":
        if input_argument.lower().endswith(suffix):
            return True
    return False


parser = argparse.ArgumentParser(
    description='Profile the data in a database or file. Generates an analysis consisting tables and images stored in an Excel workbook or HTML pages. For string columns provides a pattern analysis with C replacing letters, 9 replacing numbers, underscore replacing spaces, and question mark replacing everything else. For numeric and datetime columns produces a histogram and box plots.')

parser.add_argument('--input',
                    metavar="/path/to/input_data_file.extension | query-against-database",
                    help="An example query is 'select a, b, c from t where x>7'. File names must end in csv, dat, txt, dsv or parquet. See also --delimiter.",
                    default="select * from gold.finance_accounting.akademos_transaction_details sample (0.01)")
parser.add_argument('--header-lines',
                    type=int,
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    default=0,
                    help="When reading from a file specifies the number of rows to skip UNTIL the header row. Ignored when getting data from a database. Default is 0.")
parser.add_argument('--delimiter',
                    metavar="CHAR",
                    default=",",
                    help="Use this character to delimit columns, default is a comma. Ignored when getting data from a database or a parquet file.")
parser.add_argument('--sample-rows',
                    type=int,
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    help=f"When reading from a file randomly choose this number of rows. If greater than or equal to the number of data rows will use all rows. Ignored when getting data from a database.")
parser.add_argument('--max-detail-values',
                    type=int,
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    default=DEFAULT_MAX_DETAIL_VALUES,
                    help=f"Produce this many of the top value occurrences, default is {DEFAULT_MAX_DETAIL_VALUES}.")
parser.add_argument('--max-pattern-length',
                    type=int,
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    default=DEFAULT_MAX_PATTERN_LENGTH,
                    help=f"When segregating strings into patterns leave untouched strings of length greater than this, default is {DEFAULT_MAX_PATTERN_LENGTH}.")
parser.add_argument('--plot-values-limit',
                    type=int,
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    default=DEFAULT_PLOT_VALUES_LIMIT,
                    help=f"Don't make histograms or box plots when there are fewer than this number of distinct values, and don't make pie charts when there are more than this number of distinct values, default is {DEFAULT_PLOT_VALUES_LIMIT}.")
parser.add_argument('--no-pattern',
                    action='store_true',
                    help=f"Don't identify patterns in text columns.")
parser.add_argument('--no-histogram',
                    action='store_true',
                    help=f"Don't make histograms.")
parser.add_argument('--no-box',
                    action='store_true',
                    help=f"Don't make box plots.")
parser.add_argument('--no-pie',
                    action='store_true',
                    help=f"Don't make pie charts.")
parser.add_argument('--no-visual',
                    action='store_true',
                    help=f"Don't make histograms or box plots or pie charts.")
parser.add_argument('--max-longest-string',
                    type=int,
                    metavar="NUM",
                    action=range_action(50, sys.maxsize),
                    default=DEFAULT_LONGEST_LONGEST,
                    help=f"When displaying long strings show a summary if string exceeds this length, default is {DEFAULT_LONGEST_LONGEST}.")
parser.add_argument('--object-sampling-limit',
                    metavar="NUM",
                    action=range_action(1, sys.maxsize),
                    default=DEFAULT_OBJECT_SAMPLING_COUNT,
                    help=f"To determine whether a string column can be treated as datetime or numeric sample this number of values, default is {DEFAULT_OBJECT_SAMPLING_COUNT}.")
parser.add_argument('--object-conversion-allowed-error-rate',
                    metavar="NUM",
                    action=range_action(1, 100),
                    default=DEFAULT_OBJECT_CONVERSION_ALLOWED_ERROR_RATE,
                    help=f"To determine whether a string column can be treated as datetime or numeric allow up to this percentage of values to remain un-parseable, default is {DEFAULT_OBJECT_CONVERSION_ALLOWED_ERROR_RATE}.")
parser.add_argument('--target-dir',
                    metavar="/path/to/dir",
                    default=Path.cwd(),
                    help="Default is the current directory. Will make intermediate directories as necessary.")
parser.add_argument('--output-file-name',
                    metavar="NAME",
                    default=DEFAULT_FILE_BASE_NAME,
                    help=f"Default is, in order: input file name, table name from query if it can be determined, '{DEFAULT_FILE_BASE_NAME}'.")
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
if is_data_file(args.input):
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
delimiter = args.delimiter
sample_rows = args.sample_rows
max_detail_values = args.max_detail_values
max_pattern_length = args.max_pattern_length
max_longest_string = args.max_longest_string
plot_values_limit = args.plot_values_limit
object_sampling_limit = args.object_sampling_limit
object_conversion_allowed_error_rate = args.object_conversion_allowed_error_rate / 100
is_pattern = not args.no_pattern
is_histogram = not args.no_histogram and not args.no_visual
is_box = not args.no_box and not args.no_visual
is_pie = not args.no_pie and not args.no_visual
target_dir = Path(args.target_dir)
output_file_name = args.output_file_name

environment_settings_dict = {
    **os.environ,
    **dotenv_values(environment_file),
}
if not target_dir.parent.exists():
    parser.error("Directory '{output_dir.parent}' does not exist.")
else:
    os.makedirs(target_dir, exist_ok=True)

if input_path and not input_path.exists():
    parser.error(f"Could not find input file '{input_path}'.")

if args.verbose:
    logger = Logger("DEBUG").get_logger()
elif args.terse:
    logger = Logger("WARNING").get_logger()
else:
    logger = Logger().get_logger()


# Determine output file name if not provided on command line
if output_file_name == DEFAULT_FILE_BASE_NAME:
    if input_path:
        # Data is from a file
        output_file_name = input_path.stem
    else:
        # Data is from a query
        pattern = re.compile(r"select\s+.+from\s+([\w\.]+)", re.IGNORECASE | re.DOTALL)
        if match := pattern.search(input_query):
            output_file_name = match.group(1).lower()

# Now, read the data
input_df = None
data_dict = defaultdict(list)
# ↑ Keys are column_names, values are a list of values from the data.
datatype_dict = dict()
# ↑ Keys are column_names, values are the type of data (NUMBER, DATETIME, STRING)
if input_query:
    SAVED_DATA_FILE = Path.home() / "PycharmProjects" / "t7" / "profile.pickle"
    if SAVED_DATA_FILE.exists():
        logger.warning(f"Not querying database, instead re-using data stored in {SAVED_DATA_FILE.as_posix()}.")
        with open(SAVED_DATA_FILE, "rb") as f:
            input_df = pickle.load(f)
    else:
        # Data is coming from a database query
        mydb = Database(
            user_name=environment_settings_dict["SNOWFLAKE_USER"],
            account=environment_settings_dict["SNOWFLAKE_ACCOUNT"],
            key_file_path=environment_settings_dict["SNOWFLAKE_PRIVATE_KEY_PATH"],
            key_file_password=environment_settings_dict["SNOWFLAKE_PRIVATE_KEY_PASSWORD"],
        )
        input_df = pl.read_database(
            query="SELECT * FROM gold.finance_accounting.akademos_transaction_details sample(0.01)",
            connection=mydb.get_connection(),
            # schema_overrides={"normalised_score": pl.UInt8},
        )
        with open(SAVED_DATA_FILE, 'wb') as f:
            pickle.dump(input_df, f, pickle.HIGHEST_PROTOCOL)
    logger.info(f"Data read: {len(input_df):,} rows.")
    if len(input_df) == 0:
        exit()
else:
    # Data is coming from a file
    logger.info(f"Reading from '{input_path}' ...")
    if input_path.name.endswith(C.PARQUET_EXTENSION.value):
        input_df = pl.read_table(input_path).to_pandas()
    else:
        input_df = pl.read_csv(input_path, delimiter=delimiter, header=header_lines)
    logger.info(f"Data read: {len(input_df):,} rows.")
    if len(input_df) == 0:
        exit()
    # Sampling requested?
    if sample_rows:
        input_df = input_df.sample(sample_rows)
        logger.info(f"Data sampled: {len(input_df):,} rows.")
    """
    Pandas will automatically attempt to convert data to datetime where possible
    It does so poorly, in my experience, so will also try "manually"
    For example, https://www.kaggle.com/datasets/philipagbavordoe/car-prices
    """
    for column_name in input_df.select_dtypes(include=["object"]).columns:
        logger.info(f"Examining datatype for column '{column_name}' ...")
        mask = (input_df[column_name].isna() | input_df[column_name].isnull())
        s = input_df[~mask][column_name]
        if not s.size:
            # Column empty, we don't care about the type
            logger.info(f"Column {column_name} is empty.")
            continue
        # Sample up to DATATYPE_SAMPLING_SIZE non-null values
        a_sample = s.sample(min(object_sampling_limit, s.size))
        failure_count = 0
        for item in list(a_sample):
            try:
                convert_str_to_datetime(item)
            except Exception as e:
                failure_count += 1
        failure_ratio = failure_count / a_sample.size
        if failure_ratio <= object_conversion_allowed_error_rate:
            logger.info(f"Casting column '{column_name}' as a datetime ...")
            input_df[column_name] = pl.to_datetime(s.apply(safe_convert_str_to_datetime))
            datatype_dict[column_name] = DATETIME
        else:
            logger.debug(f"Error rate of {100 * failure_ratio:.1f}% when attempting to cast column '{column_name}' as a datetime.")
            failure_count = 0
            for item in list(a_sample):
                try:
                    float(item)
                except Exception as e:
                    failure_count += 1
            failure_ratio = failure_count / a_sample.size
            if failure_ratio <= object_conversion_allowed_error_rate:
                logger.info(f"Casting column '{column_name}' as numeric ...")
                input_df[column_name] = pl.to_numeric(s, errors="coerce")
                datatype_dict[column_name] = NUMBER
            else:
                logger.debug(f"Error rate of {100 * failure_ratio:.1f}% when attempting to cast column '{column_name}' as numeric.")
                logger.info(f"Will keep column '{column_name}' as a string.")
                datatype_dict[column_name] = STRING

# To temporarily hold plots and html files
tempdir = tempfile.TemporaryDirectory()
tempdir_path = Path(tempdir.name)
# To keep track of which columns have histogram plots
histogram_plot_list = list()
box_plot_list = list()
pie_plot_list = list()

summary_dict = dict()  # To be converted into the summary worksheet
detail_dict = dict()  # Each element to be converted into a detail worksheet
pattern_dict = dict()  # For each string column calculate the frequency of patterns
# for column_name, datatype in dict(input_df.dtypes).items():
for column_name in input_df.columns:
    values = input_df[column_name]
    if True and not column_name.upper().startswith("BOOKED_AT"):  # For testing
        continue
    # A list of non-null values are useful for some calculations below
    non_null_series = input_df[column_name].drop_nulls().drop_nans()
    try:
        a = non_null_series[0]
        datatype = get_datatype(a)
        logger.info(f"Working on column '{column_name}' ({datatype}) ...")
    except IndexError:
        logger.warning(f"Skipping column '{column_name}' because it is empty.")
        summary_dict[column_name] = dict()
        continue
    column_dict = dict.fromkeys(ANALYSIS_LIST)
    # Row count
    row_count = len(values)
    column_dict[ROW_COUNT] = row_count
    # Null
    null_count = row_count - len(non_null_series)
    column_dict[NULL_COUNT] = null_count
    # Null%
    column_dict[NULL_PERCENT] = round(100 * null_count / row_count, ROUNDING)
    # Unique
    unique_count = len(values.unique())
    column_dict[UNIQUE_COUNT] = unique_count
    # Unique%
    column_dict[UNIQUE_PERCENT] = round(100 * unique_count / row_count, ROUNDING)

    if null_count != row_count:
        temp = "temp"
        # Largest & smallest
        column_dict[LARGEST] = max(non_null_series.to_list())
        column_dict[SMALLEST] = min(non_null_series.to_list())
        if datatype == STRING:
            # Longest & shortest
            min_len = min(non_null_series.str.len_chars())
            candidates = non_null_series.filter(non_null_series.str.len_chars() == min_len)
            shortest_string = candidates.to_list().pop()
            column_dict[SHORTEST] = format_long_string(shortest_string, max_longest_string)
            max_len = max(non_null_series.str.len_chars())
            candidates = non_null_series.filter(non_null_series.str.len_chars() == max_len)
            shortest_string = candidates.to_list().pop()
            column_dict[LONGEST] = format_long_string(shortest_string, max_longest_string)
            # No mean/quartiles/stddev statistics for strings
        elif datatype == NUMBER:
            # No longest/shortest for numbers and dates
            column_dict[SHORTEST] = None
            column_dict[LONGEST] = None
            # Mean/quartiles/stddev statistics
            column_dict[MEAN] = non_null_series.mean()
            column_dict[STDDEV] = non_null_series.std()
            column_dict[PERCENTILE_25TH] = non_null_series.quantile(0.25)
            column_dict[MEDIAN] = non_null_series.quantile(0.5)
            column_dict[PERCENTILE_75TH] = non_null_series.quantile(0.75)
        elif datatype == DATETIME:
            # No longest/shortest for numbers and dates
            column_dict[SHORTEST] = None
            column_dict[LONGEST] = None
            # Mean/quartiles/stddev statistics
            column_dict[MEAN] = non_null_series.mean()
            column_dict[MEDIAN] = non_null_series.median()
            # quartiles/stddev cannot be calculated directly on datetimes
            temp_series = non_null_series.dt.epoch('ms')
            column_dict[STDDEV] = convert_seconds(temp_series.std() / 1000)  # Returns e.g. 123:12:34:56
            percentile_25th = temp_series.quantile(0.25) / 1000  # epoch
            column_dict[PERCENTILE_25TH] = datetime.fromtimestamp(percentile_25th)
            percentile_75th = temp_series.quantile(0.25) / 1000  # epoch
            column_dict[PERCENTILE_75TH] = datetime.fromtimestamp(percentile_75th)
            logger.info(column_dict)
        else:
            raise Exception("Programming error.")


        # Value counts
        # Collect no more than number of values available or what was given on the command-line
        # whichever is less
        counter = Counter(non_null_series.to_list())
        max_length = min(max_detail_values, len(values))
        most_common_list = counter.most_common(max_length)
        most_common, most_common_count = most_common_list[0]
        column_dict[MOST_COMMON] = most_common
        column_dict[MOST_COMMON_PERCENT] = round(100 * most_common_count / row_count, ROUNDING)
        detail_df = pl.DataFrame()
        i = 0
        # Create 3-column descending visual
        s = pl.Series("rank", list(range(1, len(most_common_list) + 1)))
        detail_df = detail_df.insert_column(i, s)
        i += 1
        s = pl.Series("value", [x[0] for x in most_common_list])
        detail_df = detail_df.insert_column(i, s)
        i += 1
        s = pl.Series("count", [x[1] for x in most_common_list])
        detail_df = detail_df.insert_column(i, s)
        i += 1
        s = pl.Series("%total", [round(x[1] * 100 / row_count, ROUNDING) for x in most_common_list])
        detail_df = detail_df.insert_column(i, s)
        i += 1
        s = pl.Series("histogram", [BLACK_SQUARE * round(x[1] * 100 / row_count) for x in most_common_list])
        detail_df = detail_df.insert_column(i, s)
        detail_dict[column_name] = detail_df

    # Produce visuals
    values = pl.Series(values)
    plot_data = values.value_counts(normalize=True)
    # Produce a pattern analysis for strings
    if is_pattern and datatype == STRING and row_count:
        pattern_counter = get_pattern(non_null_series[column_name].to_list())
        max_length = min(max_detail_values, len(non_null_series))
        most_common_pattern_list = pattern_counter.most_common(max_length)
        pattern_df = pl.DataFrame()
        # Create 3-column descending visual
        pattern_df["rank"] = list(range(1, len(most_common_pattern_list) + 1))
        pattern_df["pattern"] = [x[0] for x in most_common_pattern_list]
        pattern_df["count"] = [x[1] for x in most_common_pattern_list]
        pattern_df["%total"] = [round(x[1] * 100 / row_count, ROUNDING) for x in most_common_pattern_list]
        pattern_df["histogram"] = [BLACK_SQUARE * round(x[1] * 100 / row_count) for x in most_common_pattern_list]
        pattern_dict[column_name] = pattern_df
    else:  # Numeric/datetime data
        try:
            values_to_plot = values.astype(FLOAT)
        except TypeError as e:
            values_to_plot = values
        if is_histogram and len(plot_data) >= plot_values_limit:
            logger.info("Creating a histogram plot ...")
            plot_output_path = tempdir_path / f"{column_name}.histogram.png"
            plt.figure(figsize=(PLOT_SIZE_X/2, PLOT_SIZE_Y/2))
            ax = values_to_plot.plot.hist(bins=min(20, len(values_to_plot)))
            ax.set_xlabel(column_name)
            ax.set_ylabel("Count")
            plt.savefig(plot_output_path)
            plt.close('all')  # Save memory
            logger.info(f"Wrote {os.stat(plot_output_path).st_size:,} bytes to '{plot_output_path}'.")
            histogram_plot_list.append(column_name)
        if is_box and len(non_null_series) >= plot_values_limit:
            logger.info("Creating box plots ...")
            plot_output_path = tempdir_path / f"{column_name}.box.png"
            fig, axs = plt.subplots(
                nrows=2,
                ncols=1,
                figsize=(PLOT_SIZE_X, PLOT_SIZE_Y)
            )
            sns.boxplot(
                ax=axs[0],
                data=None,
                x=values_to_plot,
                showfliers=True,
                orient="h"
            )
            axs[0].set_xlabel(f"'{column_name}' with outliers")
            sns.boxplot(
                ax=axs[1],
                data=None,
                x=values_to_plot,
                showfliers=False,
                orient="h"
            )
            axs[1].set_xlabel(f"'{column_name}' without outliers")
            #plt.subplots_adjust(left=1, right=4, bottom=0.75, top=3, wspace=0.5, hspace=3)
            plt.savefig(plot_output_path)
            plt.close('all')  # Save memory
            logger.info(f"Wrote {os.stat(plot_output_path).st_size:,} bytes to '{plot_output_path}'.")
            box_plot_list.append(column_name)
    if is_pie and len(plot_data) < plot_values_limit:
        logger.info("Creating pie plot ...")
        plot_output_path = tempdir_path / f"{column_name}.pie.png"
        s = values.value_counts()
        fig, ax = plt.subplots()
        ax.pie(s, labels=s.index, autopct="%1.1f%%")
        ax.set_title(column_name)
        plt.savefig(plot_output_path)
        plt.close('all')  # Save memory
        logger.info(f"Wrote {os.stat(plot_output_path).st_size:,} bytes to '{plot_output_path}'.")
        pie_plot_list.append(column_name)

exit()
# Convert the summary_dict dictionary of dictionaries to a DataFrame
result_df = pl.DataFrame.from_dict(summary_dict, orient='index')

# Output
root_output_dir = tempdir_path / output_file_name
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
        writer.write(f"<h2>Detail analysis for column '{column_name}'</h2>")
        writer.write(f"<hr>")
        writer.write(f"<h3>Value frequency</h3>")
        writer.write(detail_df.to_html(justify="center", na_rep="", index=False))
        if column_name in pattern_dict:
            logger.info(f"Writing pattern information for string column '{column_name}' to '{target_file}' ...")
            writer.write(f"<hr>")
            writer.write(f"<h3>Pattern frequency</h3>")
            pattern_df = pattern_dict[column_name]
            writer.write(pattern_df.to_html(justify="center", na_rep="", index=False))
        if column_name in histogram_plot_list:
            logger.info(f"Adding histogram plot for column '{column_name}' to '{target_file}' ...")
            writer.write(f"<hr>")
            writer.write(f"<h3>Histogram</h3>")
            writer.write(f'<img src="../images/{column_name}.histogram.png" alt="Histogram for column :{column_name}:">')
        if column_name in pie_plot_list:
            logger.info(f"Adding pie plot for column '{column_name}' to '{target_file}' ...")
            writer.write(f"<hr>")
            writer.write(f"<h3>Box plots</h3>")
            writer.write(f'<img src="../images/{column_name}.pie.png" alt="Pie plot for column :{column_name}:">')
        if column_name in box_plot_list:
            logger.info(f"Adding box plots for column '{column_name}' to '{target_file}' ...")
            writer.write(f"<hr>")
            writer.write(f"<h3>Box plots</h3>")
            writer.write(f'<img src="../images/{column_name}.box.png" alt="Box plots for column :{column_name}:">')
        writer.write(make_html_footer())
with open(root_output_dir / f"{output_file_name}.html", "w") as writer:
    logger.info("Writing summary ...")
    writer.write(make_html_header(f"Exploratory Data Analysis for {input}"))
    # Replace column names in summary dataframe with URL links
    replacement_list = [f'<a href="columns/{x} det.html">{x}</a>' for x in result_df.index]
    replacement_dict = dict(zip(result_df.index, replacement_list))
    result_df = result_df.rename(index=replacement_dict)
    writer.write(result_df.to_html(justify="center", na_rep="", escape=False))
    writer.write(make_html_footer())
logger.info("Making zip archive ...")
output_file = shutil.make_archive(
    base_name=target_dir / output_file_name,
    format="zip",
    root_dir=tempdir_path,
    base_dir=".",
)
logger.info(f"Wrote {os.stat(output_file).st_size:,} bytes to '{output_file}'.")

if cleaned_version_output_file:
    target_path = target_dir / cleaned_version_output_file
    if cleaned_version_output_file.lower().endswith(C.CSV_EXTENSION):
        zipped_target_path = target_path.with_suffix(".zip")
        compression_opts = dict(method='zip', archive_name=cleaned_version_output_file)
        input_df.to_csv(zipped_target_path, index=False, compression=compression_opts)
    else:
        zipped_target_path = target_path.with_suffix(".gz")
        input_df.to_parquet(zipped_target_path, index=False, compression="gzip")
    logger.info(f"Wrote {os.stat(zipped_target_path).st_size:,} bytes to '{zipped_target_path}'.")
