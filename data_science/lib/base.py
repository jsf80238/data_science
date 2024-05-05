import sys
from dataclasses import dataclass
import enum
from inspect import stack, getargvalues, currentframe, FrameInfo
import logging
import os
from pathlib import Path
from random import choices
import re
from string import ascii_lowercase
import tempfile
import types
from typing import Union, Optional, Type, Tuple
import unicodedata
# Imports above are standard Python
# Imports below are 3rd-party
import pendulum
import jaydebeapi as jdbc
from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

min_major, min_minor = 3, 11
major, minor = sys.version_info[:2]
if major < min_major or minor < min_minor:
    raise Exception(f"Your Python version needs to be at least {min_major}.{min_minor}.")

old_factory = logging.getLogRecordFactory()

class Config:
    PRIMARY_CONFIG_FILE = "config.yaml"
    CONFIG_DIR = Path(__file__).parent.parent.parent / "config"

    @classmethod
    def get_config(cls, file_name: str = PRIMARY_CONFIG_FILE) -> dict:
        """
        Read a configuration file from the configuration file directory
        :param file_name: file within the configuration directory
        :return: the configuration corresponding to that file
        """
        if not file_name.lower().endswith(".yaml"):
            file_name += ".yaml"
        config_file = cls.CONFIG_DIR / file_name
        text = open(config_file).read()
        return load(text, Loader=Loader)


class C(enum.StrEnum):
    BLACK_SQUARE = unicodedata.lookup("BLACK SQUARE")  # ■, x25A0
    CHAR = "CHAR"
    CLASSPATH = "CLASSPATH"
    CLASS_NAME = "class_name"
    CONNECTION_STRING = "connection_string"
    CSV_EXTENSION = ".csv"
    DATABASE = "database"
    DATE = "DATE"
    DECIMAL = "DECIMAL"
    EXCEL_EXTENSION = ".xlsx"
    FLOAT = "FLOAT"
    JAR = "jar"
    JDBC = "jdbc"
    NUMBER = "NUMBER"
    PARQUET_EXTENSION = ".parquet"
    PORT_NUMBER = "port_number"
    SQL_EXTENSION = ".sql"
    VARCHAR = "VARCHAR"


config_dict = Config.get_config()
if sys.platform in ("linux", "darwin"):
    path_separator = ":"
elif sys.platform in ("win32",):
    path_separator = ";"
else:
    raise Exception(f"Unexpected platform '{sys.platform}'.")


class Logger:
    __instance = None

    def record_factory_factory(session_id: str):
        """Enables us to display a session_id identifier with each log message."""
        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            record.session_id = session_id
            return record

        return record_factory

    def __new__(cls,
                level: [str | int] = None,
                is_generate_session_id: bool = False,
                session_id: str = None,
                **kwargs
                ):
        """
        Return the same logger for every invocation.
        Optionally include a session_id string to help with correlation. By default it's a random 6-character string.
        """
        if not cls.__instance:
            if session_id:
                cls.session_id = session_id
            else:
                cls.session_id = ''.join(choices(ascii_lowercase, k=6))
            if level:
                cls.level = level.upper()
            else:
                cls.level = config_dict["logging"]["level"]

            cls.logger = logging.getLogger()
            # Add session_id identifier?
            if is_generate_session_id:
                logging.setLogRecordFactory(cls.record_factory_factory(cls.session_id))
            # Set overall logging level, will be overridden by the handlers
            cls.logger.setLevel(logging.DEBUG)
            # Formatting
            date_format = '%Y-%m-%dT%H:%M:%S%z'
            if is_generate_session_id:
                formatter = logging.Formatter('%(asctime)s | %(levelname)8s | session_id=%(session_id)s | %(message)s', datefmt=date_format)
            else:
                formatter = logging.Formatter('%(asctime)s | %(levelname)8s | %(message)s', datefmt=date_format)
            # Logging to STDERR
            console_handler = logging.StreamHandler()
            console_handler.setLevel(cls.level)
            console_handler.setFormatter(formatter)
            # Add console handler to logger
            cls.logger.addHandler(console_handler)
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @classmethod
    def get_logger(cls) -> logging.Logger:
        return cls.logger

    @classmethod
    def set_level(cls, level: str) -> None:
        for handler in cls.logger.handlers:
            handler.setLevel(level)


class Database:
    """
    Wrapper around the jaydebeapi module.
    """
    __instance = None

    def __new__(cls,
                host_name: str,
                port_number: int,
                database_name: str,
                user_name: str,
                password: str,
                auto_commit: bool = False,
                **kwargs
                ):
        """
        Return the same database object (connection) for every invocation.
        """
        cls.logger = Logger().get_logger()
        if not cls.__instance:
            if type(port_number) == str:
                if not port_number.isdigit():
                    raise Exception(f"Port number {port_number} is not valid.")
                port_number = int(port_number)
            cls.logger.info(f"Connecting to '{database_name} as {user_name}' ...")
            # Determine database type based on port number
            dbtype_dict = config_dict[C.JDBC][C.PORT_NUMBER]
            for database_type_name, assigned_port in dbtype_dict.items():
                if assigned_port == port_number:
                    break
            else:
                raise Exception(f"I don't know what kind of database listens on port {port_number}.")
            classpath = Path(config_dict[C.JDBC][C.JAR][database_type_name])
            absolute_classpath = Path(__file__).parent.parent / classpath
            os.environ[C.CLASSPATH] = os.environ.get(C.CLASSPATH, "") + path_separator + classpath.as_posix()
            connection_string = config_dict[C.JDBC][C.CONNECTION_STRING][database_type_name]
            connection_string = connection_string.replace("put_host_name_here", host_name)
            connection_string = connection_string.replace("put_port_number_here", str(port_number))
            connection_string = connection_string.replace("put_database_name_here", database_name)
            cls.database_connection = jdbc.connect(config_dict[C.JDBC][C.CLASS_NAME][database_type_name],
                                      connection_string,
                                      [user_name, password],
                                      absolute_classpath.as_posix())
            cls.logger.info("... connected.")
            cls.database_connection.jconn.setAutoCommit(auto_commit)
            cls.__instance = object.__new__(cls)
        return cls.__instance

    @classmethod
    def get_connection(cls) -> jdbc.Connection:
        return cls.database_connection

    @classmethod
    def execute(
            cls,
            sql: str,
            parameters: list = list(),
            cursor: jdbc.Cursor = None,
            is_debug: bool = False,
            ) -> Tuple[jdbc.Cursor, list]:
        """
        | Wrapper around the Cursor class
        | Returns a tuple containing:
        | 1: the cursor with the result set
        | 2: a list of the column names in the result set, or an empty list if not a SELECT statement

        :param sql: the query to be executed
        :param parameters: the parameters to fill the placeholders
        :param cursor: if provided will be used, else will create a new one
        :param is_debug: if True log the query but don't do anything
        :return: a tuple containing:
        """
        # Gather information about the caller so we can log a useful message
        # Search the stack for the first file which is not this one (that will be the caller we are interested in)
        for frame_info in stack():
            if frame_info.filename != __file__:
                identification = f"From directly above line {frame_info.lineno} in file {Path(frame_info.filename).name}"
                break
        else:
            identification = "<unknown>"
        # Format the SQL to fit on one line
        formatted_sql = re.sub(r"\s+", " ", sql).strip()
        # Make a cursor if one was not supplied by the caller
        if not cursor:
            cursor = cls.database_connection.cursor()
        # Log the statement with the parameters converted to their passed values
        sql_for_logging = sql
        pattern = re.compile(r"\s*=\s*\?")
        needed_parameter_count = pattern.findall(sql)
        if len(needed_parameter_count) != len(parameters):
            cls.logger.warning(
                f"I think the query contains {len(needed_parameter_count)} placeholders and I was given {len(parameters)} parameters: {parameters}")
        for param in parameters:
            if type(param) == str:
                param = "'" + param + "'"
            elif type(param) == int:
                param = str(param)
            else:
                cls.logger.warning("Cannot log SQL, sorry.")
                break
            sql_for_logging = re.sub(pattern, " = " + param, sql_for_logging, 1)
        # Format the SQL to fit on one line
        sql_for_logging = re.sub(r"\s+", " ", sql_for_logging).strip()
        if is_debug:
            cls.logger.info(f"{identification} would have executed: {sql_for_logging}.")
            return cursor, list()
        # We are not merely debugging, so try to execute and return results
        cls.logger.info(f"{identification} executing: {sql_for_logging} ...")
        try:
            cursor.execute(sql, parameters)
        except Exception as e:
            cls.logger.error(e)
            raise e
        # Successfully executed, now return a list of the column names
        try:
            column_list = [column[0] for column in cursor.description]
        except TypeError:  # For DML statements there will be no column description returned
            column_list = list()
            cls.logger.info(f"Rows affected: {cursor.rowcount:,d}.")
        return cursor, column_list

    @classmethod
    def fetch_one_row(
        cls,
        sql: str,
        parameters: list = list(),
        default_value=None
        ) -> Union[list, str, int]:
        """
        | Run the given query and fetch the first row.
        | If default_value not provided then ...
        | If there is only a single element in the select clause the function returns None.
        | If there are multiple elements in the select clause the function to return [None]*the number of elements.

        :param sql: the query to be executed
        :param parameters: the parameters to fill the placeholders
        :param default_value: if the query does not return any rows, return this.
        :return: if the return contains two or more things return them as a list, else return a single item.
        """
        cursor, column_list = cls.execute(sql, parameters)
        for row in cursor.fetchall():
            if len(row) == 1:
                return row[0]
            else:
                return row
            break
        cls.logger.info("No rows selected.")
        if default_value:
            return default_value
        else:
            if len(column_list) == 1:
                return None
            else:
                return [None] * len(column_list)


def dedent_sql(s):
    """
    Remove leading spaces from all lines of a SQL query.
    Useful for logging.

    :param s: query
    :return: cleaned-up version of query
    """
    return "\n".join([x.lstrip() for x in s.splitlines()])


def get_line_count(file_path: Union[str, Path]) -> int:
    """
    See https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
    """
    f = open(file_path, 'rb')
    line_count = 0
    buf_size = 1024 * 1024
    read_f = f.raw.read

    buf = read_f(buf_size)
    while buf:
        line_count += buf.count(b'\n')
        buf = read_f(buf_size)

    return line_count


if __name__ == "__main__":
    logger = Logger().get_logger()
    logger.info("a logging message")
    # mydb = Database(
    #     host_name="localhost",
    #     port_number=1433,
    #     database_name="master",
    #     user_name="sa",
    #     password="!1Jkrvhmhzyjwc"
    # )
    mydb = Database(
        host_name="localhost",
        port_number=5432,
        database_name="example",
        user_name="postgres",
        password="password"
    )
    query = """
        SELECT * from my_table
        """
    cursor, column_list = mydb.execute(query)
    for item in cursor.description:
        print(item)
    for r in cursor.fetchall():
        # row = dict(zip(column_list, r))
        for value in r:
            print()
            print(value)
            print(type(value))
    exit()
