import argparse
from typing import Optional, Tuple

import grpc
import importlib.util
import inspect
import json
import os
import unicodedata
from unidecode import unidecode
import platform
import requests as rq
import shutil
import subprocess
import sys
import time
import traceback
import re
import socket
import ast

from concurrent import futures
from datetime import datetime
from enum import IntEnum
from google.protobuf import timestamp_pb2
from zipfile import ZipFile, ZIP_DEFLATED
from http import HTTPStatus

from fivetran_connector_sdk.protos import common_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2_grpc

# Version format: <major_version>.<minor_version>.<patch_version>
# (where Major Version = 1 for GA, Minor Version is incremental MM from Jan 25 onwards, Patch Version is incremental within a month)
__version__ = "1.4.5"

WIN_OS = "windows"
ARM_64 = "arm64"
X64 = "x64"

OS_MAP = {
    "darwin": "mac",
    "linux": "linux",
    WIN_OS: WIN_OS
}

ARCH_MAP = {
    "x86_64": X64,
    "amd64": X64,
    ARM_64: ARM_64,
    "aarch64": ARM_64
}

TESTER_VERSION = "0.25.0521.001"
TESTER_FILENAME = "run_sdk_tester.jar"
VERSION_FILENAME = "version.txt"
UPLOAD_FILENAME = "code.zip"
LAST_VERSION_CHECK_FILE = "_last_version_check"
ROOT_LOCATION = ".ft_sdk_connector_tester"
CONFIG_FILE = "_config.json"
OUTPUT_FILES_DIR = "files"
REQUIREMENTS_TXT = "requirements.txt"
PYPI_PACKAGE_DETAILS_URL = "https://pypi.org/pypi/fivetran_connector_sdk/json"
ONE_DAY_IN_SEC = 24 * 60 * 60
MAX_RETRIES = 3
LOGGING_PREFIX = "Fivetran-Connector-SDK"
LOGGING_DELIMITER = ": "
VIRTUAL_ENV_CONFIG = "pyvenv.cfg"
ROOT_FILENAME = "connector.py"

# Compile patterns used in the implementation
WORD_DASH_DOT_PATTERN = re.compile(r'^[\w.-]*$')
NON_WORD_PATTERN = re.compile(r'\W')
WORD_OR_DOLLAR_PATTERN = re.compile(r'[\w$]')
DROP_LEADING_UNDERSCORE = re.compile(r'_+([a-zA-Z]\w*)')
WORD_PATTERN = re.compile(r'\w')

EXCLUDED_DIRS = ["__pycache__", "lib", "include", OUTPUT_FILES_DIR]
EXCLUDED_PIPREQS_DIRS = ["bin,etc,include,lib,Lib,lib64,Scripts,share"]
VALID_COMMANDS = ["debug", "deploy", "reset", "version"]
MAX_ALLOWED_EDIT_DISTANCE_FROM_VALID_COMMAND = 3
COMMANDS_AND_SYNONYMS = {
    "debug": {"test", "verify", "diagnose", "check"},
    "deploy": {"upload", "ship", "launch", "release"},
    "reset": {"reinitialize", "reinitialise", "re-initialize", "re-initialise", "restart", "restore"},
}

CONNECTION_SCHEMA_NAME_PATTERN = r'^[_a-z][_a-z0-9]*$'
DEBUGGING = False
EXECUTED_VIA_CLI = False
PRODUCTION_BASE_URL = "https://api.fivetran.com"
TABLES = {}
RENAMED_TABLE_NAMES = {}
RENAMED_COL_NAMES = {}
INSTALLATION_SCRIPT_MISSING_MESSAGE = "The 'installation.sh' file is missing in the 'drivers' directory. Please ensure that 'installation.sh' is present to properly configure drivers."
INSTALLATION_SCRIPT = "installation.sh"
DRIVERS = "drivers"
JAVA_LONG_MAX_VALUE = 9223372036854775807
MAX_CONFIG_FIELDS = 100
SUPPORTED_PYTHON_VERSIONS = ["3.12", "3.11", "3.10", "3.9"]
DEFAULT_PYTHON_VERSION = "3.12"
FIVETRAN_HD_AGENT_ID = "FIVETRAN_HD_AGENT_ID"
UTF_8 = "utf-8"


class Logging:
    class Level(IntEnum):
        FINE = 1
        INFO = 2
        WARNING = 3
        SEVERE = 4

    LOG_LEVEL = None

    @staticmethod
    def __log(level: Level, message: str):
        """Logs a message with the specified logging level.

        Args:
            level (Logging.Level): The logging level.
            message (str): The message to log.
        """
        if DEBUGGING:
            current_time = datetime.now().strftime("%b %d, %Y %I:%M:%S %p")
            escaped_message = json.dumps(message).strip('"')
            print(f"{Logging._get_color(level)}{current_time} {level.name}: {escaped_message} {Logging._reset_color()}")
        else:
            escaped_message = json.dumps(message)
            log_message = f'{{"level":"{level.name}", "message": {escaped_message}, "message_origin": "connector_sdk"}}'
            print(log_message)

    @staticmethod
    def _get_color(level):
        if level == Logging.Level.WARNING:
            return "\033[93m"  # Yellow
        elif level == Logging.Level.SEVERE:
            return "\033[91m"  # Red
        return ""

    @staticmethod
    def _reset_color():
        return "\033[0m"

    @staticmethod
    def fine(message: str):
        """Logs a fine-level message.

        Args:
            message (str): The message to log.
        """
        if DEBUGGING and Logging.LOG_LEVEL == Logging.Level.FINE:
            Logging.__log(Logging.Level.FINE, message)

    @staticmethod
    def info(message: str):
        """Logs an info-level message.

        Args:
            message (str): The message to log.
        """
        if Logging.LOG_LEVEL <= Logging.Level.INFO:
            Logging.__log(Logging.Level.INFO, message)

    @staticmethod
    def warning(message: str):
        """Logs a warning-level message.

        Args:
            message (str): The message to log.
        """
        if Logging.LOG_LEVEL <= Logging.Level.WARNING:
            Logging.__log(Logging.Level.WARNING, message)

    @staticmethod
    def severe(message: str, exception: Exception = None):
        """Logs a severe-level message.

        Args:
            message (str): The message to log.
            exception (Exception, optional): Exception to be logged if provided.
        """
        if Logging.LOG_LEVEL <= Logging.Level.SEVERE:
            Logging.__log(Logging.Level.SEVERE, message)

            if exception:
                exc_type, exc_value, exc_traceback = type(exception), exception, exception.__traceback__
                tb_str = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback, limit=1))

                for error in tb_str.split("\n"):
                    Logging.__log(Logging.Level.SEVERE, error)


class Operations:
    @staticmethod
    def upsert(table: str, data: dict) -> list[connector_sdk_pb2.UpdateResponse]:
        """Updates records with the same primary key if already present in the destination. Inserts new records if not already present in the destination.

        Args:
            table (str): The name of the table.
            data (dict): The data to upsert.

        Returns:
            list[connector_sdk_pb2.UpdateResponse]: A list of update responses.
        """
        if DEBUGGING:
            _yield_check(inspect.stack())

        responses = []

        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        if not columns:
            global TABLES
            for field in data.keys():
                field_name = get_renamed_column_name(field)
                columns[field_name] = common_pb2.Column(
                    name=field_name, type=common_pb2.DataType.UNSPECIFIED, primary_key=False)
            new_table = common_pb2.Table(name=table, columns=columns.values())
            TABLES[table] = new_table

        mapped_data = _map_data_to_columns(data, columns)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.OpType.UPSERT,
            data=mapped_data
        )

        responses.append(
            connector_sdk_pb2.UpdateResponse(
                operation=connector_sdk_pb2.Operation(record=record)))

        return responses

    @staticmethod
    def update(table: str, modified: dict) -> connector_sdk_pb2.UpdateResponse:
        """Performs an update operation on the specified table with the given modified data.

        Args:
            table (str): The name of the table.
            modified (dict): The modified data.

        Returns:
            connector_sdk_pb2.UpdateResponse: The update response.
        """
        if DEBUGGING:
            _yield_check(inspect.stack())

        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        mapped_data = _map_data_to_columns(modified, columns)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.OpType.UPDATE,
            data=mapped_data
        )

        return connector_sdk_pb2.UpdateResponse(
            operation=connector_sdk_pb2.Operation(record=record))

    @staticmethod
    def delete(table: str, keys: dict) -> connector_sdk_pb2.UpdateResponse:
        """Performs a soft delete operation on the specified table with the given keys.

        Args:
            table (str): The name of the table.
            keys (dict): The keys to delete.

        Returns:
            connector_sdk_pb2.UpdateResponse: The delete response.
        """
        if DEBUGGING:
            _yield_check(inspect.stack())

        table = get_renamed_table_name(table)
        columns = _get_columns(table)
        mapped_data = _map_data_to_columns(keys, columns)
        record = connector_sdk_pb2.Record(
            schema_name=None,
            table_name=table,
            type=common_pb2.OpType.DELETE,
            data=mapped_data
        )

        return connector_sdk_pb2.UpdateResponse(
            operation=connector_sdk_pb2.Operation(record=record))

    @staticmethod
    def checkpoint(state: dict) -> connector_sdk_pb2.UpdateResponse:
        """Checkpoint saves the connector's state. State is a dict which stores information to continue the
        sync from where it left off in the previous sync. For example, you may choose to have a field called
        "cursor" with a timestamp value to indicate up to when the data has been synced. This makes it possible
        for the next sync to fetch data incrementally from that time forward. See below for a few example fields
        which act as parameters for use by the connector code.\n
        {
            "initialSync": true,\n
            "cursor": "1970-01-01T00:00:00.00Z",\n
            "last_resync": "1970-01-01T00:00:00.00Z",\n
            "thread_count": 5,\n
            "api_quota_left": 5000000
        }

        Args:
            state (dict): The state to checkpoint/save.

        Returns:
            connector_sdk_pb2.UpdateResponse: The checkpoint response.
        """
        if DEBUGGING:
            _yield_check(inspect.stack())

        return connector_sdk_pb2.UpdateResponse(
            operation=connector_sdk_pb2.Operation(checkpoint=connector_sdk_pb2.Checkpoint(
                state_json=json.dumps(state))))


def check_newer_version():
    """Periodically checks for a newer version of the SDK and notifies the user if one is available."""
    tester_root_dir = _tester_root_dir()
    last_check_file_path = os.path.join(tester_root_dir, LAST_VERSION_CHECK_FILE)
    if not os.path.isdir(tester_root_dir):
        os.makedirs(tester_root_dir, exist_ok=True)

    if os.path.isfile(last_check_file_path):
        # Is it time to check again?
        with open(last_check_file_path, 'r', encoding=UTF_8) as f_in:
            timestamp = int(f_in.read())
            if (int(time.time()) - timestamp) < ONE_DAY_IN_SEC:
                return

    for index in range(MAX_RETRIES):
        try:
            # check version and save current time
            response = rq.get(PYPI_PACKAGE_DETAILS_URL)
            response.raise_for_status()
            data = json.loads(response.text)
            latest_version = data["info"]["version"]
            if __version__ < latest_version:
                print_library_log(f"[notice] A new release of 'fivetran-connector-sdk' is available: {latest_version}")
                print_library_log("[notice] To update, run: pip install --upgrade fivetran-connector-sdk")

            with open(last_check_file_path, 'w', encoding=UTF_8) as f_out:
                f_out.write(f"{int(time.time())}")
            break
        except Exception:
            retry_after = 2 ** index
            print_library_log(f"Unable to check if a newer version of `fivetran-connector-sdk` is available. Retrying again after {retry_after} seconds", Logging.Level.WARNING)
            time.sleep(retry_after)


def _tester_root_dir() -> str:
    """Returns the root directory for the tester."""
    return os.path.join(os.path.expanduser("~"), ROOT_LOCATION)


def _get_columns(table: str) -> dict:
    """Retrieves the columns for the specified table.

    Args:
        table (str): The name of the table.

    Returns:
        dict: The columns for the table.
    """
    columns = {}
    if table in TABLES:
        for column in TABLES[table].columns:
            columns[column.name] = column

    return columns


def _map_data_to_columns(data: dict, columns: dict) -> dict:
    """Maps data to the specified columns.

    Args:
        data (dict): The data to map.
        columns (dict): The columns to map the data to.

    Returns:
        dict: The mapped data.
    """
    mapped_data = {}
    for k, v in data.items():
        key = get_renamed_column_name(k)
        if v is None:
            mapped_data[key] = common_pb2.ValueType(null=True)
        elif (key in columns) and columns[key].type != common_pb2.DataType.UNSPECIFIED:
            map_defined_data_type(columns, key, mapped_data, v)
        else:
            map_inferred_data_type(key, mapped_data, v)
    return mapped_data


def map_inferred_data_type(k, mapped_data, v):
    # We can infer type from the value
    if isinstance(v, int):
        if abs(v) > JAVA_LONG_MAX_VALUE:
            mapped_data[k] = common_pb2.ValueType(float=v)
        else:
            mapped_data[k] = common_pb2.ValueType(long=v)
    elif isinstance(v, float):
        mapped_data[k] = common_pb2.ValueType(float=v)
    elif isinstance(v, bool):
        mapped_data[k] = common_pb2.ValueType(bool=v)
    elif isinstance(v, bytes):
        mapped_data[k] = common_pb2.ValueType(binary=v)
    elif isinstance(v, list):
        raise ValueError(
            "Values for the columns cannot be of type 'list'. Please ensure that all values are of a supported type. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#supporteddatatypes")
    elif isinstance(v, dict):
        mapped_data[k] = common_pb2.ValueType(json=json.dumps(v))
    elif isinstance(v, str):
        mapped_data[k] = common_pb2.ValueType(string=v)
    else:
        # Convert arbitrary objects to string
        mapped_data[k] = common_pb2.ValueType(string=str(v))


def map_defined_data_type(columns, k, mapped_data, v):
    if columns[k].type == common_pb2.DataType.BOOLEAN:
        mapped_data[k] = common_pb2.ValueType(bool=v)
    elif columns[k].type == common_pb2.DataType.SHORT:
        mapped_data[k] = common_pb2.ValueType(short=v)
    elif columns[k].type == common_pb2.DataType.INT:
        mapped_data[k] = common_pb2.ValueType(int=v)
    elif columns[k].type == common_pb2.DataType.LONG:
        mapped_data[k] = common_pb2.ValueType(long=v)
    elif columns[k].type == common_pb2.DataType.DECIMAL:
        mapped_data[k] = common_pb2.ValueType(decimal=v)
    elif columns[k].type == common_pb2.DataType.FLOAT:
        mapped_data[k] = common_pb2.ValueType(float=v)
    elif columns[k].type == common_pb2.DataType.DOUBLE:
        mapped_data[k] = common_pb2.ValueType(double=v)
    elif columns[k].type == common_pb2.DataType.NAIVE_DATE:
        timestamp = timestamp_pb2.Timestamp()
        dt = datetime.strptime(v, "%Y-%m-%d")
        timestamp.FromDatetime(dt)
        mapped_data[k] = common_pb2.ValueType(naive_date=timestamp)
    elif columns[k].type == common_pb2.DataType.NAIVE_DATETIME:
        if '.' not in v: v = v + ".0"
        timestamp = timestamp_pb2.Timestamp()
        dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f")
        timestamp.FromDatetime(dt)
        mapped_data[k] = common_pb2.ValueType(naive_datetime=timestamp)
    elif columns[k].type == common_pb2.DataType.UTC_DATETIME:
        timestamp = timestamp_pb2.Timestamp()
        dt = v if isinstance(v, datetime) else _parse_datetime_str(v)
        timestamp.FromDatetime(dt)
        mapped_data[k] = common_pb2.ValueType(utc_datetime=timestamp)
    elif columns[k].type == common_pb2.DataType.BINARY:
        mapped_data[k] = common_pb2.ValueType(binary=v)
    elif columns[k].type == common_pb2.DataType.XML:
        mapped_data[k] = common_pb2.ValueType(xml=v)
    elif columns[k].type == common_pb2.DataType.STRING:
        incoming = v if isinstance(v, str) else str(v)
        mapped_data[k] = common_pb2.ValueType(string=incoming)
    elif columns[k].type == common_pb2.DataType.JSON:
        mapped_data[k] = common_pb2.ValueType(json=json.dumps(v))
    else:
        raise ValueError(f"Unsupported data type encountered: {columns[k].type}. Please use valid data types.")

def _warn_exit_usage(filename, line_no, func):
    print_library_log(f"Avoid using {func} to exit from the Python code as this can cause the connector to become stuck. Throw a error if required " +
                      f"at: {filename}:{line_no}. See the Technical Reference for details: https://fivetran.com/docs/connector-sdk/technical-reference#handlingexceptions",
                      Logging.Level.WARNING)

def _exit_check(project_path):
    """Checks for the presence of 'exit()' in the calling code.
    Args:
        project_path: The absolute project_path to check exit in the connector.py file in the project.
    """
    # We expect the connector.py to catch errors or throw exceptions
    # This is a warning shown to let the customer know that we expect either the yield call or error thrown
    # exit() or sys.exit() in between some yields can cause the connector to be stuck without processing further upsert calls

    filepath = os.path.join(project_path, ROOT_FILENAME)
    with open(filepath, "r", encoding=UTF_8) as f:
        try:
            tree = ast.parse(f.read())
            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name) and node.func.id == "exit":
                        _warn_exit_usage(ROOT_FILENAME, node.lineno, "exit()")
                    elif isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
                        if node.func.attr == "_exit" and node.func.value.id == "os":
                            _warn_exit_usage(ROOT_FILENAME, node.lineno, "os._exit()")
                        if node.func.attr == "exit" and node.func.value.id == "sys":
                            _warn_exit_usage(ROOT_FILENAME, node.lineno, "sys.exit()")
        except SyntaxError as e:
            print_library_log(f"SyntaxError in {ROOT_FILENAME}: {e}", Logging.Level.SEVERE)


def _parse_datetime_str(dt):
    return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f%z" if '.' in dt else "%Y-%m-%dT%H:%M:%S%z")


def _yield_check(stack):
    """Checks for the presence of 'yield' in the calling code.
    Args:
        stack: The stack frame to check.
    """

    # Known issue with inspect.getmodule() and yield behavior in a frozen application.
    # When using inspect.getmodule() on stack frames obtained by inspect.stack(), it fails
    # to resolve the modules in a frozen application due to incompatible assumptions about
    # the file paths. This can lead to unexpected behavior, such as yield returning None or
    # the failure to retrieve the module inside a frozen app
    # (Reference: https://github.com/pyinstaller/pyinstaller/issues/5963)

    called_method = stack[0].function
    calling_code = stack[1].code_context[0]
    if f"{called_method}(" in calling_code:
        if 'yield' not in calling_code:
            print_library_log(
                f"Please add 'yield' to '{called_method}' operation on line {stack[1].lineno} in file '{stack[1].filename}'", Logging.Level.SEVERE)
            os._exit(1)
    else:
        # This should never happen
        raise RuntimeError(
            f"The '{called_method}' function is missing in the connector calling code '{calling_code}'. Please ensure that the '{called_method}' function is properly defined in your code to proceed. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsmethods")


def _check_dict(incoming: dict, string_only: bool = False) -> dict:
    """Validates the incoming dictionary.
    Args:
        incoming (dict): The dictionary to validate.
        string_only (bool): Whether to allow only string values.

    Returns:
        dict: The validated dictionary.
    """

    if not incoming:
        return {}

    if not isinstance(incoming, dict):
        raise ValueError(
            "Configuration must be provided as a JSON dictionary. Please check your input. Reference: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile")

    if string_only:
        for k, v in incoming.items():
            if not isinstance(v, str):
                print_library_log(
                    "All values in the configuration must be STRING. Please check your configuration and ensure that every value is a STRING.", Logging.Level.SEVERE)
                os._exit(1)

    return incoming


def is_connection_name_valid(connection: str):
    """Validates if the incoming connection schema name is valid or not.
    Args:
        connection (str): The connection schema name being validated.

    Returns:
        bool: True if connection name is valid.
    """

    pattern = re.compile(CONNECTION_SCHEMA_NAME_PATTERN)
    return pattern.match(connection)


def log_unused_deps_error(package_name: str, version: str):
    print_library_log(f"Please remove `{package_name}` from requirements.txt."
          f" The latest version of `{package_name}` is always available when executing your code."
          f" Current version: {version}", Logging.Level.SEVERE)


def validate_deploy_parameters(connection, deploy_key):
    if not deploy_key or not connection:
        print_library_log("The deploy command needs the following parameters:"
              "\n\tRequired:\n"
              "\t\t--api-key <BASE64-ENCODED-FIVETRAN-API-KEY-FOR-DEPLOYMENT>\n"
              "\t\t--connection <VALID-CONNECTOR-SCHEMA_NAME>\n"
              "\t(Optional):\n"
              "\t\t--destination <DESTINATION_NAME> (Becomes required if there are multiple destinations)\n"
              "\t\t--configuration <CONFIGURATION_FILE> (Completely replaces the existing configuration)", Logging.Level.SEVERE)
        os._exit(1)
    elif not is_connection_name_valid(connection):
        print_library_log(f"Connection name: {connection} is invalid!\n The connection name should start with an "
              f"underscore or a lowercase letter (a-z), followed by any combination of underscores, lowercase "
              f"letters, or digits (0-9). Uppercase characters are not allowed.", Logging.Level.SEVERE)
        os._exit(1)

def print_library_log(message: str, level: Logging.Level = Logging.Level.INFO):
    """Logs a library message with the specified logging level.

    Args:
        level (Logging.Level): The logging level.
        message (str): The message to log.
    """
    if DEBUGGING or EXECUTED_VIA_CLI:
        current_time = datetime.now().strftime("%b %d, %Y %I:%M:%S %p")
        escaped_message = json.dumps(message).strip('"')
        print(f"{Logging._get_color(level)}{current_time} {level.name} {LOGGING_PREFIX}: {escaped_message} {Logging._reset_color()}")
    else:
        escaped_message = json.dumps(LOGGING_PREFIX + LOGGING_DELIMITER + message)
        log_message = f'{{"level":"{level.name}", "message": {escaped_message}, "message_origin": "library"}}'
        print(log_message)


def is_port_in_use(port: int):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


def get_available_port():
    for port in range(50051, 50061):
        if not is_port_in_use(port):
            return port
    return None


def update_base_url_if_required():
    config_file_path = os.path.join(_tester_root_dir(), CONFIG_FILE)
    if os.path.isfile(config_file_path):
        with open(config_file_path, 'r', encoding=UTF_8) as f:
            data = json.load(f)
            base_url = data.get('production_base_url')
            if base_url is not None:
                global PRODUCTION_BASE_URL
                PRODUCTION_BASE_URL = base_url
                print_library_log(f"Updating PRODUCTION_BASE_URL to: {base_url}")


def is_special(c):
    """Check if the character is a special character."""
    return not WORD_OR_DOLLAR_PATTERN.fullmatch(c)


def starts_word(previous, current):
    """
    Check if the current character starts a new word based on the previous character.
    """
    return (previous and previous.islower() and current.isupper()) or (
            previous and previous.isdigit() != current.isdigit()
    )


def underscore_invalid_leading_character(name, valid_leading_regex):
    """
    Ensure the name starts with a valid leading character.
    """
    if name and not valid_leading_regex.match(name[0]):
        name = f'_{name}'
    return name


def single_underscore_case(name):
    """
    Convert the input name to single underscore case, replacing special characters and spaces.
    """
    acc = []
    previous = None

    for char_index, c in enumerate(name):
        if char_index == 0 and c == '$':
            acc.append('_')
        elif is_special(c):
            acc.append('_')
        elif c == ' ':
            acc.append('_')
        elif starts_word(previous, c):
            acc.append('_')
            acc.append(c.lower())
        else:
            acc.append(c.lower())

        previous = c

    name = ''.join(acc)
    return re.sub(r'_+', '_', name)


def contains_only_word_dash_dot(name):
    """
    Check if the name contains only word characters, dashes, and dots.
    """
    return bool(WORD_DASH_DOT_PATTERN.fullmatch(name))


def transliterate(name):
    """
    Transliterate the input name if it contains non-word, dash, or dot characters.
    """
    if contains_only_word_dash_dot(name):
        return name
    # Step 1: Normalize the name to NFD form (decomposed form)
    normalized_name = unicodedata.normalize('NFD', name)
    # Step 2: Remove combining characters (diacritics, accents, etc.)
    normalized_name = ''.join(char for char in normalized_name if not unicodedata.combining(char))
    # Step 3: Normalize back to NFC form (composed form)
    normalized_name = unicodedata.normalize('NFC', normalized_name)
    # Step 4: Convert the string to ASCII using `unidecode` (removes any remaining non-ASCII characters)
    normalized_name = unidecode(normalized_name)
    # Step 5: Return the normalized name
    return normalized_name


def redshift_safe(name):
    """
    Make the name safe for use in Redshift.
    """
    name = transliterate(name)
    name = NON_WORD_PATTERN.sub('_', name)
    name = single_underscore_case(name)
    name = underscore_invalid_leading_character(name, WORD_PATTERN)
    return name


def safe_drop_underscores(name):
    """
    Drop leading underscores if the name starts with valid characters after sanitization.
    """
    safe_name = redshift_safe(name)
    match = DROP_LEADING_UNDERSCORE.match(safe_name)
    if match:
        return match.group(1)
    return safe_name


def get_renamed_table_name(source_table):
    """
    Process a source table name to ensure it conforms to naming rules.
    """
    if source_table not in RENAMED_TABLE_NAMES:
        RENAMED_TABLE_NAMES[source_table] = safe_drop_underscores(source_table)

    return RENAMED_TABLE_NAMES[source_table]


def get_renamed_column_name(source_column):
    """
    Process a source column name to ensure it conforms to naming rules.
    """
    if source_column not in RENAMED_COL_NAMES:
        RENAMED_COL_NAMES[source_column] = redshift_safe(source_column)

    return RENAMED_COL_NAMES[source_column]

class Connector(connector_sdk_pb2_grpc.ConnectorServicer):
    def __init__(self, update, schema=None):
        """Initializes the Connector instance.
        Args:
            update: The update method.
            schema: The schema method.
        """

        self.schema_method = schema
        self.update_method = update

        self.configuration = None
        self.state = None

        update_base_url_if_required()

    @staticmethod
    def fetch_requirements_from_file(file_path: str) -> list[str]:
        """Reads a requirements file and returns a list of dependencies.

        Args:
            file_path (str): The path to the requirements file.

        Returns:
            list[str]: A list of dependencies as strings.
        """
        with open(file_path, 'r', encoding=UTF_8) as f:
            return f.read().splitlines()

    @staticmethod
    def fetch_requirements_as_dict(self, file_path: str) -> dict:
        """Converts a list of dependencies from the requirements file into a dictionary.

        Args:
            file_path (str): The path to the requirements file.

        Returns:
            dict: A dictionary where keys are package names (lowercased) and
            values are the full dependency strings.
        """
        requirements_dict = {}
        if not os.path.exists(file_path):
            return requirements_dict
        for requirement in self.fetch_requirements_from_file(file_path):
            requirement = requirement.strip()
            if not requirement or requirement.startswith("#"):  # Skip empty lines and comments
                continue
            try:
                key = re.split(r"==|>=|<=|>|<", requirement)[0]
                requirements_dict[key.lower().replace('-', '_')] = requirement.lower()
            except ValueError:
                print_library_log(f"Invalid requirement format: '{requirement}'", Logging.Level.SEVERE)
        return requirements_dict

    def validate_requirements_file(self, project_path: str, is_deploy: bool, force: bool = False):
        """Validates the `requirements.txt` file against the project's actual dependencies.

        This method generates a temporary requirements file using `pipreqs`, compares
        it with the existing `requirements.txt`, and checks for version mismatches,
        missing dependencies, and unused dependencies. It will issue warnings, errors,
        or even terminate the process depending on whether it's being run for deployment.

        Args:
            project_path (str): The path to the project directory containing the `requirements.txt`.
            is_deploy (bool): If `True`, the method will exit the process on critical errors.
            force (bool): Force update an existing connection.

        """
        # Detect and exclude virtual environment directories
        venv_dirs = [name for name in os.listdir(project_path)
                     if os.path.isdir(os.path.join(project_path, name)) and
                     VIRTUAL_ENV_CONFIG in os.listdir(os.path.join(project_path, name))]

        ignored_dirs = EXCLUDED_PIPREQS_DIRS + venv_dirs if venv_dirs else EXCLUDED_PIPREQS_DIRS

        # tmp_requirements is only generated when pipreqs command is successful
        requirements_file_path = os.path.join(project_path, REQUIREMENTS_TXT)
        tmp_requirements_file_path = os.path.join(project_path, 'tmp_requirements.txt')
        # copying packages of requirements file to tmp file to handle pipreqs fail use-case
        self.copy_requirements_file_to_tmp_requirements_file(os.path.join(project_path, REQUIREMENTS_TXT), tmp_requirements_file_path)
        # Run the pipreqs command and capture stderr
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            result = subprocess.run(
                ["pipreqs", project_path, "--savepath", tmp_requirements_file_path, "--ignore", ",".join(ignored_dirs)],
                stderr=subprocess.PIPE,
                text=True  # Ensures output is in string format
            )

            if result.returncode == 0:
                break

            print_library_log(f"Attempt {attempt}: pipreqs check failed.", Logging.Level.WARNING)

            if attempt < MAX_RETRIES:
                retry_after = 3 ** attempt
                print_library_log(f"Retrying in {retry_after} seconds...", Logging.Level.SEVERE)
                time.sleep(retry_after)
            else:
                print_library_log(f"pipreqs failed after {MAX_RETRIES} attempts with:", Logging.Level.SEVERE)
                print_library_log(result.stderr, Logging.Level.SEVERE)
                print_library_log(f"Skipping validation of requirements.txt due to error connecting to PyPI (Python Package Index) APIs. Continuing with {'deploy' if is_deploy else 'debug'}...", Logging.Level.SEVERE)

        tmp_requirements = self.fetch_requirements_as_dict(self, tmp_requirements_file_path)
        self.remove_unwanted_packages(tmp_requirements)
        os.remove(tmp_requirements_file_path)

        # remove corrupt requirements listed by pipreqs
        corrupt_requirements = [key for key in tmp_requirements if key.startswith("~")]
        for requirement in corrupt_requirements:
            del tmp_requirements[requirement]

        update_version_requirements = False
        update_missing_requirements = False
        update_unused_requirements = False
        if len(tmp_requirements) > 0:
            requirements = self.load_or_add_requirements_file(requirements_file_path)

            version_mismatch_deps = {key: tmp_requirements[key] for key in
                                     (requirements.keys() & tmp_requirements.keys())
                                     if requirements[key] != tmp_requirements[key]}
            if version_mismatch_deps:
                print_library_log("We recommend using the current stable version for the following:", Logging.Level.WARNING)
                print(version_mismatch_deps)
                if is_deploy and not force:
                    confirm = input(
                            f"Would you like us to update {REQUIREMENTS_TXT} to the current stable versions of the dependent libraries? (Y/N):")
                    if confirm.lower() == "y":
                        update_version_requirements = True
                        for requirement in version_mismatch_deps:
                            requirements[requirement] = tmp_requirements[requirement]
                    elif confirm.lower() == "n":
                        print_library_log(f"Ignored the identified dependency version conflicts. These changes are NOT made to {REQUIREMENTS_TXT}")

            missing_deps = {key: tmp_requirements[key] for key in (tmp_requirements.keys() - requirements.keys())}
            if missing_deps:
                self.handle_missing_deps(missing_deps)
                if is_deploy and not force:
                    confirm = input(
                            f"Would you like us to update {REQUIREMENTS_TXT} to add missing dependent libraries? (Y/N):")
                    if confirm.lower() == "n":
                        print_library_log(f"Ignored dependencies identified as needed. These changes are NOT made to {REQUIREMENTS_TXT}. Please review the requirements as this can fail after deploy.")
                    elif confirm.lower() == "y":
                        update_missing_requirements = True
                        for requirement in missing_deps:
                            requirements[requirement] = tmp_requirements[requirement]

            unused_deps = list(requirements.keys() - tmp_requirements.keys())
            if unused_deps:
                self.handle_unused_deps(unused_deps)
                if is_deploy and not force:
                    confirm = input(f"Would you like us to update {REQUIREMENTS_TXT} to remove the unused libraries? (Y/N):")
                    if confirm.lower() == "n":
                        if 'fivetran_connector_sdk' in unused_deps or 'requests' in unused_deps:
                            print_library_log(f"Please fix your {REQUIREMENTS_TXT} file by removing pre-installed dependencies to proceed with deployment.")
                            os._exit(1)
                        print_library_log(f"Ignored libraries identified as unused. These changes are NOT made to {REQUIREMENTS_TXT}")
                    elif confirm.lower() == "y":
                        update_unused_requirements = True
                        for requirement in unused_deps:
                            del requirements[requirement]


            if update_version_requirements or update_missing_requirements or update_unused_requirements:
                with open(requirements_file_path, "w", encoding=UTF_8) as file:
                    file.write("\n".join(requirements.values()))
                    print_library_log(f"`{REQUIREMENTS_TXT}` has been updated successfully.")

        else:
            if os.path.exists(requirements_file_path):
                print_library_log(f"{REQUIREMENTS_TXT} is not required as no additional "
                      "Python libraries are required or all required libraries for "
                      "your code are pre-installed.", Logging.Level.WARNING)
                with open(requirements_file_path, 'w') as file:
                    file.write("")


        if is_deploy: print_library_log(f"Validation of {REQUIREMENTS_TXT} completed.")

    def handle_unused_deps(self, unused_deps):
        if 'fivetran_connector_sdk' in unused_deps:
            log_unused_deps_error("fivetran_connector_sdk", __version__)
        if 'requests' in unused_deps:
            log_unused_deps_error("requests", "2.32.3")
        print_library_log("The following dependencies are not needed, "
              f"they are not used or already installed. Please remove them from {REQUIREMENTS_TXT}:", Logging.Level.WARNING)
        print(*unused_deps)

    def handle_missing_deps(self, missing_deps):
        print_library_log(f"Please include the following dependency libraries in {REQUIREMENTS_TXT}, to be used by "
              "Fivetran production. "
              "For more information, please visit: "
              "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide"
              "#workingwithrequirementstxtfile", Logging.Level.SEVERE)
        print(*list(missing_deps.values()))

    def load_or_add_requirements_file(self, requirements_file_path):
        if os.path.exists(requirements_file_path):
            requirements = self.fetch_requirements_as_dict(self, requirements_file_path)
        else:
            with open(requirements_file_path, 'w', encoding=UTF_8):
                pass
            requirements = {}
            print_library_log("Adding `requirements.txt` file to your project folder.", Logging.Level.WARNING)
        return requirements

    def copy_requirements_file_to_tmp_requirements_file(self, requirements_file_path: str, tmp_requirements_file_path):
        if os.path.exists(requirements_file_path):
            requirements_file_content = self.fetch_requirements_from_file(requirements_file_path)
            with open(tmp_requirements_file_path, 'w') as file:
                file.write("\n".join(requirements_file_content))

    @staticmethod
    def remove_unwanted_packages(requirements: dict):
        # remove the `fivetran_connector_sdk` and `requests` packages from requirements as we already pre-installed them.
        if requirements.get("fivetran_connector_sdk") is not None:
            requirements.pop("fivetran_connector_sdk")
        if requirements.get('requests') is not None:
            requirements.pop("requests")

    # Call this method to deploy the connector to Fivetran platform
    def deploy(self, args: dict, deploy_key: str, group: str, connection: str, hd_agent_id: str, configuration: dict = None):
        """Deploys the connector to the Fivetran platform.

        Args:
            args (dict): The command arguments.
            deploy_key (str): The deployment key.
            group (str): The group name.
            connection (str): The connection name.
            hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.
            configuration (dict): The configuration dictionary.
        """
        global EXECUTED_VIA_CLI
        EXECUTED_VIA_CLI = True

        print_library_log("We support only `.py` files and a `requirements.txt` file as part of the code upload. *No other code files* are supported or uploaded during the deployment process. Ensure that your code is structured accordingly and all dependencies are listed in `requirements.txt`")

        validate_deploy_parameters(connection, deploy_key)

        _check_dict(configuration, True)

        secrets_list = []
        if configuration:
            for k, v in configuration.items():
                secrets_list.append({"key": k, "value": v})

        connection_config = {
            "schema": connection,
            "secrets_list": secrets_list,
            "sync_method": "DIRECT"
        }

        if args.python_version:
            connection_config["python_version"] = args.python_version

        self.validate_requirements_file(args.project_path, True, args.force)

        group_id, group_name = self.__get_group_info(group, deploy_key)
        connection_id, service = self.__get_connection_id(connection, group, group_id, deploy_key) or (None, None)

        if connection_id:
            if service != 'connector_sdk':
                print_library_log(
                    f"The connection '{connection}' already exists and does not use the 'Connector SDK' service. You cannot update this connection.", Logging.Level.SEVERE)
                os._exit(1)
            else:
                if args.force:
                    confirm = "y"
                    if args.configuration:
                        confirm_config = "y"
                else:
                    confirm = input(
                        f"The connection '{connection}' already exists in the destination '{group}'. Updating it will overwrite the existing code. Do you want to proceed with the update? (Y/N): ")
                    if confirm.lower() == "y" and args.configuration:
                        confirm_config = input(f"Your deploy will overwrite the configuration using the values provided in '{args.configuration}': key-value pairs not present in the new configuration will be removed; existing keys' values set in the cofiguration file or in the dashboard will be overwritten with new (empty or non-empty) values; new key-value pairs will be added. Do you want to proceed with the update? (Y/N): ")
                if confirm.lower() == "y" and (not connection_config["secrets_list"] or (confirm_config.lower() == "y")):
                    print_library_log("Updating the connection...\n")
                    self.__upload_project(
                        args.project_path, deploy_key, group_id, group_name, connection)
                    response = self.__update_connection(
                        args, connection_id, connection, group_name, connection_config, deploy_key, hd_agent_id)
                    print("✓")
                    print_library_log(f"Python version {response.json()['data']['config']['python_version']} to be used at runtime.",
                                      Logging.Level.INFO)
                    print_library_log(f"Connection ID: {connection_id}")
                    print_library_log(
                        f"Visit the Fivetran dashboard to manage the connection: https://fivetran.com/dashboard/connectors/{connection_id}/status")
                else:
                    print_library_log("Update canceled. The process is now terminating.")
                    os._exit(1)
        else:
            self.__upload_project(args.project_path, deploy_key,
                                  group_id, group_name, connection)
            response = self.__create_connection(
                deploy_key, group_id, connection_config, hd_agent_id)
            if response.ok and response.status_code == HTTPStatus.CREATED:
                if Connector.__are_setup_tests_failing(response):
                    Connector.__handle_failing_tests_message_and_exit(response, "The connection was created, but setup tests failed!")
                else:
                    print_library_log(
                        f"The connection '{connection}' has been created successfully.\n")
                    connection_id = response.json()['data']['id']
                    print_library_log(f"Python version {response.json()['data']['config']['python_version']} to be used at runtime.",
                                      Logging.Level.INFO)
                    print_library_log(f"Connection ID: {connection_id}")
                    print_library_log(
                        f"Visit the Fivetran dashboard to start the initial sync: https://fivetran.com/dashboard/connectors/{connection_id}/status")
            else:
                print_library_log(
                    f"Unable to create a new connection, failed with error: {response.json()['message']}", Logging.Level.SEVERE)
                self.__cleanup_uploaded_project(deploy_key,group_id, connection)
                print_library_log("Please try again with the deploy command after resolving the issue!")
                os._exit(1)

    def __upload_project(self, project_path: str, deploy_key: str, group_id: str, group_name: str, connection: str):
        print_library_log(
            f"Deploying '{project_path}' to connection '{connection}' in destination '{group_name}'.\n")
        upload_file_path = self.__create_upload_file(project_path)
        upload_result = self.__upload(
            upload_file_path, deploy_key, group_id, connection)
        os.remove(upload_file_path)
        if not upload_result:
            os._exit(1)

    def __cleanup_uploaded_project(self, deploy_key: str, group_id: str, connection: str):
        cleanup_result = self.__cleanup_uploaded_code(deploy_key, group_id, connection)
        if not cleanup_result:
            os._exit(1)

    @staticmethod
    def __update_connection(args: dict, id: str, name: str, group: str, config: dict, deploy_key: str, hd_agent_id: str):
        """Updates the connection with the given ID, name, group, configuration, and deployment key.

        Args:
            args (dict): The command arguments.
            id (str): The connection ID.
            name (str): The connection name.
            group (str): The group name.
            config (dict): The configuration dictionary.
            deploy_key (str): The deployment key.
            hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.
        """
        if not args.configuration:
            del config["secrets_list"]

        json_payload = {
            "config": config,
            "run_setup_tests": True
        }

        # hybrid_deployment_agent_id is optional when redeploying your connection.
        # Customer can use it to change existing hybrid_deployment_agent_id.
        if hd_agent_id:
            json_payload["hybrid_deployment_agent_id"] = hd_agent_id

        response = rq.patch(f"{PRODUCTION_BASE_URL}/v1/connectors/{id}",
                        headers={"Authorization": f"Basic {deploy_key}"},
                        json=json_payload)

        if response.ok and response.status_code == HTTPStatus.OK:
            if Connector.__are_setup_tests_failing(response):
                Connector.__handle_failing_tests_message_and_exit(response, "The connection was updated, but setup tests failed!")
            else:
                print_library_log(f"Connection '{name}' in group '{group}' updated successfully.", Logging.Level.INFO)

        else:
            print_library_log(
                f"Unable to update Connection '{name}' in destination '{group}', failed with error: '{response.json()['message']}'.", Logging.Level.SEVERE)
            os._exit(1)
        return response

    @staticmethod
    def __handle_failing_tests_message_and_exit(resp, log_message):
        print_library_log(log_message, Logging.Level.SEVERE)
        Connector.__print_failing_setup_tests(resp)
        connection_id = resp.json().get('data', {}).get('id')
        print_library_log(f"Connection ID: {connection_id}")
        print_library_log("Please try again with the deploy command after resolving the issue!")
        os._exit(1)

    @staticmethod
    def __are_setup_tests_failing(response) -> bool:
        """Checks for failed setup tests in the response and returns True if any test has failed, otherwise False."""
        response_json = response.json()
        setup_tests = response_json.get("data", {}).get("setup_tests", [])

        # Return True if any test has "FAILED" status, otherwise False
        return any(test.get("status") == "FAILED" or test.get("status") == "JOB_FAILED" for test in setup_tests)


    @staticmethod
    def __print_failing_setup_tests(response):
        """Checks for failed setup tests in the response and print errors."""
        response_json = response.json()
        setup_tests = response_json.get("data", {}).get("setup_tests", [])

        # Collect failed setup tests
        failed_tests = [test for test in setup_tests if test.get("status") == "FAILED" or test.get("status") == "JOB_FAILED"]

        if failed_tests:
            print_library_log("Following setup tests have failed!", Logging.Level.WARNING)
            for test in failed_tests:
                print_library_log(f"Test: {test.get('title')}", Logging.Level.WARNING)
                print_library_log(f"Status: {test.get('status')}", Logging.Level.WARNING)
                print_library_log(f"Message: {test.get('message')}", Logging.Level.WARNING)


    @staticmethod
    def __get_connection_id(name: str, group: str, group_id: str, deploy_key: str) -> Optional[Tuple[str, str]]:
        """Retrieves the connection ID for the specified connection schema name, group, and deployment key.

        Args:
            name (str): The connection name.
            group (str): The group name.
            group_id (str): The group ID.
            deploy_key (str): The deployment key.

        Returns:
            str: The connection ID, or None
        """
        resp = rq.get(f"{PRODUCTION_BASE_URL}/v1/groups/{group_id}/connectors",
                      headers={"Authorization": f"Basic {deploy_key}"},
                      params={"schema": name})
        if not resp.ok:
            print_library_log(
                f"Unable to fetch connection list in destination '{group}'", Logging.Level.SEVERE)
            os._exit(1)

        if resp.json()['data']['items']:
            return resp.json()['data']['items'][0]['id'], resp.json()['data']['items'][0]['service']

        return None

    @staticmethod
    def __create_connection(deploy_key: str, group_id: str, config: dict, hd_agent_id: str) -> rq.Response:
        """Creates a new connection with the given deployment key, group ID, and configuration.

        Args:
            deploy_key (str): The deployment key.
            group_id (str): The group ID.
            config (dict): The configuration dictionary.
            hd_agent_id (str): The hybrid deployment agent ID within the Fivetran system.

        Returns:
            rq.Response: The response object.
        """
        response = rq.post(f"{PRODUCTION_BASE_URL}/v1/connectors",
                           headers={"Authorization": f"Basic {deploy_key}"},
                           json={
                               "group_id": group_id,
                               "service": "connector_sdk",
                               "config": config,
                               "paused": True,
                               "run_setup_tests": True,
                               "sync_frequency": "360",
                               "hybrid_deployment_agent_id": hd_agent_id
                           })
        return response

    def __create_upload_file(self, project_path: str) -> str:
        """Creates an upload file for the given project path.

        Args:
            project_path (str): The path to the project.

        Returns:
            str: The path to the upload file.
        """
        print_library_log("Packaging your project for upload...")
        zip_file_path = self.__zip_folder(project_path)
        print("✓")
        return zip_file_path

    def __zip_folder(self, project_path: str) -> str:
        """Zips the folder at the given project path.

        Args:
            project_path (str): The path to the project.

        Returns:
            str: The path to the zip file.
        """
        upload_filepath = os.path.join(project_path, UPLOAD_FILENAME)
        connector_file_exists = False
        custom_drivers_exists = False
        custom_driver_installation_script_exists = False

        with ZipFile(upload_filepath, 'w', ZIP_DEFLATED) as zipf:
            for root, files in self.__dir_walker(project_path):
                if os.path.basename(root) == DRIVERS:
                    custom_drivers_exists = True
                if INSTALLATION_SCRIPT in files:
                    custom_driver_installation_script_exists = True
                for file in files:
                    if file == ROOT_FILENAME:
                        connector_file_exists = True
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, project_path)
                    zipf.write(file_path, arcname)

        if not connector_file_exists:
            print_library_log(
                "The 'connector.py' file is missing. Please ensure that 'connector.py' is present in your project directory, and that the file name is in lowercase letters. All custom connectors require this file because Fivetran calls it to start a sync.", Logging.Level.SEVERE)
            os._exit(1)

        if custom_drivers_exists and not custom_driver_installation_script_exists:
            print_library_log(INSTALLATION_SCRIPT_MISSING_MESSAGE, Logging.Level.SEVERE)
            os._exit(1)

        return upload_filepath

    def __dir_walker(self, top):
        """Walks the directory tree starting at the given top directory.

        Args:
            top (str): The top directory to start the walk.

        Yields:
            tuple: A tuple containing the current directory path and a list of files.
        """
        dirs, files = [], []
        for name in os.listdir(top):
            path = os.path.join(top, name)
            if os.path.isdir(path):
                if (name not in EXCLUDED_DIRS) and (not name.startswith(".")):
                    if VIRTUAL_ENV_CONFIG not in os.listdir(path):  # Check for virtual env indicator
                        dirs.append(name)
            else:
                # Include all files if in `drivers` folder
                if os.path.basename(top) == DRIVERS:
                    files.append(name)
                if name.endswith(".py") or name == "requirements.txt":
                    files.append(name)

        yield top, files
        for name in dirs:
            new_path = os.path.join(top, name)
            for x in self.__dir_walker(new_path):
                yield x

    @staticmethod
    def __upload(local_path: str, deploy_key: str, group_id: str, connection: str) -> bool:
        """Uploads the local code file for the specified group and connection.

        Args:
            local_path (str): The local file path.
            deploy_key (str): The deployment key.
            group_id (str): The group ID.
            connection (str): The connection name.

        Returns:
            bool: True if the upload was successful, False otherwise.
        """
        print_library_log("Uploading your project...")
        response = rq.post(f"{PRODUCTION_BASE_URL}/v1/deploy/{group_id}/{connection}",
                           files={'file': open(local_path, 'rb')},
                           headers={"Authorization": f"Basic {deploy_key}"})
        if response.ok:
            print("✓")
            return True

        print_library_log(f"Unable to upload the project, failed with error: {response.reason}", Logging.Level.SEVERE)
        return False

    @staticmethod
    def __cleanup_uploaded_code(deploy_key: str, group_id: str, connection: str) -> bool:
        """Cleans up the uploaded code file for the specified group and connection, if creation fails.

        Args:
            deploy_key (str): The deployment key.
            group_id (str): The group ID.
            connection (str): The connection name.

        Returns:
            bool: True if the cleanup was successful, False otherwise.
        """
        print_library_log("INFO: Cleaning up your uploaded project ")
        response = rq.post(f"{PRODUCTION_BASE_URL}/v1/cleanup_code/{group_id}/{connection}",
                           headers={"Authorization": f"Basic {deploy_key}"})
        if response.ok:
            print("✓")
            return True

        print_library_log(f"SEVERE: Unable to cleanup the project, failed with error: {response.reason}", Logging.Level.SEVERE)
        return False

    @staticmethod
    def __get_os_arch_suffix() -> str:
        """
        Returns the operating system and architecture suffix for the current operating system.
        """
        system = platform.system().lower()
        machine = platform.machine().lower()

        if system not in OS_MAP:
            raise RuntimeError(f"Unsupported OS: {system}")

        plat = OS_MAP[system]

        if machine not in ARCH_MAP or (plat == WIN_OS and ARCH_MAP[machine] != X64):
            raise RuntimeError(f"Unsupported architecture '{machine}' for {plat}")

        return f"{plat}-{ARCH_MAP[machine]}"

    @staticmethod
    def __get_group_info(group: str, deploy_key: str) -> tuple[str, str]:
        """Retrieves the group information for the specified group and deployment key.

        Args:
            group (str): The group name.
            deploy_key (str): The deployment key.

        Returns:
            tuple[str, str]: A tuple containing the group ID and group name.
        """
        groups_url = f"{PRODUCTION_BASE_URL}/v1/groups"

        params = {"limit": 500}
        headers = {"Authorization": f"Basic {deploy_key}"}
        resp = rq.get(groups_url, headers=headers, params=params)

        if not resp.ok:
            print_library_log(
                f"The request failed with status code: {resp.status_code}. Please ensure you're using a valid base64-encoded API key and try again.", Logging.Level.SEVERE)
            os._exit(1)

        data = resp.json().get("data", {})
        groups = data.get("items")

        if not groups:
            print_library_log("No destinations defined in the account", Logging.Level.SEVERE)
            os._exit(1)

        if not group:
            if len(groups) == 1:
                return groups[0]['id'], groups[0]['name']
            else:
                print_library_log(
                    "Destination name is required when there are multiple destinations in the account", Logging.Level.SEVERE)
                os._exit(1)
        else:
            while True:
                for grp in groups:
                    if grp['name'] == group:
                        return grp['id'], grp['name']

                next_cursor = data.get("next_cursor")
                if not next_cursor:
                    break

                params = {"cursor": next_cursor, "limit": 500}
                resp = rq.get(groups_url, headers=headers, params=params)
                data = resp.json().get("data", {})
                groups = data.get("items", [])

        print_library_log(
            f"The specified destination '{group}' was not found in your account.", Logging.Level.SEVERE)
        os._exit(1)

    # Call this method to run the connector in production
    def run(self,
            port: int = 50051,
            configuration: dict = None,
            state: dict = None,
            log_level: Logging.Level = Logging.Level.INFO) -> grpc.Server:
        """Runs the connector server.

        Args:
            port (int): The port number to listen for incoming requests.
            configuration (dict): The configuration dictionary.
            state (dict): The state dictionary.
            log_level (Logging.Level): The logging level.

        Returns:
            grpc.Server: The gRPC server instance.
        """
        self.configuration = _check_dict(configuration, True)
        self.state = _check_dict(state)
        Logging.LOG_LEVEL = log_level

        if not DEBUGGING:
            print_library_log(f"Running on fivetran_connector_sdk: {__version__}")

        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        connector_sdk_pb2_grpc.add_ConnectorServicer_to_server(self, server)
        server.add_insecure_port("[::]:" + str(port))
        server.start()
        if DEBUGGING:
            return server
        server.wait_for_termination()

    # This method starts both the server and the local testing environment
    def debug(self,
              project_path: str = None,
              configuration: dict = None,
              state: dict = None,
              log_level: Logging.Level = Logging.Level.FINE):
        """Tests the connector code by running it with the connector tester.\n
        state.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithstatejsonfile\n
        configuration.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile

        Args:
            project_path (str): The path to the project.
            configuration (dict): The configuration dictionary, same as configuration.json if present.
            state (dict): The state dictionary, same as state.json if present.
            log_level (Logging.Level): The logging level.
        """
        global DEBUGGING
        DEBUGGING = True

        check_newer_version()

        Logging.LOG_LEVEL = log_level
        os_arch_suffix = self.__get_os_arch_suffix()
        tester_root_dir = _tester_root_dir()
        java_exe = self.__java_exe(tester_root_dir, os_arch_suffix)
        install_tester = False
        version_file = os.path.join(tester_root_dir, VERSION_FILENAME)
        if os.path.isfile(version_file):
            # Check version number & update if different
            with open(version_file, 'r', encoding=UTF_8) as fi:
                current_version = fi.readline()

            if current_version != TESTER_VERSION:
                shutil.rmtree(tester_root_dir)
                install_tester = True
        else:
            install_tester = True

        if install_tester:
            os.makedirs(tester_root_dir, exist_ok=True)
            download_filename = f"sdk-connector-tester-{os_arch_suffix}-{TESTER_VERSION}.zip"
            download_filepath = os.path.join(tester_root_dir, download_filename)
            try:
                print_library_log(f"Downloading connector tester version: {TESTER_VERSION} ")
                download_url = f"https://github.com/fivetran/fivetran_sdk_tools/releases/download/{TESTER_VERSION}/{download_filename}"
                r = rq.get(download_url)
                if r.ok:
                    with open(download_filepath, 'wb') as fo:
                        fo.write(r.content)
                else:
                    raise RuntimeError(
                        f"\nSEVERE: Failed to download the connector tester. Please check your access permissions or "
                        f"try again later ( status code: {r.status_code}), url: {download_url}")
            except RuntimeError:
                raise RuntimeError(
                    f"SEVERE: Failed to download the connector tester. Error details: {traceback.format_exc()}")

            try:
                # unzip it
                with ZipFile(download_filepath, 'r') as z_object:
                    z_object.extractall(path=tester_root_dir)
                # delete zip file
                os.remove(download_filepath)
                # make java binary executable
                import stat
                st = os.stat(java_exe)
                os.chmod(java_exe, st.st_mode | stat.S_IEXEC)
                print("✓")
            except:
                shutil.rmtree(tester_root_dir)
                raise RuntimeError(f"\nSEVERE: Failed to install the connector tester. Error details: {traceback.format_exc()}")

        project_path = os.getcwd() if project_path is None else project_path
        self.validate_requirements_file(project_path, False)
        print_library_log(f"Debugging connector at: {project_path}")
        available_port = get_available_port()
        _exit_check(project_path)

        if available_port is None:
            raise RuntimeError("SEVERE: Unable to allocate a port in the range 50051-50061. "
                               "Please ensure a port is available and try again")

        server = self.run(available_port, configuration, state, log_level=log_level)

        # Uncomment this to run the tester manually
        # server.wait_for_termination()

        try:
            print_library_log("Running connector tester...")
            for log_msg in self.__run_tester(java_exe, tester_root_dir, project_path, available_port, json.dumps(self.state), json.dumps(self.configuration)):
                print(log_msg, end="")
        except:
            print(traceback.format_exc())
        finally:
            server.stop(grace=2.0)

    @staticmethod
    def __java_exe(location: str, os_arch_suffix: str) -> str:
        """Returns the path to the Java executable.

        Args:
            location (str): The location of the Java executable.
            os_arch_suffix (str): The name of the operating system and architecture

        Returns:
            str: The path to the Java executable.
        """
        java_exe_base = os.path.join(location, "bin", "java")
        return f"{java_exe_base}.exe" if os_arch_suffix == f"{WIN_OS}-{X64}" else java_exe_base

    @staticmethod
    def process_stream(stream):
        """Processes a stream of text lines, replacing occurrences of a specified pattern.

        This method reads each line from the provided stream, searches for occurrences of
        a predefined pattern, and replaces them with a specified replacement string.

        Args:
            stream (iterable): An iterable stream of text lines, typically from a file or another input source.

        Yields:
            str: Each line from the stream after replacing the matched pattern with the replacement string.
        """
        pattern = r'com\.fivetran\.partner_sdk.*\.tools\.testers\.\S+'

        for line in iter(stream.readline, ""):
            if not re.search(pattern, line):
                yield line

    @staticmethod
    def __run_tester(java_exe: str, root_dir: str, project_path: str, port: int, state_json: str, configuration_json: str):
        """Runs the connector tester.

        Args:
            java_exe (str): The path to the Java executable.
            root_dir (str): The root directory.
            project_path (str): The path to the project.

        Yields:
            str: The log messages from the tester.
        """
        working_dir = os.path.join(project_path, OUTPUT_FILES_DIR)
        try:
            os.mkdir(working_dir)
        except FileExistsError:
            pass

        cmd = [java_exe,
               "-jar",
               os.path.join(root_dir, TESTER_FILENAME),
               "--connector-sdk=true",
               f"--port={port}",
               f"--working-dir={working_dir}",
               "--tester-type=source",
               f"--state={state_json}",
               f"--configuration={configuration_json}"]

        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        for line in Connector.process_stream(popen.stderr):
            yield Connector._maybe_colorize_jar_output(line)

        for line in Connector.process_stream(popen.stdout):
            yield Connector._maybe_colorize_jar_output(line)
        popen.stdout.close()
        return_code = popen.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)

    # -- Methods below override ConnectorServicer methods
    def ConfigurationForm(self, request, context):
        """Overrides the ConfigurationForm method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            common_pb2.ConfigurationFormResponse: An empty configuration form response.
        """
        if not self.configuration:
            self.configuration = {}

        # Not going to use the tester's configuration file
        return common_pb2.ConfigurationFormResponse()

    def Test(self, request, context):
        """Overrides the Test method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            None: As this method is not implemented.
        """
        return None

    def Schema(self, request, context):
        """Overrides the Schema method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Returns:
            connector_sdk_pb2.SchemaResponse: The schema response.
        """
        global TABLES
        table_list = {}

        if not self.schema_method:
            return connector_sdk_pb2.SchemaResponse(schema_response_not_supported=True)
        else:
            try:
                configuration = self.configuration if self.configuration else request.configuration
                print_library_log("Initiating the 'schema' method call...", Logging.Level.INFO)
                response = self.schema_method(configuration)
                self.process_tables(response, table_list)
                return connector_sdk_pb2.SchemaResponse(without_schema=common_pb2.TableList(tables=TABLES.values()))

            except Exception as e:
                tb = traceback.format_exc()
                error_message = f"Error: {str(e)}\n{tb}"
                print_library_log(error_message, Logging.Level.SEVERE)
                raise RuntimeError(error_message) from e

    def process_tables(self, response, table_list):
        for entry in response:
            if 'table' not in entry:
                raise ValueError("Entry missing table name: " + entry)

            table_name = get_renamed_table_name(entry['table'])

            if table_name in table_list:
                raise ValueError("Table already defined: " + table_name)

            table = common_pb2.Table(name=table_name)
            columns = {}

            if "primary_key" in entry:
                self.process_primary_keys(columns, entry)

            if "columns" in entry:
                self.process_columns(columns, entry)

            table.columns.extend(columns.values())
            TABLES[table_name] = table
            table_list[table_name] = table

    def process_primary_keys(self, columns, entry):
        for pkey_name in entry["primary_key"]:
            column_name = get_renamed_column_name(pkey_name)
            column = columns[column_name] if column_name in columns else common_pb2.Column(name=column_name)
            column.primary_key = True
            columns[column_name] = column

    def process_columns(self, columns, entry):
        for name, type in entry["columns"].items():
            column_name = get_renamed_column_name(name)
            column = columns[column_name] if column_name in columns else common_pb2.Column(name=column_name)

            if isinstance(type, str):
                self.process_data_type(column, type)

            elif isinstance(type, dict):
                if type['type'].upper() != "DECIMAL":
                    raise ValueError("Expecting DECIMAL data type")
                column.type = common_pb2.DataType.DECIMAL
                column.decimal.precision = type['precision']
                column.decimal.scale = type['scale']

            else:
                raise ValueError("Unrecognized column type: ", str(type))

            if "primary_key" in entry and name in entry["primary_key"]:
                column.primary_key = True

            columns[column_name] = column

    def process_data_type(self, column, type):
        if type.upper() == "BOOLEAN":
            column.type = common_pb2.DataType.BOOLEAN
        elif type.upper() == "SHORT":
            column.type = common_pb2.DataType.SHORT
        elif type.upper() == "INT":
            column.type = common_pb2.DataType.INT
        elif type.upper() == "LONG":
            column.type = common_pb2.DataType.LONG
        elif type.upper() == "DECIMAL":
            raise ValueError("DECIMAL data type missing precision and scale")
        elif type.upper() == "FLOAT":
            column.type = common_pb2.DataType.FLOAT
        elif type.upper() == "DOUBLE":
            column.type = common_pb2.DataType.DOUBLE
        elif type.upper() == "NAIVE_DATE":
            column.type = common_pb2.DataType.NAIVE_DATE
        elif type.upper() == "NAIVE_DATETIME":
            column.type = common_pb2.DataType.NAIVE_DATETIME
        elif type.upper() == "UTC_DATETIME":
            column.type = common_pb2.DataType.UTC_DATETIME
        elif type.upper() == "BINARY":
            column.type = common_pb2.DataType.BINARY
        elif type.upper() == "XML":
            column.type = common_pb2.DataType.XML
        elif type.upper() == "STRING":
            column.type = common_pb2.DataType.STRING
        elif type.upper() == "JSON":
            column.type = common_pb2.DataType.JSON
        else:
            raise ValueError("Unrecognized column type encountered:: ", str(type))

    def Update(self, request, context):
        """Overrides the Update method from ConnectorServicer.

        Args:
            request: The gRPC request.
            context: The gRPC context.

        Yields:
            connector_sdk_pb2.UpdateResponse: The update response.
        """
        configuration = self.configuration if self.configuration else request.configuration
        state = self.state if self.state else json.loads(request.state_json)

        try:
            print_library_log("Initiating the 'update' method call...", Logging.Level.INFO)
            for resp in self.update_method(configuration=configuration, state=state):
                if isinstance(resp, list):
                    for r in resp:
                        yield r
                else:
                    yield resp

        except TypeError as e:
            if str(e) != "'NoneType' object is not iterable":
                raise e

        except Exception as e:
            tb = traceback.format_exc()
            error_message = f"Error: {str(e)}\n{tb}"
            print_library_log(error_message, Logging.Level.SEVERE)
            raise RuntimeError(error_message) from e

    @staticmethod
    def _maybe_colorize_jar_output(line: str) -> str:
        if not DEBUGGING:
            return line

        if "SEVERE" in line or "ERROR" in line or "Exception" in line or "FAILED" in line:
            return f"\033[91m{line}\033[0m"  # Red
        elif "WARN" in line or "WARNING" in line:
            return f"\033[93m{line}\033[0m"  # Yellow
        return line


def find_connector_object(project_path) -> Optional[Connector]:
    """Finds the connector object in the given project path.
    Args:
        project_path (str): The path to the project.
    
    Returns:
        Optional[Connector]: The connector object or None if not found.
    """

    sys.path.append(project_path) # Allows python interpreter to search for modules in this path
    module_name = "connector_connector_code"
    connector_py = os.path.join(project_path, ROOT_FILENAME)
    try:
        spec = importlib.util.spec_from_file_location(module_name, connector_py)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        for obj in dir(module):
            if not obj.startswith('__'):  # Exclude built-in attributes
                obj_attr = getattr(module, obj)
                if '<fivetran_connector_sdk.Connector object at' in str(obj_attr):
                    return obj_attr
    except FileNotFoundError:
        print_library_log(
            "The connector object is missing in the current directory. Please ensure that you are running the command from correct directory or that you have defined a connector object using the correct syntax in your `connector.py` file. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector", Logging.Level.SEVERE)
        return None

    print_library_log(
        "The connector object is missing. Please ensure that you have defined a connector object using the correct syntax in your `connector.py` file. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector", Logging.Level.SEVERE)
    return None


def suggest_correct_command(input_command: str) -> bool:
    # for typos
    # calculate the edit distance of the input command (lowercased) with each of the valid commands
    edit_distances_of_commands = sorted(
        [(command, edit_distance(command, input_command.lower())) for command in VALID_COMMANDS], key=lambda x: x[1])

    if edit_distances_of_commands[0][1] <= MAX_ALLOWED_EDIT_DISTANCE_FROM_VALID_COMMAND:
        # if the closest command is within the max allowed edit distance, we suggest that command
        # threshold is kept to prevent suggesting a valid command for an obvious wrong command like `fivetran iknowthisisntacommandbuttryanyway`
        print_suggested_command_message(edit_distances_of_commands[0][0], input_command)
        return True

    # for synonyms
    for (command, synonyms) in COMMANDS_AND_SYNONYMS.items():
        # check if the input command (lowercased) is a recognised synonym of any of the valid commands, if yes, suggest that command
        if input_command.lower() in synonyms:
            print_suggested_command_message(command, input_command)
            return True

    return False


def print_suggested_command_message(valid_command: str, input_command: str) -> None:
    print_library_log(f"`fivetran {input_command}` is not a valid command.", Logging.Level.SEVERE)
    print_library_log(f"Did you mean `fivetran {valid_command}`?", Logging.Level.SEVERE)
    print_library_log("Use `fivetran --help` for more details.", Logging.Level.SEVERE)


def edit_distance(first_string: str, second_string: str) -> int:
    first_string_length: int = len(first_string)
    second_string_length: int = len(second_string)

    # Initialize the previous row of distances (for the base case of an empty first string) 'previous_row[j]' holds
    # the edit distance between an empty prefix of 'first_string' and the first 'j' characters of 'second_string'.
    # The first row is filled with values [0, 1, 2, ..., second_string_length]
    previous_row: list[int] = list(range(second_string_length + 1))

    # Rest of the rows
    for first_string_index in range(1, first_string_length + 1):
        # Start the current row with the distance for an empty second string
        current_row: list[int] = [first_string_index]

        # Iterate over each character in the second string
        for second_string_index in range(1, second_string_length + 1):
            if first_string[first_string_index - 1] == second_string[second_string_index - 1]:
                # If characters match, no additional cost
                current_row.append(previous_row[second_string_index - 1])
            else:
                # Minimum cost of insertion, deletion, or substitution
                current_row.append(
                    1 + min(current_row[-1], previous_row[second_string_index], previous_row[second_string_index - 1]))

        # Move to the next row
        previous_row = current_row

    # The last value in the last row is the edit distance
    return previous_row[second_string_length]


def get_input_from_cli(prompt : str, default_value: str) -> str:
    """
    Prompts the user for input.
    """
    if default_value:
        value = input(f"{prompt} [Default : {default_value}]: ").strip() or default_value
    else:
        value = input(f"{prompt}: ").strip()

    if not value:
        raise ValueError("Missing required input: Expected a value but received None")
    return value


def main():
    """The main entry point for the script.
    Parses command line arguments and passes them to connector object methods
    """
    global EXECUTED_VIA_CLI
    EXECUTED_VIA_CLI = True

    parser = argparse.ArgumentParser(allow_abbrev=False, add_help=True)
    parser._option_string_actions["-h"].help = "Show this help message and exit"

    # Positional
    parser.add_argument("command", help="|".join(VALID_COMMANDS))
    parser.add_argument("project_path", nargs='?', default=os.getcwd(), help="Path to connector project directory")

    # Optional (Not all of these are valid with every mutually exclusive option below)
    parser.add_argument("--state", type=str, default=None, help="Provide state as JSON string or file")
    parser.add_argument("--configuration", type=str, default=None, help="Provide secrets as JSON file")
    parser.add_argument("--api-key", type=str, default=None, help="Provide your base64-encoded API key for deployment")
    parser.add_argument("--destination", type=str, default=None, help="Destination name (aka 'group name')")
    parser.add_argument("--connection", type=str, default=None, help="Connection name (aka 'destination schema')")
    parser.add_argument("-f", "--force", action="store_true", help="Force update an existing connection")
    parser.add_argument("--python-version", "--python", type=str, help=f"Supported Python versions you can use: {SUPPORTED_PYTHON_VERSIONS}. Defaults to {DEFAULT_PYTHON_VERSION}")
    parser.add_argument("--hybrid-deployment-agent-id", type=str, help="The Hybrid Deployment agent within the Fivetran system. If nothing is passed, the default agent of the destination is used.")

    args = parser.parse_args()

    if args.command.lower() == "version":
        print_library_log("fivetran_connector_sdk " + __version__)
        return
    elif args.command.lower() == "reset":
        reset_local_file_directory(args)
        return

    connector_object = find_connector_object(args.project_path)

    if not connector_object:
        sys.exit(1)

    # Process optional args
    ft_group = args.destination if args.destination else None
    ft_connection = args.connection if args.connection else None
    ft_deploy_key = args.api_key if args.api_key else None
    hd_agent_id = args.hybrid_deployment_agent_id if args.hybrid_deployment_agent_id else os.getenv(FIVETRAN_HD_AGENT_ID, None)
    configuration = args.configuration if args.configuration else None
    state = args.state if args.state else os.getenv('FIVETRAN_STATE', None)

    configuration = validate_and_load_configuration(args, configuration)
    state = validate_and_load_state(args, state)

    FIVETRAN_API_KEY = os.getenv('FIVETRAN_API_KEY', None)
    FIVETRAN_DESTINATION_NAME = os.getenv('FIVETRAN_DESTINATION_NAME', None)
    FIVETRAN_CONNECTION_NAME = os.getenv('FIVETRAN_CONNECTION_NAME', None)

    if args.command.lower() == "deploy":
        if args.state:
            print_library_log("'state' parameter is not used for 'deploy' command", Logging.Level.WARNING)

        if not ft_deploy_key:
            ft_deploy_key = get_input_from_cli("Please provide the API Key", FIVETRAN_API_KEY)

        if not ft_group:
            ft_group = get_input_from_cli("Please provide the destination", FIVETRAN_DESTINATION_NAME)

        if not ft_connection:
            ft_connection = get_input_from_cli("Please provide the connection name",FIVETRAN_CONNECTION_NAME)

        connector_object.deploy(args, ft_deploy_key, ft_group, ft_connection, hd_agent_id, configuration)

    elif args.command.lower() == "debug":
        connector_object.debug(args.project_path, configuration, state)
    else:
        if not suggest_correct_command(args.command):
            raise NotImplementedError(f"Invalid command: {args.command}, see `fivetran --help`")


def validate_and_load_configuration(args, configuration):
    if configuration:
        json_filepath = os.path.join(args.project_path, args.configuration)
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r', encoding=UTF_8) as fi:
                configuration = json.load(fi)
            if len(configuration) > MAX_CONFIG_FIELDS:
                raise ValueError(f"Configuration field count exceeds maximum of {MAX_CONFIG_FIELDS}. Reduce the field count.")
        else:
            raise ValueError(
                "Configuration must be provided as a JSON file. Please check your input. Reference: "
                "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile")
    else:
        json_filepath = os.path.join(args.project_path, "configuration.json")
        if os.path.exists(json_filepath):
            print_library_log("Configuration file detected in the project, but no configuration input provided via the command line", Logging.Level.WARNING)
        configuration = {}
    return configuration


def validate_and_load_state(args, state):
    if state:
        json_filepath = os.path.join(args.project_path, args.state)
    else:
        json_filepath = os.path.join(args.project_path, "files", "state.json")

    if os.path.exists(json_filepath):
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r', encoding=UTF_8) as fi:
                state = json.load(fi)
        elif state.lstrip().startswith("{"):
            state = json.loads(state)
    else:
        state = {}
    return state


def reset_local_file_directory(args):
    files_path = os.path.join(args.project_path, OUTPUT_FILES_DIR)
    confirm = input(
        "This will delete your current state and `warehouse.db` files. Do you want to continue? (Y/N): ")
    if confirm.lower() != "y":
        print_library_log("Reset canceled")
    else:
        try:
            if os.path.exists(files_path) and os.path.isdir(files_path):
                shutil.rmtree(files_path)
            print_library_log("Reset Successful")
        except Exception as e:
            print_library_log("Reset Failed", Logging.Level.SEVERE)
            raise e


if __name__ == "__main__":
    main()
