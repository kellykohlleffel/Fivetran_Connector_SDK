import argparse
from typing import Optional, Tuple

import grpc
import importlib.util
import inspect
import json
import os
import platform
import requests as rq
import shutil
import subprocess
import sys
import time
import traceback
import re

from concurrent import futures
from datetime import datetime
from enum import IntEnum
from google.protobuf import timestamp_pb2
from zipfile import ZipFile, ZIP_DEFLATED

from fivetran_connector_sdk.protos import common_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2
from fivetran_connector_sdk.protos import connector_sdk_pb2_grpc

__version__ = "0.10.15.1"

MAC_OS = "mac"
WIN_OS = "windows"
LINUX_OS = "linux"

TESTER_VERSION = "0.24.0916.001"
TESTER_FILENAME = "run_sdk_tester.jar"
VERSION_FILENAME = "version.txt"
UPLOAD_FILENAME = "code.zip"
LAST_VERSION_CHECK_FILE = "_last_version_check"
ROOT_LOCATION = ".ft_sdk_connector_tester"
OUTPUT_FILES_DIR = "files"
REQUIREMENTS_TXT = "requirements.txt"
ONE_DAY_IN_SEC = 24 * 60 * 60

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
TABLES = {}


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
            print(message)
        else:
            print(f'{{"level":"{level.name}", "message": "{message}", "message-origin": "connector_sdk"}}')

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
    def severe(message: str):
        """Logs a severe-level message.

        Args:
            message (str): The message to log.
        """
        if Logging.LOG_LEVEL <= Logging.Level.SEVERE:
            Logging.__log(Logging.Level.SEVERE, message)


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
        _yield_check(inspect.stack())

        responses = []

        columns = _get_columns(table)
        if not columns:
            global TABLES
            for field in data.keys():
                columns[field] = common_pb2.Column(
                    name=field, type=common_pb2.DataType.UNSPECIFIED, primary_key=False)

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
        _yield_check(inspect.stack())

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
        _yield_check(inspect.stack())

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
        with open(last_check_file_path, 'r') as f_in:
            timestamp = int(f_in.read())
            if (int(time.time()) - timestamp) < ONE_DAY_IN_SEC:
                return

    # check version and save current time
    from get_pypi_latest_version import GetPyPiLatestVersion
    obtainer = GetPyPiLatestVersion()
    latest_version = obtainer('fivetran_connector_sdk')
    if __version__ < latest_version:
        print(f"[notice] A new release of 'fivetran-connector-sdk' is available: {latest_version}\n" +
              f"[notice] To update, run: pip install --upgrade fivetran-connector-sdk\n")

    with open(last_check_file_path, 'w') as f_out:
        f_out.write(f"{int(time.time())}")


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
        if v is None:
            mapped_data[k] = common_pb2.ValueType(null=True)
        elif (k in columns) and columns[k].type != common_pb2.DataType.UNSPECIFIED:
            map_defined_data_type(columns, k, mapped_data, v)
        else:
            map_inferred_data_type(k, mapped_data, v)

    return mapped_data


def map_inferred_data_type(k, mapped_data, v):
    # We can infer type from the value
    if isinstance(v, int):
        if abs(v) > 2147483647:
            mapped_data[k] = common_pb2.ValueType(long=v)
        else:
            mapped_data[k] = common_pb2.ValueType(int=v)
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
        if '.' in v:
            dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f%z")
        else:
            dt = datetime.strptime(v, "%Y-%m-%dT%H:%M:%S%z")
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
    if not DEBUGGING:
        return

    called_method = stack[0].function
    calling_code = stack[1].code_context[0]
    if f"{called_method}(" in calling_code:
        if 'yield' not in calling_code:
            print(
                f"SEVERE: Please add 'yield' to '{called_method}' operation on line {stack[1].lineno} in file '{stack[1].filename}'")
            os._exit(1)
    else:
        # This should never happen
        raise RuntimeError(
            f"The '{called_method}' function is missing in the connector. Please ensure that the '{called_method}' function is properly defined in your code to proceed. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsmethods")


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
                print(
                    "SEVERE: All values in the configuration must be STRING. Please check your configuration and ensure that every value is a STRING.")
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
    print(f"ERROR: Please remove `{package_name}` from requirements.txt."
          f" The latest version of `{package_name}` is always available when executing your code."
          f" Current version: {version}")
    os._exit(1)


def validate_deploy_parameters(connection, deploy_key):
    if not deploy_key or not connection:
        print("SEVERE: The deploy command needs the following parameters:"
              "\n\tRequired:\n"
              "\t\t--api-key <BASE64-ENCODED-FIVETRAN-API-KEY-FOR-DEPLOYMENT>\n"
              "\t\t--connection <VALID-CONNECTOR-SCHEMA_NAME>\n"
              "\t(Optional):\n"
              "\t\t--destination <DESTINATION_NAME> (Becomes required if there are multiple destinations)\n"
              "\t\t--configuration <CONFIGURATION_FILE> (Completely replaces the existing configuration)")
        os._exit(1)
    if not is_connection_name_valid(connection):
        print(f"SEVERE: Connection name: {connection} is invalid!\n The connection name should start with an "
              f"underscore or a lowercase letter (a-z), followed by any combination of underscores, lowercase "
              f"letters, or digits (0-9). Uppercase characters are not allowed.")
        os._exit(1)


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

    @staticmethod
    def fetch_requirements_from_file(file_path: str) -> list[str]:
        """Reads a requirements file and returns a list of dependencies.

        Args:
            file_path (str): The path to the requirements file.

        Returns:
            list[str]: A list of dependencies as strings.
        """
        with open(file_path, 'r') as f:
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
        for requirement in self.fetch_requirements_from_file(file_path):
            requirement = requirement.strip()
            if not requirement or requirement.startswith("#"):  # Skip empty lines and comments
                continue
            try:
                key = re.split(r"==|>=|<=|>|<", requirement)[0]
                requirements_dict[key.lower()] = requirement.lower()
            except ValueError:
                print(f"Error: Invalid requirement format: '{requirement}'")
        return requirements_dict

    def validate_requirements_file(self, project_path: str, is_deploy: bool):
        """Validates the `requirements.txt` file against the project's actual dependencies.

        This method generates a temporary requirements file using `pipreqs`, compares
        it with the existing `requirements.txt`, and checks for version mismatches,
        missing dependencies, and unused dependencies. It will issue warnings, errors,
        or even terminate the process depending on whether it's being run for deployment.

        Args:
            project_path (str): The path to the project directory containing the `requirements.txt`.
            is_deploy (bool): If `True`, the method will exit the process on critical errors.

        """
        subprocess.check_call(["pipreqs", "--savepath", "tmp_requirements.txt", "--ignore"] + EXCLUDED_PIPREQS_DIRS,
                              stderr=subprocess.PIPE)
        tmp_requirements_file_path = os.path.join(project_path, 'tmp_requirements.txt')

        tmp_requirements = self.fetch_requirements_as_dict(self, tmp_requirements_file_path)
        tmp_requirements.pop("fivetran_connector_sdk")
        if tmp_requirements.get('requests') is not None:
            tmp_requirements.pop("requests")
        os.remove(tmp_requirements_file_path)

        if len(tmp_requirements) > 0:
            requirements = self.load_or_add_requirements_file(project_path)

            version_mismatch_deps = {key: tmp_requirements[key] for key in
                                     (requirements.keys() & tmp_requirements.keys())
                                     if requirements[key] != tmp_requirements[key]}
            if version_mismatch_deps:
                print("WARNING: We recommend using the current stable version for the following:")
                print(version_mismatch_deps)

            missing_deps = {key: tmp_requirements[key] for key in (tmp_requirements.keys() - requirements.keys())}
            if missing_deps:
                self.handle_missing_deps(is_deploy, missing_deps)

            unused_deps = list(requirements.keys() - tmp_requirements.keys())
            if unused_deps:
                self.handle_unused_deps(unused_deps)
        else:
            if os.path.exists(REQUIREMENTS_TXT):
                print("WARNING: `requirements.txt` is not required as no additional "
                      "Python libraries are required or all required libraries for "
                      "your code are pre-installed.")

        if is_deploy: print("Successful validation of requirements.txt")

    def handle_unused_deps(self, unused_deps):
        if 'fivetran_connector_sdk' in unused_deps:
            log_unused_deps_error("fivetran_connector_sdk", __version__)
        elif 'requests' in unused_deps:
            log_unused_deps_error("requests", "2.32.3")
        else:
            print("INFO: The following dependencies are not needed, "
                  "they are not used or already installed. Please remove them from requirements.txt:")
            print(*unused_deps)

    def handle_missing_deps(self, is_deploy, missing_deps):
        log_level = "ERROR" if is_deploy else "WARNING"
        print(log_level +
              ":  Please include the following dependency libraries in requirements.txt, to be used by "
              "Fivetran production. "
              "For more information, please visit: "
              "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide"
              "#workingwithrequirementstxtfile")
        print(*list(missing_deps.values()))
        if is_deploy:
            os._exit(1)

    def load_or_add_requirements_file(self, project_path):
        if os.path.exists(REQUIREMENTS_TXT):
            requirements = self.fetch_requirements_as_dict(self, os.path.join(project_path, 'requirements.txt'))
        else:
            with open(REQUIREMENTS_TXT, 'w'):
                pass
            requirements = {}
            print("WARNING: Adding `requirements.txt` file to your project folder.")
        return requirements

    # Call this method to deploy the connector to Fivetran platform
    def deploy(self, project_path: str, deploy_key: str, group: str, connection: str, configuration: dict = None):
        """Deploys the connector to the Fivetran platform.

        Args:
            project_path (str): The path to the connector project.
            deploy_key (str): The deployment key.
            group (str): The group name.
            connection (str): The connection name.
            configuration (dict): The configuration dictionary.
        """
        validate_deploy_parameters(connection, deploy_key)

        _check_dict(configuration, True)

        secrets_list = []
        if configuration:
            for k, v in configuration.items():
                secrets_list.append({"key": k, "value": v})

        connection_config = {
            "schema": connection,
            "secrets_list": secrets_list,
            "sync_method": "DIRECT",
            "custom_payloads": [],
        }

        self.validate_requirements_file(project_path, True)

        group_id, group_name = self.__get_group_info(group, deploy_key)
        connection_id, service = self.__get_connection_id(connection, group, group_id, deploy_key) or (None, None)

        if connection_id:
            if service != 'connector_sdk':
                print(
                    f"SEVERE: The connection '{connection}' already exists and does not use the 'Connector SDK' service. You cannot update this connection.")
                os._exit(1)
            confirm = input(
                f"The connection '{connection}' already exists in the destination '{group}'. Updating it will overwrite the existing code and configuration. Do you want to proceed with the update? (Y/N): ")
            if confirm.lower() == "y":
                print("INFO: Updating the connection...\n")
                self.__upload_project(
                    project_path, deploy_key, group_id, group_name, connection)
                self.__update_connection(
                    connection_id, connection, group_name, connection_config, deploy_key)
                print("✓")
                print(
                    f"INFO: Visit the Fivetran dashboard to manage the connection: https://fivetran.com/dashboard/connectors/{connection_id}/status")
            else:
                print("INFO: Update canceled. The process is now terminating.")
                os._exit(1)
        else:
            self.__upload_project(project_path, deploy_key,
                                  group_id, group_name, connection)
            response = self.__create_connection(
                deploy_key, group_id, connection_config)
            if response.ok:
                print(
                    f"INFO: The connection '{connection}' has been created successfully.\n")
                connection_id = response.json()['data']['id']
                print(
                    f"INFO: Visit the Fivetran dashboard to start the initial sync: https://fivetran.com/dashboard/connectors/{connection_id}/status")
            else:
                print(
                    f"SEVERE: Unable to create a new connection, failed with error: {response.json()['message']}")
                os._exit(1)

    def __upload_project(self, project_path: str, deploy_key: str, group_id: str, group_name: str, connection: str):
        print(
            f"INFO: Deploying '{project_path}' to connection '{connection}' in destination '{group_name}'.\n")
        upload_file_path = self.__create_upload_file(project_path)
        upload_result = self.__upload(
            upload_file_path, deploy_key, group_id, connection)
        os.remove(upload_file_path)
        if not upload_result:
            os._exit(1)

    @staticmethod
    def __update_connection(id: str, name: str, group: str, config: dict, deploy_key: str):
        """Updates the connection with the given ID, name, group, configuration, and deployment key.

        Args:
            id (str): The connection ID.
            name (str): The connection name.
            group (str): The group name.
            config (dict): The configuration dictionary.
            deploy_key (str): The deployment key.
        """
        if not config["secrets_list"]:
            del config["secrets_list"]

        resp = rq.patch(f"https://api.fivetran.com/v1/connectors/{id}",
                        headers={"Authorization": f"Basic {deploy_key}"},
                        json={
                            "config": config,
                            "run_setup_tests": True
                        })

        if not resp.ok:
            print(
                f"SEVERE: Unable to update Connection '{name}' in destination '{group}', failed with error: '{resp.json()['message']}'.")
            os._exit(1)

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
        resp = rq.get(f"https://api.fivetran.com/v1/groups/{group_id}/connectors",
                      headers={"Authorization": f"Basic {deploy_key}"},
                      params={"schema": name})
        if not resp.ok:
            print(
                f"SEVERE: Unable to fetch connection list in destination '{group}'")
            os._exit(1)

        if resp.json()['data']['items']:
            return resp.json()['data']['items'][0]['id'], resp.json()['data']['items'][0]['service']

        return None

    @staticmethod
    def __create_connection(deploy_key: str, group_id: str, config: dict) -> rq.Response:
        """Creates a new connection with the given deployment key, group ID, and configuration.

        Args:
            deploy_key (str): The deployment key.
            group_id (str): The group ID.
            config (dict): The configuration dictionary.

        Returns:
            rq.Response: The response object.
        """
        response = rq.post(f"https://api.fivetran.com/v1/connectors",
                           headers={"Authorization": f"Basic {deploy_key}"},
                           json={
                               "group_id": group_id,
                               "service": "connector_sdk",
                               "config": config,
                               "paused": True,
                               "run_setup_tests": True,
                               "sync_frequency": "360",
                           })
        return response

    def __create_upload_file(self, project_path: str) -> str:
        """Creates an upload file for the given project path.

        Args:
            project_path (str): The path to the project.

        Returns:
            str: The path to the upload file.
        """
        print("INFO: Packaging your project for upload...")
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

        with ZipFile(upload_filepath, 'w', ZIP_DEFLATED) as zipf:
            for root, files in self.__dir_walker(project_path):
                for file in files:
                    if file == "connector.py":
                        connector_file_exists = True
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, project_path)
                    zipf.write(file_path, arcname)

        if not connector_file_exists:
            print(
                "SEVERE: The 'connector.py' file is missing. Please ensure that 'connector.py' is present in your project directory, and that the file name is in lowercase letters. All custom connectors require this file because Fivetran calls it to start a sync.")
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
                    dirs.append(name)
            else:
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
        print("INFO: Uploading your project...", end="", flush=True)
        response = rq.post(f"https://api.fivetran.com/v2/deploy/{group_id}/{connection}",
                           files={'file': open(local_path, 'rb')},
                           headers={"Authorization": f"Basic {deploy_key}"})
        if response.ok:
            print("✓")
            return True

        print("SEVERE: Unable to upload the project, failed with error: ", response.reason)
        return False

    @staticmethod
    def __get_os_name() -> str:
        """Returns the name of the operating system.

        Returns:
            str: The name of the operating system.
        """
        os_sysname = platform.system().lower()
        if os_sysname.startswith("darwin"):
            return MAC_OS
        elif os_sysname.startswith("windows"):
            return WIN_OS
        elif os_sysname.startswith("linux"):
            return LINUX_OS
        raise ValueError(f"Unrecognized OS: {os_sysname}")

    @staticmethod
    def __get_group_info(group: str, deploy_key: str) -> tuple[str, str]:
        """Retrieves the group information for the specified group and deployment key.

        Args:
            group (str): The group name.
            deploy_key (str): The deployment key.

        Returns:
            tuple[str, str]: A tuple containing the group ID and group name.
        """
        groups_url = "https://api.fivetran.com/v1/groups"

        params = {"limit": 500}
        headers = {"Authorization": f"Basic {deploy_key}"}
        resp = rq.get(groups_url, headers=headers, params=params)

        if not resp.ok:
            print(
                f"SEVERE: Unable to retrieve destination details. The request failed with status code: {resp.status_code}. Please ensure you're using a valid base64-encoded API key and try again.")
            os._exit(1)

        data = resp.json().get("data", {})
        groups = data.get("items")

        if not groups:
            print("SEVERE: No destinations defined in the account")
            os._exit(1)

        if not group:
            if len(groups) == 1:
                return groups[0]['id'], groups[0]['name']
            else:
                print(
                    "SEVERE: Destination name is required when there are multiple destinations in the account")
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

        print(
            f"SEVERE: The specified destination '{group}' was not found in your account.")
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
            print(f"Running on fivetran_connector_sdk: {__version__}")

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
              log_level: Logging.Level = Logging.Level.FINE) -> bool:
        """Tests the connector code by running it with the connector tester.\n
        state.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithstatejsonfile\n
        configuration.json docs: https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile

        Args:
            project_path (str): The path to the project.
            configuration (dict): The configuration dictionary, same as configuration.json if present.
            state (dict): The state dictionary, same as state.json if present.
            log_level (Logging.Level): The logging level.

        Returns:
            bool: True if there was an error, False otherwise.
        """
        global DEBUGGING
        DEBUGGING = True

        check_newer_version()

        Logging.LOG_LEVEL = log_level
        os_name = self.__get_os_name()
        tester_root_dir = _tester_root_dir()
        java_exe = self.__java_exe(tester_root_dir, os_name)
        install_tester = False
        version_file = os.path.join(tester_root_dir, VERSION_FILENAME)
        if os.path.isfile(version_file):
            # Check version number & update if different
            with open(version_file, 'r') as fi:
                current_version = fi.readline()

            if current_version != TESTER_VERSION:
                shutil.rmtree(tester_root_dir)
                install_tester = True
        else:
            install_tester = True

        if install_tester:
            os.makedirs(tester_root_dir, exist_ok=True)
            download_filename = f"sdk-connector-tester-{os_name}-{TESTER_VERSION}.zip"
            download_filepath = os.path.join(tester_root_dir, download_filename)
            try:
                print(f"INFO: Downloading connector tester version: {TESTER_VERSION} ", end="", flush=True)
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
                raise RuntimeError(f"\nSEVERE: Failed to install the connector tester. Error details: ",
                                   traceback.format_exc())

        project_path = os.getcwd() if project_path is None else project_path
        self.validate_requirements_file(project_path, False)
        print(f"INFO: Debugging connector at: {project_path}")
        server = self.run(50051, configuration, state, log_level=log_level)

        # Uncomment this to run the tester manually
        # server.wait_for_termination()

        try:
            print(f"INFO: Running connector tester...")
            for log_msg in self.__run_tester(java_exe, tester_root_dir, project_path, 50051):
                print(log_msg, end="")
        except:
            print(traceback.format_exc())
        finally:
            server.stop(grace=2.0)

    @staticmethod
    def __java_exe(location: str, os_name: str) -> str:
        """Returns the path to the Java executable.

        Args:
            location (str): The location of the Java executable.
            os_name (str): The name of the operating system.

        Returns:
            str: The path to the Java executable.
        """
        java_exe_base = os.path.join(location, "bin", "java")
        return f"{java_exe_base}.exe" if os_name == WIN_OS else java_exe_base

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
        pattern = re.compile(r'com\.fivetran\.fivetran_sdk\.tools\.testers\.\S+')
        replacement = 'Fivetran SDK Tester'

        for line in iter(stream.readline, ""):
            modified_line = pattern.sub(replacement, line)
            yield modified_line

    @staticmethod
    def __run_tester(java_exe: str, root_dir: str, project_path: str, port: int = 50051):
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
               "--tester-type=source"]

        popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        for line in Connector.process_stream(popen.stderr):
            yield line

        for line in Connector.process_stream(popen.stdout):
            yield line
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

        if not self.schema_method:
            return connector_sdk_pb2.SchemaResponse(schema_response_not_supported=True)
        else:
            configuration = self.configuration if self.configuration else request.configuration
            response = self.schema_method(configuration)
            self.process_tables(response)
            return connector_sdk_pb2.SchemaResponse(without_schema=common_pb2.TableList(tables=TABLES.values()))

    def process_tables(self, response):
        for entry in response:
            if 'table' not in entry:
                raise ValueError("Entry missing table name: " + entry)

            table_name = entry['table']

            if table_name in TABLES:
                raise ValueError("Table already defined: " + table_name)

            table = common_pb2.Table(name=table_name)
            columns = {}

            if "primary_key" in entry:
                self.process_primary_keys(columns, entry)

            if "columns" in entry:
                self.process_columns(columns, entry)

            table.columns.extend(columns.values())
            TABLES[table_name] = table

    def process_primary_keys(self, columns, entry):
        for pkey_name in entry["primary_key"]:
            column = columns[pkey_name] if pkey_name in columns else common_pb2.Column(name=pkey_name)
            column.primary_key = True
            columns[pkey_name] = column

    def process_columns(self, columns, entry):
        for name, type in entry["columns"].items():
            column = columns[name] if name in columns else common_pb2.Column(name=name)

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

            columns[name] = column

    def process_data_type(self, column, type):
        if type.upper() == "BOOLEAN":
            column.type = common_pb2.DataType.BOOLEAN
        elif type.upper() == "SHORT":
            column.type = common_pb2.DataType.SHORT
        elif type.upper() == "INT":
            column.type = common_pb2.DataType.SHORT
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
            for resp in self.update_method(configuration=configuration, state=state):
                if isinstance(resp, list):
                    for r in resp:
                        yield r
                else:
                    yield resp

        except TypeError as e:
            if str(e) != "'NoneType' object is not iterable":
                raise e


def find_connector_object(project_path) -> Connector:
    """Finds the connector object in the given project path.
    Args:
        project_path (str): The path to the project.
    
    Returns:
        object: The connector object.
    """

    sys.path.append(project_path) # Allows python interpreter to search for modules in this path
    module_name = "connector_connector_code"
    connector_py = os.path.join(project_path, "connector.py")
    spec = importlib.util.spec_from_file_location(module_name, connector_py)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    for obj in dir(module):
        if not obj.startswith('__'):  # Exclude built-in attributes
            obj_attr = getattr(module, obj)
            if '<fivetran_connector_sdk.Connector object at' in str(obj_attr):
                return obj_attr

    print(
        "SEVERE: The connector object is missing. Please ensure that you have defined a connector object using the correct syntax in your `connector.py` file. Reference: https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector")
    sys.exit(1)


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
    print(f"`fivetran {input_command}` is not a valid command.")
    print(f"Did you mean `fivetran {valid_command}`?")
    print("Use `fivetran --help` for more details.")


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


def main():
    """The main entry point for the script.
    Parses command line arguments and passes them to connector object methods
    """

    parser = argparse.ArgumentParser(allow_abbrev=False)

    # Positional
    parser.add_argument("command", help="|".join(VALID_COMMANDS))
    parser.add_argument("project_path", nargs='?', default=os.getcwd(), help="Path to connector project directory")

    # Optional (Not all of these are valid with every mutually exclusive option below)
    parser.add_argument("--state", type=str, default=None, help="Provide state as JSON string or file")
    parser.add_argument("--configuration", type=str, default=None, help="Provide secrets as JSON file")
    parser.add_argument("--api-key", type=str, default=None, help="Provide api key for deployment to production")
    parser.add_argument("--destination", type=str, default=None, help="Destination name (aka 'group name')")
    parser.add_argument("--connection", type=str, default=None, help="Connection name (aka 'destination schema')")

    args = parser.parse_args()

    if args.command.lower() == "version":
        print("fivetran_connector_sdk " + __version__)
        return

    connector_object = find_connector_object(args.project_path)

    # Process optional args
    ft_group = args.destination if args.destination else os.getenv('FIVETRAN_DESTINATION', None)
    ft_connection = args.connection if args.connection else os.getenv('FIVETRAN_CONNECTION', None)
    ft_deploy_key = args.api_key if args.api_key else os.getenv('FIVETRAN_API_KEY', None)
    configuration = args.configuration if args.configuration else None
    state = args.state if args.state else os.getenv('FIVETRAN_STATE', None)

    configuration = validate_and_load_configuration(args, configuration)
    state = validate_and_load_state(args, state)

    if args.command.lower() == "deploy":
        if args.state:
            print("WARNING: 'state' parameter is not used for 'deploy' command")
        connector_object.deploy(args.project_path, ft_deploy_key, ft_group, ft_connection, configuration)

    elif args.command.lower() == "debug":
        connector_object.debug(args.project_path, configuration, state)

    elif args.command.lower() == "reset":
        reset_local_file_directory(args)
    else:
        if not suggest_correct_command(args.command):
            raise NotImplementedError(f"Invalid command: {args.command}, see `fivetran --help`")


def validate_and_load_configuration(args, configuration):
    if configuration:
        json_filepath = os.path.join(args.project_path, args.configuration)
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r') as fi:
                configuration = json.load(fi)
        else:
            raise ValueError(
                "Configuration must be provided as a JSON file. Please check your input. Reference: "
                "https://fivetran.com/docs/connectors/connector-sdk/detailed-guide#workingwithconfigurationjsonfile")
    else:
        configuration = {}
    return configuration


def validate_and_load_state(args, state):
    if state:
        json_filepath = os.path.join(args.project_path, args.state)
        if os.path.isfile(json_filepath):
            with open(json_filepath, 'r') as fi:
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
        print("INFO: Reset canceled")
    else:
        try:
            if os.path.exists(files_path) and os.path.isdir(files_path):
                shutil.rmtree(files_path)
            print("INFO: Reset Successful")
        except Exception as e:
            print("ERROR: Reset Failed")
            raise e


if __name__ == "__main__":
    main()
