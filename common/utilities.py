
# TODO: clean up imports, remove unused ones
import importlib 
import json
import os 
import sys 
from io import BytesIO
import datetime
import traceback

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def log_message(log_level, message, exception=None, context=None):
    """
    Logs a message with the specified log level.

    :param log_level: The severity level of the log message.
    :param message: The log message to be logged.
    :param exception: An exception object to log the exception details and traceback. Defaults to None.
    :param context: Additional contextual information. Defaults to None.
    """

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{log_level}] {timestamp} - {message}"
    if exception:
        log_entry += f"\nException: {exception}\n{traceback.format_exc()}\n{traceback.print_exc()}"
    if context:
        log_entry += f"\nContext: {context}"
    print(log_entry)

def read_json_file(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError as e:
        log_message(log_level='Error',
                    message=f'Error: File not found at {file_path}.',
                    exception=e)
        return {}
    except json.JSONDecodeError as e:
        log_message(log_level='Error',
                    message=f'Error: Failed to decode JSON',
                    exception=e)
        return {}
    # MODIFIED: added this to handle most other exceptions that might occur, is this necessary?
    except Exception as e:
        log_message(log_level='Error',
                message=f'Error: Unexpected error reading {file_path}. Exception type: {type(e).__name__}',
                exception=e)
        return {}

# MODIFIED: not needed anymore
# def import_libraries(file_path: str):
#     try:
#         with open(file_path, 'r') as file:
#             libraries = file.read().splitlines()

#         for library in libraries:
#             if library.strip():  # Ignore empty lines
#                 try:
#                     importlib.import_module(library)
#                     print(f"Successfully imported {library}")
#                 except ImportError:
#                     print(f"Error: Could not import {library}. Is it installed?")
#     except FileNotFoundError:
#         print(f"Error: File '{file_path}' not found.")
#         sys.exit(1)


def update_table_name_that_starts_with_digit(table_name: str) -> str:
    """
    This method handles reconciling Vault objects that begin with a number and appending a 'n_' so that Redshift will
    accept the naming convention
    :param table_name: The name of the table that needs to be updated
    :return: The updated table name
    """
    # note: this applies to databricks as well
    # https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers
    # TODO: do we need to handle this for column names as well?
    # MODIFIED: proposed change
    if table_name and (table_name.startswith('_') or table_name[0].isalpha()):
        return table_name
    else:
        return f'_{table_name}'
    # ORIGINAL CODE:
    # if table_name.isdigit():
    #     return f'n_{table_name}'
    # else:
    #     return table_name