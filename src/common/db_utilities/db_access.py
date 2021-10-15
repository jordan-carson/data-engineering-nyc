import sys
import pandas as pd
import datetime
import time
import logging


def get_connection(
    connection_string: str, database_type: str, library=None, logger=print
):
    """
    Opens a connection to a database, which will then feed subsequent functions.
    Args:
        connection_string: the connection string to be used to database connection
        database_type: name of database type
        library: name of library to use to connect to database_type
        logger: Optional - allows to change between print and logging.info

    Returns:
        connection_library.connect(connection_string)
    """
    connection_library = None
    config_dict = {
        "postgresql": "psycopg2",
        "oracle": "cx_Oracle",
        "sqlserver": "pyodbc",
        "redshift": "psycopg2",
    }
    library = config_dict.get(database_type) if not library else library
    if database_type in ["postgresql", "redshift"]:
        if library == "psycopg2":
            try:
                import psycopg2 as connection_library
            except ImportError:
                logger(f"{library} was not found. run `pip install {library}`")
                sys.exit(1)
    elif database_type == "oracle":
        if library == "cx_Oracle":
            try:
                import cx_oracle as connection_library
            except ImportError:
                logger(f"{library} was not found. run `pip install {library}`")
                sys.exit(1)
    elif database_type == "sqlserver":
        if library == "pypyodbc":
            try:
                import pypyodbc as connection_library
            except ImportError:
                logger(f"{library} was not found. run `pip install {library}`")
                sys.exit(1)
        elif library == "pyodbc":
            try:
                import pyodbc as connection_library
            except ImportError:
                logger(f"{library} was not found. run `pip install {library}`")
                sys.exit(1)
    if not connection_library:
        raise ValueError(f"Invalid {database_type} and {library} combinations.")
    return connection_library.connect(connection_string)


def query_df(
    sql_string,
    connection_string,
    database_type="postgresql",
    library="psycopg2",
    logger=print,
):
    """
    Returns a pandas.DataFrame from database_type server.
    Args:
        sql_string: SQL query to put into pandas.DataFrame
        connection_string: connection_string to the database
        database_type: the type of database
        library: the python module to use for connecting to database
        logger: logging to console - print or logging.info

    Returns:
        pandas.DataFrame (read from server)
    """
    start = datetime.datetime.now()
    connection = get_connection(
        connection_string, database_type=database_type, library=library
    )
    try:
        data = pd.read_sql(sql_string, connection)

    except Exception as err:
        # logger(f'ERROR: {err}')
        logger(
            f"[{__name__}] Error reading SQL query: {err} - Time Elapsed: "
            f"{(datetime.datetime.now() - start).total_seconds()} seconds"
        )
        if database_type in ["oracle", "sqlserver"] and library in [
            "cx_Oracle",
            "pyodbc",
        ]:
            logger("Using fallback library pypyodbc")
            return query_df(
                sql_string, connection_string, database_type, library="pypyodbc"
            )
        return pd.DataFrame()
    finally:
        connection.close()
    end = datetime.datetime.now()
    logger(f"Retrieved {len(data)} rows in {(end-start).total_seconds()} seconds.")
    return data
