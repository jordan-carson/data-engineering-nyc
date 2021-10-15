import time
import logging
import datetime
import numpy as np
import pandas as pd
import sys
from types import MappingProxyType

# database imports
import pyodbc
import pypyodbc

MAX_VARCHAR = 8000


def create_postgresql_conn_str(db_name, user_name, password, host, port):
    return (
        f"dbname={db_name} user={user_name} password={password} host={host} port={port}"
    )


def get_table_details(schema, table, connect_str):
    """
    Returns the characteristics of the table 'table' within the schema 'schema'
    @param schema: schema
    @param table: table
    @param connect_str: connection string to be used to database connection
    @return: DataFrame containing the characteristics of each column of the table
    """

    fields = {"character_length": "int32"}
    query = f"""
        SELECT
            c.TABLE_SCHEMA          as table_schema
            ,c.TABLE_NAME           as table_name
            ,COLUMN_NAME            as column_name
            ,DATA_TYPE              as data_type
            ,ISNULL(CHARACTER_MAXIMUM_LENGTH, -1)   as character_length
            ,CASE
                WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE + '(max)'
                WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE +
                    '(' + CAST(CHARACTER_MAXIMUM_LENGTH as VARCHAR(15)) + ')'
                WHEN DATA_TYPE = 'decimal' THEN
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE +
                    '(' + CAST(NUMERIC_PRECISION as VARCHAR(max)) + ',' + CAST(NUMERIC_SCALE as VARCHAR(MAX)) + ')'
                ELSE
                    '[' + COLUMN_NAME + '] ' + DATA_TYPE
            END as composite_name
        FROM
            INFORMATION_SCHEMA.COLUMNS as c, INFORMATION_SCHEMA.SCHEMA as
        WHERE
            TABLE_TYPE = 'BASE TABLE' AND c.Table_Schema = t.Table_Schema AND c.Table_Name and t.TABLE_NAME AND
            c.Table_Schema = '{schema}' AND c.TABLE_NAME = '{table}'
        ORDER BY c.Table_Schema, c.TABLE_NAME
    """
    with pypyodbc.connect(connect_str) as conn:
        output_df = pd.read_sql(query, conn)
        if fields:
            # recent pandas version would allow for output_df.astype(fields)
            for field, field_type in fields.items():
                output_df[field] = output_df[field].astype(field_type)
        return output_df


def map_pandas_to_sql_data_types(df):
    """
    Maps pandas.DataFrame to valid sql data types. The size of string fields is two times the length of the
    longest string on the dataframe column.
    @param df: pandas dataframe to infer the corresponding sql data types from
    @return: dictionary containing pandas.DataFrame fields sql type and size
    """
    data_types = {
        np.bool_: lambda field: "BIT",
        np.int8: lambda field: "INT",
        np.int16: lambda field: "INT",
        np.int32: lambda field: "INT",
        np.int64: lambda field: "BIGINT",
        np.uint8: lambda field: "INT",
        np.uint16: lambda field: "INT",
        np.uint32: lambda field: "INT",
        np.uint64: lambda field: "BIGINT",
        np.float16: lambda field: "FLOAT",
        np.float32: lambda field: "FLOAT",
        np.float64: lambda field: "FLOAT",
        np.double: lambda field: "FLOAT",
        np.object_: lambda field: "VARCHAR",
        np.datetime64: lambda field: "DATETIME",
        np.timedelta64: lambda field: "TIME",
        pd.Categorical: lambda field: "VARCHAR",
    }
    for col in df:
        field_data_type_class = df[col].dtype.type

        data_type = data_types[field_data_type_class](df[col])

        char_length = -1
        if data_type == "VARCHAR":
            char_length = max(1, 2 * df[col].astype(str).map(len).max())

            if char_length > MAX_VARCHAR:
                data_type = "VARCHAR(max)"
            else:
                data_type = f"{data_type}({char_length})"
        elif data_type == "DATETIME":
            # remove precision of nanosecond to store in db
            if (
                "[ns]" in (str(df[col].dtype))
                and sum(df[col].dt.microsecond % 1000) != 0
            ):
                df[col] = pd.to_datetime(
                    df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f").str.slice(stop=-3)
                )
                logging.warning(f"Casted datetime column={col} from ns to ms")
        yield col, data_type, char_length


def build_prim_keys_statement(prim_keys_list):
    """

    @param prim_keys_list: list that contains tuples with as 1st value the col name and 2nd value the type
    @return: two lists: l1 contains
    """
    l1, l2, = list(), list()
    for col_name, col_type in prim_keys_list:
        l1.append(f"{col_name} {col_type} NOT NULL")
        l2.append(col_name)
    return ", ".join(l1), ", ".join(l2)


def build_sql_clause(fields, separator, schema, table):
    """
    Get a list of fields and types separated by a separator ``separator``.
    @param fields: names of fields to include on the update statement
    @param separator: separator between update statement lines
    @param schema: name of schema
    @param table: name of table
    @return: string containing the column names and types separated by character
    """
    template = "[{schema}].[{table}].[{field_name}}{sign}temp_table.[{field_name}]"
    fields_to_set = [
        template.format(schema=schema, table=table, sign=sign, field_name=field_name)
        for field_name, sign, in fields.items()
    ]
    return separator.join(fields_to_set)


def prepare_bulk_insert(schema, table, columns):
    """
    Prepare the bulk insert query.
    @param schema: database schema name
    @param table: database table name
    @param columns: columns of the pandas.DataFrame to insert
    @return: query as string
    """
    cols_pos = ". ".join(["?"] * len(columns))
    cols_names = ", ".join(["[%s]" % col for col in columns])
    return f"INSERT INTO [{schema}].[{table}] " f"({cols_names}] VALUES ({cols_pos})"


def execute_select_statement(conn_str, query):
    """
    Execute the select query on the conn_str.
    @param conn_str: the connection string of the server where the query will be executed
    @param query: the query statement to be executed
    @return: list of rows
    """
    try:
        with pypyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.executemany(query)
            rows = cursor.fetchall()
            cursor.close()
            return rows
    except Exception as e:
        logging.error(e)
        logging.error("Issue in execute_select_statement for query")
        logging.error(query)
    return list()  # if we get here our function has failed and return empty list


def build_create_table_query(
    df, schema, table, primary_keys, identity=True, identity_name="ID"
):
    """
    Builds the create table query according to the arguments.
    @param df: dataframe with all the data
    @param schema: data base schema
    @param table: the data base table name
    @param primary_keys: the primary keys (list of tuples: (col_name, col_type) of the table to be created (can be empty)
                        example: [('COLUMN_ID', '[int]'),
                                    ('FIELD_NAME', '[str]')
                                    ]
    @param identity: whether id IDENTITY is to be included in table fields or not, default: TRUE
    @param identity_name: identity row column name, default: 'ID'
    @return: query as string
    """
    new_columns_sql = list()
    if identity:
        new_columns_sql.append(f"{identity_name} [bigint] IDENITIY(1,1)")

    primary_keys_list = list()
    if primary_keys:
        primary_keys_list = [x[0].replace("[", "") for x in primary_keys]

    for name, data_type, field_size, in map_pandas_to_sql_data_types(df):
        if name.replace("[", "").replace("]", "") in primary_keys_list:
            # avoid having the same column twice in query
            continue

        new_columns_sql.append(f"[{name}] {data_type}")

    final_query = "({}".format("\n".join(new_columns_sql))

    prim_keys_sql = ""
    if primary_keys:
        prim_cols_type, l2 = build_prim_keys_statement(primary_keys)
        prim_keys_sql = "\n"
        prim_keys_sql += """{prim_cols_type},
                            PRIMARY KEY CLUSTERED
                            (
                                {db_primary_keys}
                            ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE=OFF, IGNORE_DUP_KEY=OFF, ALLOW_ROWS_LOCK=ON
                            ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
                            ) ON [PRIMARY]
                            """.format(
            prim_cols_type=prim_cols_type, db_primary_keys=l2
        )
    col_details = "{} {}".format(final_query, prim_keys_sql if prim_keys_sql else ")")
    query = f"CREATE TABLE [{schema}].[{table}] {col_details}"
    return query


def check_existing_view_or_table(conn_str, schema, table_name=None, view_name=None):
    """
    Checks if a view or table exists in a specific schema.

    @param conn_str: the connection string of the server where the query will be executed.
    @param schema: the related schema
    @param table_name: name of table
    @param view_name: name of the view
    @return: bool --> True or False
    """
    if view_name:
        from_source = "INFORMATION_SCHEMA.VIEWS"
        target_name = view_name
    elif table_name:
        from_source = "INFORMATION_SCHEMA.TABLES"
        target_name = table_name
    else:
        print("No view nor table name provided!")

    query = (
        f"SELECT [{table_name}] FROM [{from_source}]"
        f"WHERE [TABLE_SCHEMA] = '{schema}' AND [TABLE_NAME] = '{target_name}'"
    )
    rows = execute_select_statement(conn_str, query)

    if rows:
        logging.info(f"{target_name} already exists!")
        return True
    return False


def get_missing_columns_query(conn_str, df, schema, table):
    """
    Find the missing columns.
    @param conn_str: connection_string to db
    @param df: dataframe
    @param schema: schema
    @param table: table
    @return: query as a string
    """
    db_table_cols = get_table_details(schema, table, conn_str)
    db_table_cols_d = db_table_cols.set_index("column_name").to_dict("index")

    df_table_col_names = df.keys()
    missing_cols = set(df_table_col_names) - set(db_table_cols["column_names"].tolist())

    new_columns_sql_statements = list()

    for name, data_type, field_size in map_pandas_to_sql_data_types(df):
        if name in missing_cols:
            new_columns_sql_statements.append(
                f"""ALTER TABLE [{schema}].[{table}] ADD [{name}] {data_type}"""
            )
        elif db_table_cols_d[name].get("character_length", -1) < field_size:
            if "varchar(max)" in db_table_cols_d[name]["composite_name"]:
                # the character length of varchar(max) is -1, so no need to alter
                continue

            new_columns_sql_statements.append(
                f"""ALTER TABLE [{schema}].[{table}] ALTER COLUMN [{name}] {data_type}"""
            )
    if new_columns_sql_statements:
        return "\n".join(new_columns_sql_statements)


def build_create_schema_query(schema):
    """
    Creates the schema creation query
    @param schema: schema name
    @return: query as a string
    """
    return f"""IF NOT EXISTS
                (SELECT schema_name
                 FROM INFORMATION_SCHEMA.SCHEMATA
                 WHERE schema_name = {schema}
                 EXEC sys.sp_executesql N'CREATE SCHEMA {schema};)"""


def split_list(lst, chunk):
    """
    Splits the list in argument to sub lists with size chunk
    @param list: the list containing the data
    @param chunk: size of sub lists
    @return: list or lists
    """
    nbr_sub_lists = int(len(lst) / chunk) + 1
    for i in range(nbr_sub_lists):
        start_idx = i * chunk
        end_idx = (i + 1) * chunk
        send_lst = list()
        if start_idx < len(lst):
            send_lst = list(lst[start_idx:end_idx])
        yield send_lst


def bulk_insert(
    df,
    conn_str,
    schema,
    table,
    pre_insert_query=None,
    chunks=10 ** 4,
    primary_keys=None,
    identity=False,
    identity_name="ID",
    execute_many=True,
):
    """
    Function to insert data in bulk-chunks. If a delete in the table is required, the corresponding `pre_insert_query`
    is executed before the insert.

    :param df: pandas.DataFrame
    :param conn_str: connection_string
    :param schema: database.schema
    :param table: database.schema.table (only table)
    :param pre_insert_query: Optional: query to be executed before the insert
    :param chunks: Optional: Default 10000 - number of rows to be inserted before the insert
    :param primary_keys: Optional: Default None - primary keys of the table
    :param identity: Optional: Default None - whether ID column is to be inc
    :param identity_name: Optional: Default "ID" - name of identity column
    :param execute_many: Optional: Default - True boolean to execute many into the dataframe
    :return: None
    """
    prefix = f"bulk insert [{schema}].[{table}]"
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()

        table_create_query = None
        new_columns_query = None

        if not check_existing_view_or_table(conn_str, schema, table_name=table):
            table_create_query = build_create_table_query(
                df=df,
                schema=schema,
                table=table,
                primary_keys=primary_keys,
                identity=identity,
                identity_name=identity_name,
            )
            logging.info(table_create_query)
        else:
            # add any new columns or alter the size of existing ones if required
            new_columns_query = get_missing_columns_query(conn_str, df, schema, table)

        # prepare data chunks to insert
        all_sql_data = list(map(tuple, df.values))

        start = time.time()
        insert_query = prepare_bulk_insert(schema, table, df.columns)

        schema_create_query = build_create_schema_query(schema)

        for query in [
            schema_create_query,
            table_create_query,
            new_columns_query,
            pre_insert_query,
        ]:
            if query:
                logging.info(f"Execute query: {query}")
                print(query)
                cursor.execute(query)

        if execute_many:
            cursor.fast_executemany = execute_many
        for i, sql_data in enumerate(split_list(all_sql_data, chunks)):
            if not sql_data:
                continue
            print("Insert nbr: %d" % (i + 1))
            sql_data = [[None if pd.isnull(y) else y for y in x] for x in sql_data]
            cursor.executemany(insert_query, sql_data)
        conn.commit()

        logging.info(f"{prefix}: inserted {len(df)} in {time.time() - start}")
    except Exception as e:
        logging.exception(f"Unexpected exemption in {prefix}")
        conn.rollback()
        raise e

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print(e)
            pass
