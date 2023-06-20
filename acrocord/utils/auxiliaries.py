# Copyright 2023 Eurobios
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#
from functools import wraps
from typing import Iterable, Dict

from sqlalchemy import text

from acrocord.misc import execution


def execute(cmd, hide=False, fetch=False):
    @wraps(cmd)
    def wrapper(*args, **kwargs):
        cmd_sql = cmd(*args, **kwargs)
        execute_sql_cmd(args[0], cmd_sql, hide=hide, fetch=fetch)

    return wrapper


def execute_sql_cmd(connection, cmd: str, hide=False, fetch=False):
    if not hide:
        execution.print_(cmd)
    with connection.engine.connect() as cursor:
        res = cursor.execute(text(cmd))
        cursor.commit()
        if fetch:
            ret = res.fetchall()
            return ret


def merge(connection, table1, table2, out="tmp_merge", out_type="VIEW",
          suffixe=("_x", "_y"), on="", left_on="", right_on=""):
    out = f"{connection._get_schema(out)}.{connection._get_name(out)}"
    out_save = out
    if out in [table1, table2]:
        out = f"{connection._get_schema(out)}.tmp_merge"
    if left_on == "":
        left_on = on
        right_on = on
    if isinstance(left_on, str):
        right_on, left_on = [right_on], [left_on]
    left_on, right_on = [l.lower() for l in left_on], [r.lower() for r in
                                                       right_on]

    all_columns = connection.get_columns(table1) + connection.get_columns(
        table2)
    col_intersect = set(connection.get_columns(table1)
                        ).intersection(connection.get_columns(table2))
    if suffixe != ("_x", "_y"):
        col_intersect = all_columns

    db_str = f"CREATE {out_type} {out} AS (\n  SELECT "
    for col in connection.get_columns(table1):
        db_str += table1 + "." + col
        if col in col_intersect and col not in left_on:
            db_str += " AS " + col + suffixe[0]
        db_str += ","
    for col in connection.get_columns(table2):
        if col not in left_on:
            db_str += table2 + "." + col
            if col in col_intersect:
                db_str += " AS " + col + suffixe[1]
            db_str += ","
    db_str = db_str[:-1]
    db_str += f" FROM {table1},{table2}"

    left_on_ = [f"{table1}.{l}" for l in left_on]
    right_on_ = [f"{table1}.{r}" for r in right_on]

    left_on_ = ", ".join(left_on_)
    right_on_ = ", ".join(right_on_)
    db_str += "\n  WHERE (" + left_on_ + ")=(" + right_on_ + "));"
    if connection.active_sql_printing:
        print(db_str)
    connection.drop_table(out, type_=out_type)
    execute_sql_cmd(connection, db_str, hide=True)

    if out_save in [table1, table2]:
        connection.drop_table(out_save)
        connection.replace(out, out_save, type_=out_type)


def count(table_name):
    sql_cmd = f"SELECT COUNT(*) FROM {table_name};"
    return sql_cmd


def drop_table(table_name, option="CASCADE", type_="TABLE"):
    return f"DROP {type_} IF EXISTS {table_name} {option};"


def list_table(schema: str):
    """

    Parameters
    ----------
    schema: str
        Schema name

    Returns
    -------
        SQL Query
    """
    sql_cmd = f"SELECT * FROM information_schema.tables " \
              f"WHERE table_schema = '{schema}'"
    return sql_cmd


def add(name, type_, column_name, dtype) -> str:
    return f"ALTER {type_} {name} ADD {column_name} {dtype}"


def drop(name: str, type_: str, column_name: str):
    """
    Drop a column or object in table

    Parameters
    ----------
    name: str
        Object to alter
    type_: str

        Type of object to alter
        - VIEW
        - TABLE (default)

    column_name: str
        Column or object to drop

    Returns
    -------
        SQL Query
    """
    return f"ALTER {type_} {name} DROP {column_name} "


def create_schema(name: str) -> str:
    """

    Parameters
    ----------
    name: str
        Name of the schema to create

    Returns
    -------
        SQL Query
    """
    return f"CREATE SCHEMA IF NOT EXISTS {name}"


def create_table(name: str, columns: Iterable[str], dtypes: Iterable[str],
                 column_comments: Dict[str, str] = None) -> str:
    """

    Parameters
    ----------
    name: str
        The name of the table
    columns: iterable of str
        The names of the columns
    dtypes: iterable of str
        The types of columns
    column_comments : dict
        The comment on columns `{column1: comment 1, ...}`

    Returns
    -------
        SQL Query
    """
    col_zip = [f'{" " * 4}"{c}"{" " * max(1, (15 - len(c)))}' + db_data_type(
        dtypes[i])
        for i, c in enumerate(columns)]
    sql_cmd = f"CREATE TABLE {name}( \n"
    sql_cmd += ",\n".join(col_zip) + "\n)"
    sql_cmd += '\n;'
    if column_comments is not None:
        for col_name, col_comment in column_comments.items():
            col_comment = col_comment.replace("'", "''")
            sql_cmd += f"COMMENT ON COLUMN {name}" \
                       f".{col_name} IS '{col_comment}';\n"
    return sql_cmd


_conversion_table = {"uint8": "int2",
                     "uint16": "int4",
                     "uint32": "int8",
                     "int8": "int2",
                     "Int8": "int2",
                     "int16": "int2",
                     "Int16": "int2",
                     "float16": "float4",
                     "float32": "float8",
                     "int32": "int4",
                     "Int32": "int4",
                     "int64": "int8",
                     "Int64": "int8",
                     "float64": "float8",
                     "Float64": "float8",
                     "float": "float8",
                     "bytes_": "text",
                     "object": "text",
                     "nan": "text",
                     "str": "text",
                     "string": "text",
                     "bool": "bool",
                     "boolean": "bool",
                     "datetime64": "timestamp",
                     "category": 'int4'}

_conversion_table_inv = {
    'int2': 'int16', 'float4': 'float16', 'int4': 'int32', 'int8': 'int64',
    'float8': 'float64', 'text': 'str', 'bool': 'bool',
    'datetime64': 'timestamp'}


def db_data_type(data_type: str, invert: bool = False) -> str:
    """

    Parameters
    ----------
    data_type: str
        data type to translate into postgresql one

    invert: bool
        reciprocal conversion

    Returns
    -------

    """
    if "datetime" in data_type:
        return "timestamp"
    if "timestamp" in data_type:
        return "datetime64[s]"
    data_type = str(data_type)

    if invert:
        return _conversion_table_inv[data_type]
    return _conversion_table[data_type]


def get_metadata(table_name: str) -> str:
    """

    Parameters
    ----------
    table_name: str
        Name of the table from which the metadata should be retrieved

    Returns
    -------
        SQL Query
    """
    schema, table = table_name.split('.')
    sql_cmd = f"""
        select
            table_cols.table_schema,
            table_cols.table_name,
            table_cols.column_name,
            pgd.description
        from pg_catalog.pg_statio_all_tables as st
        full outer join pg_catalog.pg_description pgd on (
            pgd.objoid = st.relid
        )
        full outer join information_schema.columns table_cols on (
            pgd.objsubid   = table_cols .ordinal_position and
            table_cols.table_schema = st.schemaname and
            table_cols.table_name   = st.relname
        )
        where
        table_cols.table_schema = '{schema}' and
        table_cols.table_name = '{table}' """
    return sql_cmd
