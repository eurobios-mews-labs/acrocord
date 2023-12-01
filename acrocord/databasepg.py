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
# limitations under the License.
"""
This module contains the main class to be used
It solely contains the class ConnectDatabase
"""

import io
import logging
import typing
from typing import Union, Iterable, Dict

import pandas as pd
from sqlalchemy import exc, text

from acrocord.misc import execution
from acrocord.utils import auxiliaries
from acrocord.utils import connect
from acrocord.utils import insert, cache
from acrocord.utils.types import warning_type

LOGGER = logging.getLogger(__name__)
logging.basicConfig(format='%(levelname)s:%(message)s')


class ConnectDatabase:
    def __init__(self, cache_data=False):
        self.engine = None
        self.connection = None
        self.replace = self.rename
        self.rename_ = {}
        self.active_execution_time = False
        self.active_sql_printing = False
        self.verbose = 1
        self.cache = cache_data

    def connect(self, verbose: int = 1,
                connection: Union[dict, str] = None,
                **kwargs) -> "ConnectDatabase":
        """
        Instantiate the connexion with the database. This function has to be
        called to access other methods.

        Return a :obj:`ConnectDatabase` object.


        Parameters
        ----------
        verbose: int,
            level of verbosity.
            - 0 nothing
            - 1 metadata
            - 2 sql command
        connection: (str or dict),
            connection parameters. If a string is
            provided the file `~/.postgresql/connections.cfg` will be loaded
            as connection parameters. If a dict is provided, it must contain
            'host', 'port', 'dbname' and 'user' keys.

        Returns
        -------
        :obj:`ConnectDatabase`
        """
        if isinstance(connection, str):
            connection = connect.load_available_connections()[connection]

        self.engine = connect.connect_psql_server(
            connection=connection, **kwargs.copy())
        self.connection = self.engine.raw_connection()
        self.active_execution_time = verbose >= 1
        self.active_sql_printing = verbose >= 2
        self.verbose = verbose
        self.username = connection["user"]

        return self

    def close(self) -> None:
        """
        Close the connection

        Returns
        -------
            None
        """
        self.engine.dispose()
        self.connection.detach()
        self.connection.close()

    # ===========================================================================
    #                           READ AND WRITE
    # ==========================================================================
    @execution.execution_time
    def read_table(
            self, table_name: str,
            columns: iter = None,
            where: str = None,
            **kwargs) -> pd.DataFrame:
        """
        Read a table from server

        Parameters
        ---------
        table_name: str
            Name of the table. Naming convention "schema.name" if no schema
            is specified the "main" schema will be used
        columns: iter
            Specific columns to request.
            If None all columns are requested.
            If errors are made or columns are missed, they will be ignored
        where: str
            Request specific rows in postgresql fashion

        Returns
        -------
            Data as :obj:`pandas.DataFrame`
        """
        table_name = f"{self._get_schema(table_name)}.{self._get_name(table_name)}"
        query = self._select(table_name, columns=columns, where=where, **kwargs)

        if self.cache:
            data_frame = cache.read(table_name, where=where,
                                    columns=columns, **kwargs)
            if len(data_frame) > 0:
                return data_frame

        copy_sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
        connection = self.engine.connect()
        with connection.connection.cursor() as cursor:
            storage = io.StringIO()
            cursor.copy_expert(copy_sql, storage)
            storage.seek(0)
        data_frame = pd.read_csv(storage, true_values=["t"], false_values=["f"])

        if self.cache:
            cache.write(data_frame, table_name,
                        columns=columns, where=where, **kwargs)
        return data_frame

    @execution.execution_time
    def write_table(self, data: typing.Union[pd.DataFrame, "GeoDataFrame"],
                    table_name: str = "tmp",
                    primary_key=None,
                    if_exists="replace_cascade",
                    foreign_keys=None,
                    column_comments=None,
                    **kwargs) -> None:
        """
        Create table based on panda's DataFrame. This function upload the data
        on server and insert all data in the table.

        Parameters
        ----------
        data: pandas.DataFrame
            Data (GeoDataFrame or DataFrame) to insert into the database.
        table_name: str
            Name of the table. Naming convention "schema.name" if no schema
            is specified the "main" schema will be used
        primary_key: list or str
            List of string specifying all keys or single string
        if_exists: str
            In case of existing table

            - "replace_cascade" : will delete all table with dependency
            - "replace" : will delete this table. Error will raise if there are dependencies
        foreign_keys: dict
            specify foreign keys in the following fashion
            {column name in the current table : foreign table name}
            Note that if the name of the foreign key is different in the
            foreign table, use the `add_key` method.
        column_comments: dict
            Columns comments of type `{column_name: column_comment}`

        Returns
        -------
            None

        >>> from acrocord import ConnectDatabase
        >>> dataframe = pd.DataFrame({'a': [155, 20, 3],
        >>>                           'b': [11, 299, 45],
        >>>                           'c': [73, 3, 39],
        >>>                           'd': [783, 488, 739],
        >>>                           "lat": [45, 45, 45],
        >>>                           "lon": [0, 1, 2]
        >>>                       })
        >>> df = ConnectDatabase().connect(
        >>>      connection="connection-name"
        >>>      ).write_table( dataframe, "test.test")
        """
        data = data.rename(columns=str.lower)
        table_name = f"{self._get_schema(table_name)}.{self._get_name(table_name)}"
        warning_type(data, verbose=self.verbose)

        if "geometry" in data.dtypes.values and self.has_postgis_extension:
            if_exists_pg = "replace" if if_exists == "replace_cascade" else if_exists
            # set postgis extension in database if not already set
            if if_exists == "replace_cascade":
                self.drop_table(table_name, option="cascade")
            data.to_postgis(
                con=self.engine,
                name=self._get_name(table_name),
                schema=self._get_schema(table_name),
                if_exists=if_exists_pg)
        else:
            if if_exists == "replace_cascade":
                self.drop_table(table_name, option="cascade")
                self.create_table(table_name, data.columns,
                                  data.dtypes.values.astype(str),
                                  column_comments)
            with execution.silence_stdout():
                self.insert(data, table_name)

        if primary_key is not None:
            self.add_key(table_name, primary_key)
        if foreign_keys is not None:
            for key in foreign_keys.keys():
                self.add_key(table_name, key, type_="foreign",
                             table_foreign=foreign_keys[key])

    def insert(self, data: pd.DataFrame, table_name: str,
               chunksize=1000) -> None:
        """
        This function insert data in existing postgresql table

        Parameters
        ---------
        data: pandas.DataFrame ,
            Data to insert
        table_name: str
            Name of the table. Note that the column of `data` and the columns
            of the table must be the same
        chunksize: int
            Size of chunk in insert function

        Returns
        -------
            None

        """

        insert.insert(self, data, table_name, chunksize)

    def create_insert(self, data: pd.DataFrame, table_name: str,
                      *args, **kwargs):
        """
        This function creates table if it does not exist and insert in the
        table otherwise.

        Parameters
        ----------
        data: pandas.DataFrame,
            Data to insert.
        table_name: str
            Name of the table. Naming convention "schema.name" if no schema
            is specified the "main" schema will be used
        """
        if self._get_name(table_name) in self.get_table_names(
                self._get_schema(table_name)):
            self.insert(data, table_name)
        else:
            self.write_table(data, table_name, *args, **kwargs)

    def add_key(self, table_name: str, key: str, type_="primary",
                table_foreign="",
                key_foreign="") -> None:
        """
        Add key (reference between tables).

        Parameters
        ----------
        table_name: str
            The name of the table for which to add key
        key: str
            The name of the key (corresponds to a column in the table)
        type_: str
            The type of key. By default, the key type is a primary key. The
            possible values are
                - "primary" to define primary key. The primary key must be
                 composed of unique values
                - "foreign" to define foreign key. If type is set to foreign,
                the argument `table_foreign` must be provided
        table_foreign: str
            The name of the foreign table. Argument is ignored if key type
            is set to "primary".
        key_foreign: str
            The name of the foreign key (column) in the foreign table table

        Returns
        -------
            None
        """

        table_name = f"{self._get_schema(table_name)}." \
                     f"{self._get_name(table_name)}"
        try:
            if key_foreign == "":
                key_foreign = key
            if table_foreign != "":
                df_type = self.get_dtypes(table_foreign)
                typ = df_type.loc[
                    df_type["column_name"].values == key_foreign,
                    "data_type"].values[0]
                sql_cmd = f"ALTER TABLE {table_name} ALTER COLUMN " \
                          f"{key} TYPE {typ} USING {key}::{typ};"
                auxiliaries.execute_sql_cmd(self, sql_cmd, fetch=False)
            key = _iterable_or_str(key)

            sql_cmd = f"ALTER TABLE {table_name} ADD {type_.upper()} KEY {key}"
            if type_ == "foreign":
                sql_cmd += f" REFERENCES {self._get_schema(table_foreign)}." \
                           f"{self._get_name(table_foreign)} ({key_foreign});"

            auxiliaries.execute_sql_cmd(self, sql_cmd, fetch=False)
        except exc.ProgrammingError as error:
            print(error)

    # ==========================================================================
    #                          ALTER DATA
    # ==========================================================================

    def add_columns(self, table_name: str, data: pd.DataFrame,
                    index: str) -> None:
        """
        Add columns to `table_name` based on the data provided on the index
        given in argument. The name of the index column given must be the same
        in table (on postgres server) and data (the data frame object)

        Parameters
        ----------
        table_name: str
            The name of table
        data: pandas.DataFrame
            Data to add in the table.
        index:
            The name of the column in the table and the dataframe on which
            to perform the merge

        Returns
        -------
            None
        """
        table_name = f"{self._get_schema(table_name)}.{self._get_name(table_name)}"
        table_tmp = self._get_schema(table_name) + ".tmp"
        self.write_table(data, table_tmp)
        if isinstance(index, str):
            index = (index,)
        index = [id_.lower() for id_ in index]

        auxiliaries.merge(self, table_name, table_tmp,
                          out=table_name, on=index, out_type="table")
        self.drop_table(table_tmp)

    def drop_column(self,
                    table_name: str, columns: Iterable[str],
                    option: str = "cascade", type_: str = "TABLE"):
        """
        Remove some columns

        Parameters
        ----------
        table_name: str
            The name of the table to alter
        columns: iter[str]
            The columns to drop
        option: str
            Option can be either
            - 'cascade' : all dependent objects will be removed
            - '': only the columns

            Default is 'cascade'
        type_:str
            The type of object can be either
                - a table and type_="TABLE"
                - a view and type_="VIEW"
        """

        sql_cmd = f"ALTER {type_} {table_name}"
        for col in columns:
            sql_cmd += f" DROP COLUMN {col} {option},"
        auxiliaries.execute_sql_cmd(self, sql_cmd[:-1], hide=True, fetch=False)

    @auxiliaries.execute
    def drop_table(self, table_name: str, option: str = "cascade",
                   type_: str = "TABLE"):
        """
        Delete a table

        Parameters
        ----------
        table_name: str
            The name of the table to drop (delete)
        option: str
            Option can be either
                - 'cascade' : all depend objects will be removed
                - '': only the columns
            Default is 'cascade'

        type_:str
            The type of object can be either
                - a table and type_="TABLE"
                - a view and type_="VIEW"

        """
        sql_cmd = auxiliaries.drop_table(table_name, option, type_)
        return sql_cmd

    @auxiliaries.execute
    def rename(self, table_name: str, new_table_name: str,
               type_: str = "TABLE"):
        """
        Rename a table

        Parameters
        ----------
        table_name: str
            The current name of the table
        new_table_name: str
            The new name of the table
        type_:str
            The type of table, either
                - a table and type_="TABLE"
                - a view and type_="VIEW"
            Default is "TABLE"

        Returns
        -------
            SQL query
        """

        sql_cmd = f"ALTER {type_} {table_name} " \
                  f"RENAME TO {self._get_name(new_table_name)}"
        return sql_cmd

    @auxiliaries.execute
    def copy(self, table_name: str, new_table_name: str, type_="TABLE") -> str:
        """
        Copy the table and create another table (or view)

        Parameters
        ----------
        table_name: str
            The name of the table to copy
        new_table_name: str
            The name of the target table
        type_: str
            The target type object, it can be either
            - "view" does not create new data
            - "table" creates new data

        Returns
        -------
            SQL query
        """
        table_name = f"{self._get_schema(table_name)}." \
                     f"{self._get_name(table_name)}"
        new_table_name = f"{self._get_schema(new_table_name)}." \
                         f"{self._get_name(new_table_name)}"
        self.drop_table(new_table_name, type_=type_)
        sql_cmd = f"CREATE {type_} {new_table_name} AS {type_} {table_name} ;"
        return sql_cmd

    # ==========================================================================
    #                           FETCH INFORMATION
    # ==========================================================================

    def get_dtypes(
            self,
            table_name: str,
            format: str = "dataframe") -> typing.Union[dict, pd.DataFrame]:
        """
        Get the column type of the table

        Parameters
        ----------
        table_name: str
            The name of the table to get type from

        format: str
            Format of the output

        Returns
        -------
           Dict or pandas.DataFrame
        """
        sql_cmd = f"select udt_name, column_name from " \
                  f"information_schema.columns where table_name=" \
                  f"'{self._get_name(table_name)}' AND table_schema=" \
                  f"'{self._get_schema(table_name)}'"
        ret = auxiliaries.execute_sql_cmd(self, sql_cmd, hide=True, fetch=True)
        df_type = pd.DataFrame(
            ret,
            columns=["data_type", "column_name"])
        return df_type

    def get_columns(self, table_name: str) -> list:
        """
        Get the list of columns of the given table name

        Parameters
        ----------
        table_name: str
            The name of the table

        Returns
        -------
            list of column names
        """
        dtypes = self.get_dtypes(table_name)
        return list(dtypes["column_name"])

    def get_metadata(self, table_name: str) -> pd.DataFrame:
        """
        Get metadata of a given table

        Parameters
        ----------
        table_name: str
            The name of the table

        Returns
        -------
            pandas.DataFrame with metadata
        """
        sql_cmd = auxiliaries.get_metadata(table_name)
        res = auxiliaries.execute_sql_cmd(self, sql_cmd, hide=True, fetch=True)
        df_meta = pd.DataFrame(res)
        df_type = self.get_dtypes(table_name)
        return pd.merge(df_meta, df_type, on='column_name')

    def get_shape(self, table_name: str) -> typing.Tuple[int, int]:
        """
        Get shape of a table

        Parameters
        ----------
        table_name:str
            The name of the table

        Returns
        -------
            tuple of row, column shape
        """
        table_name = f"{self._get_schema(table_name)}.{self._get_name(table_name)}"
        row = auxiliaries.execute_sql_cmd(
            self,
            auxiliaries.count(table_name),
            fetch=True,
            hide=True
        )[0][0]
        col = len(self.get_columns(table_name))
        return row, col

    def get_schema_names(self) -> list:
        """
        Get the names of schemas for a given connection.
        The connection is associated with a database.

        Returns
        -------
            List of schema names
        """

        with self.engine.connect() as cursor:
            res = cursor.execute(text('SELECT * FROM pg_catalog.pg_tables'))
            data_frame = pd.DataFrame(res.fetchall())
        list_ = list(set(data_frame.iloc[:, 0]))
        list_ = [schema for schema in list_ if
                 "pg" not in schema and "schema" not in schema]
        return list_

    def get_table_names(self, schema: str = "public") -> list:
        """
        Get a list of table for a given schema

        Parameters
        ----------
        schema: str
            The name of the schema

        Returns
        -------
            List of table names
        """
        res = auxiliaries.execute_sql_cmd(
            self,
            f"SELECT * FROM pg_catalog.pg_tables where schemaname='{schema}'",
            hide=True, fetch=True)
        df_names = pd.DataFrame(res)
        if len(df_names) == 0:
            return []
        return list(df_names.iloc[:, 1])

    def get_view_names(self, schema="public"):
        """
        Get a list of views for a given schema

        Parameters
        ----------
        schema: str
            The name of the schema

        Returns
        -------
            List of view names
        """
        res = auxiliaries.execute_sql_cmd(
            self, 'SELECT * FROM pg_catalog.pg_views',
            hide=True, fetch=True)
        df_names = pd.DataFrame(res)
        df_names = df_names[df_names.iloc[:, 0] == schema]
        return list(df_names.iloc[:, 1])

    def _select(self, table_name: str, columns: Iterable[str] = None,
                where: str = None,
                limit: int = None):
        """
        Select type query

        Parameters
        ----------
        table_name: str
            The name of the table on which to apply a select
        columns: iterable of str
            The name of the selected columns
        where: str
            Selection conditions according to the indexes
        limit: int
            The maximal number of row to select

        Returns
        -------
            SQL query
        """
        table_name = f"{self._get_schema(table_name)}.{self._get_name(table_name)}"
        if columns is not None:
            columns = [col.lower() for col in columns if
                       col in self.get_columns(table_name)]
            columns = pd.Series(columns).drop_duplicates(keep='first')
            query = ",\n  ".join(columns)
            query = "SELECT " + query + " FROM " + table_name
        else:
            query = "SELECT * FROM " + table_name

        if where is not None:
            query += " WHERE " + where

        if limit is not None:
            query += " LIMIT " + str(limit)
        if self.active_sql_printing:
            execution.print_(query)
        return query

    def update(self, table_name: str, where: str = "",
               column: str = "", value: str = "NULL") -> None:
        """
        Update values in table

        Parameters
        ----------
        table_name: str
            The name of the table
        where: str
            The location according to the indexes of the values to be modified
        column: str
            The column concerned
        value
            The value

        Returns
        -------
            None
        """
        if isinstance(value, str):
            value = f"'{value}'"

        sql_cmd = f"UPDATE {table_name} SET {column}={value}"
        if where != "":
            sql_cmd += f" where {where}"

        auxiliaries.execute_sql_cmd(self, sql_cmd, fetch=False)

    @staticmethod
    def _get_schema(name):
        return name.split(".")[0].lower() if "." in name else "main"

    @staticmethod
    def _get_name(name):
        return name.split(".")[1].lower() if "." in name else name.lower()

    @auxiliaries.execute
    def create_table(self, name: str, columns: Iterable[str],
                     dtypes: Iterable[str],
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
            SQL query
        """

        return auxiliaries.create_table(name, columns, dtypes, column_comments)

    @auxiliaries.execute
    def drop(self, name: str, type_: str, column_name: str) -> str:
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
            SQL query
        """
        return auxiliaries.drop(name, type_, column_name)

    @auxiliaries.execute
    def create_schema(self, name: str) -> str:
        """
        Create a schema

        Parameters
        ----------
        name: str
            Name of the schema

        Returns
        -------
            None
        """
        return auxiliaries.create_schema(name)

    def list_table(self, schema: str) -> pd.DataFrame:
        """
        List tables provided a postregsql schema

        Parameters
        ----------
        schema: str
            Schema name

        Returns
        -------
            :obj: `pandas.DataFrame` with table list and properties

        """
        return pd.DataFrame(auxiliaries.execute_sql_cmd(
            self, auxiliaries.list_table(schema),
            fetch=True))

    def execute(self, sql: str) -> None:
        """
        Provided a query as string,
        execute the query using SQLalchemy engine with psycopg2 driver

        Parameters
        ----------
        sql: str
            Some query to execute

        Returns
        -------
            None
        """
        auxiliaries.execute_sql_cmd(self, sql, hide=False)

    # ==========================================================================
    #                       Property of connexion
    # ==========================================================================
    @property
    def has_postgis_extension(self) -> bool:
        with self.engine.connect() as con:
            out_ = con.execute(
                text("SELECT COUNT(1) FROM information_schema.routines"
                     " WHERE routine_name = 'postgis_version'"))
            has_postgis = next(out_)[0]
        return has_postgis


def _iterable_or_str(arg) -> str:
    if isinstance(arg, str):
        arg = f"({arg})"
    else:
        arg = str(tuple(arg)).replace("'", "")
    return arg
