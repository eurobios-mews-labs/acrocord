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
import os.path
from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import Type
from typing import Union

import pandas as pd
import xlsxwriter
from pandas.core.dtypes.base import ExtensionDtype
from termcolor import colored
from termcolor import cprint

from acrocord import ConnectDatabase


class TableFactory(ABC):
    """
    Description
    -----------

    Abstract class for table creation and interaction with database.

    Usage
    -----

    If you want to use the static class you can call :

    >>> table_factory = TableFactory.get_instance()

    Otherwise you can create your own instance:

    >>> table_factory= TableFactory()

    Then you can access to the table by calling

    >>> table_factory.get_table()

    If you want to save the table to the database you can call:

    >>> table_factory.write_table(...)

    Then you can read the table from the same database using:

    >>> table_factory.read_table(...)
    """
    _instance: "TableFactory" = None

    @classmethod
    @abstractmethod
    def data_definition(cls) -> Dict[str, Tuple[ExtensionDtype, str]]:
        """
        Returns
        -------
        dict
            Column datatypes and description. The dict follow this structure
            {'column_name': (dtype, ' description')}

        """

    @classmethod
    @abstractmethod
    def get_db_connection(cls):
        ...

    @classmethod
    @abstractmethod
    def table_name(cls) -> str:
        """
        Returns
        -------
        str
            the database table name
        """

    @classmethod
    @abstractmethod
    def schema_name(cls) -> str:
        """
        Returns
        -------
        str
            the database schema name
        """

    @classmethod
    def read_table(cls,
                   connection: Union[dict, str, ConnectDatabase] = None,
                   schema_name: str = None,
                   table_name: str = None,
                   columns: iter = None,
                   where: str = None
                   ) -> pd.DataFrame:
        """
        if exists, read the table from database.
        Returns
        -------
        pd.DataFrame
            the table
        """
        schema_name: str = cls.schema_name() if schema_name is None else schema_name
        table_name: str = cls.table_name() if table_name is None else table_name
        columns = list(
            cls.data_definition().keys()) if columns is None else columns

        if isinstance(connection, ConnectDatabase):
            connection_ = connection
            close_db = False
        else:
            connection_ = cls.get_db_connection()
            close_db = True
        table = connection_.read_table(
            f'{schema_name}.{table_name}', columns=columns,
            where=where)

        # assert all columns and oly columns are retrieved
        assert table.columns.isin(columns).all() and pd.Index(columns).isin(
            table.columns).all()

        # convert to correct datatype
        for col in columns:
            dtype = cls.data_definition()[col][0]
            table[col] = table[col].astype(dtype)

        if close_db:
            connection_.close()
        return table

    @classmethod
    @abstractmethod
    def id_key(cls) -> Optional[Union[str, Sequence[str]]]:
        """
        Return Primary key column name, or None
        """
        ...

    @classmethod
    def get_column_description(cls) -> dict:
        """
        Returns
        -------
        dict
            dictionary with name of column as key and its description as value
        """
        doc = cls.data_definition()
        return {col_name: col_data[1] for col_name, col_data in doc.items()}

    @classmethod
    def get_instance(cls) -> "TableFactory":
        """
        Static instance getter to avoid recomputing the table for multiple usages.
        Returns
        -------
        TableFactory
            static table factory instance
        """

        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def data_description(cls) -> pd.DataFrame:
        """

        Returns
        -------
        pd.DataFrame
            a dataframe with table documentation
        """
        return pd.DataFrame(cls.data_definition(),
                            index=['data type', 'column description']).T

    @classmethod
    def get_foreign_keys(cls) -> Dict[str, Tuple[Type['TableFactory'], str]]:
        """
        return each foreign keys to be implemented in the following format:
        {key in current TableFactory columns (str) :
        (foreign table (TableFactory), foreign key (str) )}
        """
        return {}

    @classmethod
    def get_full_name(cls):
        return f"{cls.schema_name()}.{cls.table_name()}"

    def __init__(self, verbose: bool = True):
        self._verbose: bool = verbose
        # does the table have been saved into the deployment database
        self._is_deployed: bool = False
        self._table: Optional[pd.DataFrame] = None
        self.columns = list(self.data_definition().keys())

    @abstractmethod
    def _create_table(self) -> None:
        """
        compute and set the table in the self._table attribute
        Returns
        -------
        None
        """
        self._set_table(pd.DataFrame())

    def get_table(self, columns=None) -> pd.DataFrame:
        """
        Returns
        -------
        pd.DataFrame
            table générée par la fabrique
        """
        if self._is_deployed:
            return self.read_table(self.get_db_connection(), columns=columns)
        if self._table is None:
            self._create_table()
        return self._table.copy() if columns is None else self._table[
            columns].copy()

    def _set_table(self, table) -> None:
        """
        save table in self._table variable after checking its consistency with documentation
        Parameters
        ----------
        table

        Returns
        -------

        """
        table_ = table.rename(columns=str.lower)
        data_definition = self.data_definition()
        self.check_table_doc(table_, data_definition, self.__class__)
        self._table = table_[data_definition.keys()]

    def write_table(self, db: Optional[ConnectDatabase] = None,
                    schema: str = None, table_name: str = None,
                    **kwargs) -> None:
        """
        Write table to database

        Parameters
        ----------
        db : ConnectDatabase
            the database connection
        schema : str
            name of the schema
        table_name : str
            name of the tabler
        kwargs
            additional arguments for ConnectDatabase.write_table

        Returns
        -------

        """

        if db is None:
            db = self.get_db_connection()

        schema = self.schema_name() if schema is None else schema
        table_name = self.table_name() if table_name is None else table_name
        full_name = f"{schema}.{table_name}"
        if self._verbose:
            cprint(f"[write_table] {full_name}", 'white', attrs=['reverse', ])
        data = self.get_table()
        db.write_table(data, table_name=full_name,
                       column_comments=self.get_column_description(),
                       opt_dtype=False,
                       primary_key=self.id_key(), **kwargs)

        self._is_deployed = True
        self._table = None

    @staticmethod
    def check_table_doc(table: pd.DataFrame, doc: dict, class_warning=None):
        """
        check table and documentation consistency
        Parameters
        ----------
        table
        doc
        class_warning
            the name of the class to print for warnings
        Returns
        -------

        """
        if class_warning is None:
            class_warning = TableFactory.__class__
        warning_header = f"WARNING: {class_warning} "
        doc_cols = doc.keys()
        table_cols = table.columns

        if table_cols.isin(doc_cols).all() and len(doc_cols) == len(table_cols):
            for col_name, val in doc.items():
                col_dtype, col_doc = val
                try:
                    table[col_name] = table[col_name].astype(col_dtype)
                except Exception as e:
                    cprint(f"{warning_header} {col_name}: "
                           f"cannot convert {col_name} "
                           f"from {table[col_name].dtype} to {col_dtype}.",
                           'yellow')
                    cprint(f"{e}", 'yellow')
                if col_doc is None or col_doc == "":
                    cprint(warning_header +
                           f"missing description of {col_name}.",
                           'yellow')
        else:
            doc_cols = pd.Series(doc_cols)
            table_cols = pd.Series(table_cols)

            missing_doc = table_cols[~(table_cols.isin(doc_cols))]
            if len(missing_doc) > 0:
                cprint(
                    warning_header + f" column statement is missing:\n{missing_doc}",
                    'yellow')

            missing_col = doc_cols[~(doc_cols.isin(table_cols))]
            if len(missing_col) > 0:
                cprint(warning_header + f" data is missing :\n{missing_col}",
                       'yellow')

    def write_to_excel(self, save: bool = True, path: str = None,
                       file_name: str = None, verbose=True) -> 'xlsxwriter':
        if file_name is None:
            file_name = self.table_name() + '.xlsx'
        elif len(file_name) <= 5 or file_name[-5:] != '.xlsx':
            file_name += '.xlsx'
        if path is None:
            import tempfile
            path = tempfile.gettempdir()

        data = self.get_table()
        full_path = os.path.join(path, file_name)
        writer = pd.ExcelWriter(full_path, engine='xlsxwriter')
        doc = self.data_description()[
            ['column description']].reset_index().rename(
            columns={'index': 'column name'})

        for df, sheet_name, save_index in (
                (doc, 'Column description', False), (data, 'Data', False)):
            if verbose:
                print(f"Writing '{sheet_name}' Excel sheet...", end='')
            df.to_excel(writer, sheet_name=sheet_name, index=save_index)
            if verbose:
                print(f"\rWriting '{sheet_name}' Excel sheet" + colored(" ok",
                                                                        'green'))
            if verbose:
                print("adjust column width " + sheet_name + "...", end='')
            worksheet = writer.sheets[sheet_name]
            worksheet.autofilter(0, 0, 0, len(df.columns) - 1)
            for idx, col in enumerate(df):  # loop through all columns
                series = df[col]
                max_len = max((
                    series.astype(str).map(len).max(),  # len of largest item
                    len(str(col)) + 2  # len of column name/header +2 for bold
                )) + 1  # adding a little extra space
                worksheet.set_column(idx, idx, max_len)
            if verbose:
                print("\radjust column width " + sheet_name + colored(" ok",
                                                                      'green'))
        if save:
            if verbose:
                print("save excel...", end='')
            if hasattr(writer, "save"):
                writer.save()
            else:
                writer._save()
            if verbose:
                print(
                    f"ok -> {colored(full_path, 'blue', attrs=['underline'])}")
        return writer

    def add_foreign_keys(self, db: ConnectDatabase = None):
        if db is None:
            db = self.get_db_connection()
        full_table_name = f"{self.schema_name()}.{self.table_name()}"
        for key, (
                foreign_table, foreign_key) in self.get_foreign_keys().items():
            foreign_table_full_name = f"{foreign_table.schema_name()}" \
                                      f".{foreign_table.table_name()}"
            print(
                f"{full_table_name} ({key}) -- > ({foreign_key}) "
                f"{foreign_table_full_name} ")
            db.add_key(type_='foreign',
                       table_name=full_table_name, key=key,
                       table_foreign=foreign_table_full_name,
                       key_foreign=foreign_key,
                       )
